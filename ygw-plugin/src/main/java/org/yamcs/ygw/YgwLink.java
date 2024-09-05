package org.yamcs.ygw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.yamcs.ConfigurationException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.Spec.OptionType;
import org.yamcs.events.EventProducerFactory;
import org.yamcs.logging.Log;
import org.yamcs.parameter.SoftwareParameterManager;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.AggregatedDataLink;
import org.yamcs.tctm.Link;
import org.yamcs.xtce.DataSource;
import org.yamcs.ygw.protobuf.Ygw.CommandAck;
import org.yamcs.ygw.protobuf.Ygw.CommandDefinitionList;
import org.yamcs.ygw.protobuf.Ygw.Event;
import org.yamcs.ygw.protobuf.Ygw.LinkStatus;
import org.yamcs.ygw.protobuf.Ygw.MessageType;
import org.yamcs.ygw.protobuf.Ygw.NodeList;
import org.yamcs.ygw.protobuf.Ygw.ParameterData;
import org.yamcs.ygw.protobuf.Ygw.ParameterDefinitionList;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import us.hebi.quickbuf.InvalidProtocolBufferException;

public class YgwLink extends AbstractLink implements AggregatedDataLink {
    final static int MAX_PACKET_LENGTH = 0xFFFF;
    public static final byte VERSION = 0;
    DataSource dataSource;

    String host;
    int port;
    long reconnectionDelay;
    String mdbPath;

    List<Link> sublinks = new ArrayList<>();
    Map<Integer, YgwNodeLink> nodes = new HashMap<>();

    YfeChannelHandler handler;
    YgwParameterManager paramMgr;
    YgwCommandManager cmdMgr;

    // this is the processor to which we listen for parameter updates
    String parameterProcessorName;

    @Override
    public void init(String instance, String name, YConfiguration config) {
        super.init(instance, name, config);
        this.host = config.getString("host");
        this.port = config.getInt("port");
        this.reconnectionDelay = config.getLong("reconnectionDelay");
        this.mdbPath = config.getString("mdbPath");

        log = new Log(getClass(), instance);
        log.setContext(name);
        eventProducer = EventProducerFactory.getEventProducer(instance, name, 10000);
        timeService = YamcsServer.getTimeService(instance);
        parameterProcessorName = config.getString("processor");
        this.dataSource = config.getEnum("dataSource", DataSource.class);

    }

    @Override
    public Spec getSpec() {
        Spec spec = getDefaultSpec();
        spec.addOption("host", OptionType.STRING).withRequired(true)
                .withDescription("The host to connect to the Yamcs gateway");

        spec.addOption("port", OptionType.INTEGER)
                .withDescription("Port to connect to the Yamcs gateway");

        spec.addOption("reconnectionDelay", OptionType.INTEGER).withDefault(5000)
                .withDescription("If the connection to the Yamcs gateway fails or breaks, "
                        + "the time (in milliseconds) to wait before reconnection.");

        spec.addOption("mdbPath", OptionType.STRING).withDefault("/ygw")
                .withDescription("Name of the subystem where the commands and parameters "
                        + "for the gateway connected to this link are created");

        spec.addOption("dataSource", OptionType.STRING).withDefault("EXTERNAL2")
                .withChoices("EXTERNAL1", "EXTERNAL2", "EXTERNAL3")
                .withDescription("The DataSource to use for the parameters registered by the gateway nodes");

        spec.addOption("processor", OptionType.STRING).withDefault("realtime")
                .withDescription("The processor providing parameter updates. "
                        + "A SoftwareParameter manager for the configured data source will be registered in this processor");

        spec.addOption("commandPostprocessorClassName", OptionType.STRING);
        spec.addOption("commandPostprocessorArgs", OptionType.MAP).withSpec(Spec.ANY);

        spec.addOption("packetPreprocessorClassName", OptionType.STRING);
        spec.addOption("packetPreprocessorArgs", OptionType.MAP).withSpec(Spec.ANY);
        spec.addOption("updateSimulationTime", OptionType.BOOLEAN).withDefault(false);
        return spec;

    }

    void connect() {
        handler = new YfeChannelHandler();
        NioEventLoopGroup workerGroup = getEventLoop();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_PACKET_LENGTH, 0, 4));
                ch.pipeline().addLast(handler);
            }
        });
        b.connect(host, port).addListener(f -> {
            if (!f.isSuccess()) {
                eventProducer.sendWarning("Failed to connect to the Yamcs gateway: " + f.cause().getMessage());
                if (reconnectionDelay > 0) {
                    workerGroup.schedule(() -> connect(), reconnectionDelay,
                            TimeUnit.MILLISECONDS);
                }
            } else {
                log.info("Connected to the Yamcs gateway at {}:{}", host, port);
            }
        });

    }

    public void updateNodes(NodeList nodeList) {
        log.info("Received list of nodes from the gateway: {}", nodeList);
        sublinks.clear();
        nodes.clear();

        for (var node : nodeList.getNodes()) {
            YgwNodeLink nl = new YgwNodeLink(this, node);
            nl.init(yamcsInstance, linkName + "." + nl.name, config);

            sublinks.add(nl);
            nodes.put(nl.nodeId, nl);
        }
        var linkManager = YamcsServer.getServer().getInstance(yamcsInstance).getLinkManager();

        try {
            linkManager.configureDataLink(this, config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public YConfiguration getConfig() {
        return config;
    }

    @Override
    public List<Link> getSubLinks() {
        return sublinks;
    }

    @Override
    public long getDataInCount() {
        return sublinks.stream().mapToLong(l -> l.getDataInCount()).sum();
    }

    @Override
    public long getDataOutCount() {
        return sublinks.stream().mapToLong(l -> l.getDataOutCount()).sum();
    }

    @Override
    public void resetCounters() {
        for (Link l : sublinks) {
            l.resetCounters();
        }
    }

    @Override
    public void doDisable() {
        doStop();
    }

    @Override
    public void doEnable() {
        doStart();
    }

    @Override
    public String getName() {
        return linkName;
    }

    @Override
    protected Status connectionStatus() {
        var _handler = handler;
        if (_handler == null) {
            return Status.UNAVAIL;
        }
        return _handler.isConnected() ? Status.OK : Status.UNAVAIL;
    }

    @Override
    protected void doStart() {
        YamcsServerInstance ysi = YamcsServer.getServer().getInstance(yamcsInstance);
        Processor processor = ysi.getProcessor(parameterProcessorName);

        if (processor == null) {
            notifyFailed(new ConfigurationException(
                    "No processor '" + parameterProcessorName + "' within instance '" + yamcsInstance + "'"));
            return;
        }
        var ppm = processor.getParameterProcessorManager();
        SoftwareParameterManager mgr = ppm.getSoftwareParameterManager(dataSource);

        if (mgr == null) {
            this.paramMgr = new YgwParameterManager(processor, yamcsInstance, dataSource);
            ppm.addSoftwareParameterManager(dataSource, paramMgr);
        } else if (mgr instanceof YgwParameterManager) {
            this.paramMgr = (YgwParameterManager) mgr;
        } else {
            notifyFailed(new ConfigurationException(
                    "There is already a different parameter manager registered for the source " + dataSource));
            return;
        }
        cmdMgr = new YgwCommandManager(this, processor, yamcsInstance);

        getEventLoop().execute(() -> connect());

        notifyStarted();
    }

    @Override
    protected void doStop() {
        getEventLoop().execute(() -> {
            if (handler != null) {
                handler.stop();
            }
        });
        notifyStopped();
    }

    boolean isConnected() {
        var _handler = handler;
        if (_handler == null) {
            return false;
        }
        return _handler.isConnected();
    }

    public CompletableFuture<Void> sendMessage(byte msgType, int nodeId, int linkId, byte[] data) {
        CompletableFuture<Void> cf = new CompletableFuture<Void>();
        var _handler = handler;
        if (_handler == null || _handler.ctx == null) {
            cf.completeExceptionally(new IOException("Connection to the gateway not open"));
        } else {
            _handler.sendMessage(msgType, nodeId, linkId, data).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    cf.complete(null);
                } else {
                    cf.completeExceptionally(future.cause());
                }
            });
        }
        return cf;
    }

    class YfeChannelHandler extends ChannelInboundHandlerAdapter {
        ChannelHandlerContext ctx;

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.warn("Connection to the Yamcs gateway closed");
            ctx.executor().schedule(() -> connect(), reconnectionDelay, TimeUnit.MILLISECONDS);
        }

        public ChannelFuture sendMessage(byte msgType, int nodeId, int linkId, byte[] data) {
            ByteBuf buf = Unpooled.buffer(14 + data.length);
            buf.writeInt(10 + data.length);
            buf.writeByte(VERSION);
            buf.writeByte(msgType);

            buf.writeInt(nodeId);
            buf.writeInt(linkId);
            buf.writeBytes(data);

            return ctx.writeAndFlush(buf);

        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buf = (ByteBuf) msg;
            buf.readInt();// length
            byte version = buf.readByte();
            if (version != VERSION) {
                log.warn("Got mesage with version {}, expected {}; closing connection", version, VERSION);
                ctx.close();
                return;
            }
            // recording number
            long rn = buf.readLong();
            byte type = buf.readByte();
            try {
                if (type == MessageType.TM_VALUE) {
                    processTm(buf);
                } else if (type == MessageType.PARAMETER_DATA_VALUE) {
                    processParameters(buf);
                } else if (type == MessageType.EVENT_VALUE) {
                    processEvent(buf);
                } else if (type == MessageType.NODE_INFO_VALUE) {
                    processNodeInfo(buf);
                } else if (type == MessageType.LINK_STATUS_VALUE) {
                    processLinkStatus(buf);
                } else if (type == MessageType.PARAMETER_DEFINITIONS_VALUE) {
                    processParameterDefs(buf);
                } else if (type == MessageType.COMMAND_DEFINITIONS_VALUE) {
                    processCommandDefs(buf);
                } else if (type == MessageType.TC_ACK_VALUE) {
                    processTcAck(buf);
                } else {
                    log.warn("message of type {} not implemented", type);
                }
            } catch (Exception e) {
                log.error("Exception processing message", e);
            }
        }

        private void processNodeInfo(ByteBuf buf) {
            try {
                NodeList nodeList = ProtoBufUtils.fromByteBuf(buf, NodeList.newInstance());
                updateNodes(nodeList);
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode node info", e);
            }
        }

        /**
         * Called when TM packet messages are received from YGW
         */
        private void processTm(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            node.processTm(linkId, buf);
        }

        /**
         * Called when event messages are received from YGW
         */
        private void processEvent(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            try {
                Event ev = ProtoBufUtils.fromByteBuf(buf, Event.newInstance());
                eventProducer.sendEvent(ProtoConverter.toYamcsEvent(timeService, ev));
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode event", e);
            }
        }

        /**
         * Called when parameter values are receive from YGW
         */
        private void processParameters(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();
            ParameterData pdata;
            try {
                pdata = ProtoBufUtils.fromByteBuf(buf, ParameterData.newInstance());
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode parameters", e);
                return;
            }
            log.trace("Got parameter data {}", pdata);

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            var pvList = paramMgr.processParameters(YgwLink.this, nodeId, pdata);

            node.processParameters(linkId, pdata.getGroup(), pdata.getSeqNum(), pvList);
        }

        /**
         * Called when the link status is received from YGW
         **/
        private void processLinkStatus(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();
            LinkStatus lstatus;
            try {
                lstatus = ProtoBufUtils.fromByteBuf(buf, LinkStatus.newInstance());
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode link status", e);
                return;
            }

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            node.processLinkStatus(linkId, lstatus);
        }

        private void processParameterDefs(ByteBuf buf) {
            int nodeId = buf.readInt();
            /*int linkId =*/ buf.readInt();
            ParameterDefinitionList pdefs;
            try {
                pdefs = ProtoBufUtils.fromByteBuf(buf, ParameterDefinitionList.newInstance());
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode parameter definition", e);
                return;
            }
            log.debug("Got parameter definitions {}", pdefs);

            paramMgr.addParameterDefs(YgwLink.this, nodeId, mdbPath, pdefs);
        }

        /**
         * Called when a command definition message is received from YGW.
         * <p>
         * Register the commands to the MDB (if not already there)
         * <p>
         * TODO: check if the commands are already in the MDB and do something about it (when Yamcs will have the
         * feature to keep track of MDB history)
         */
        private void processCommandDefs(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            YgwNodeLink link = node.getSublink(linkId);
            if (link == null) {
                log.warn("Got message for unknown node/link {}/{}", nodeId, linkId);
                return;
            }
            CommandDefinitionList cdefs;

            try {
                cdefs = ProtoBufUtils.fromByteBuf(buf, CommandDefinitionList.newInstance());
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode parameter definition", e);
                return;
            }
            log.debug("Got command definitions {}", cdefs);

            cmdMgr.addCommandDefs(mdbPath, cdefs, link);
        }

        /**
         * Called when a TC ACK definition message is received from YGW.
         * 
         */
        private void processTcAck(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            YgwNodeLink link = node.getSublink(linkId);
            if (link == null) {
                log.warn("Got message for unknown node/link {}/{}", nodeId, linkId);
                return;
            }
            CommandAck cmdAck;

            try {
                cmdAck = ProtoBufUtils.fromByteBuf(buf, CommandAck.newInstance());
            } catch (InvalidProtocolBufferException e) {
                log.warn("Failed to decode parameter definition", e);
                return;
            }
            log.debug("Got command definitions {}", cmdAck);

            node.processCommandAck(linkId, cmdAck);
        }

        public boolean isConnected() {
            return ctx != null && ctx.channel().isOpen();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Caught exception {}", cause.getMessage());
        }

        public void stop() {
            ctx.close();
        }
    }

}
