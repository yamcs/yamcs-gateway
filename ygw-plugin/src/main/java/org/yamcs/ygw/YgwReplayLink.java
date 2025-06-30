package org.yamcs.ygw;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.yamcs.ConfigurationException;
import org.yamcs.YConfiguration;
import org.yamcs.memento.MementoDb;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.AggregatedDataLink;
import org.yamcs.tctm.Link;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.ygw.ReplayMemento.Interval;
import org.yamcs.ygw.protobuf.Ygw.Event;
import org.yamcs.ygw.protobuf.Ygw.MessageType;
import org.yamcs.ygw.protobuf.Ygw.ParameterData;

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

/**
 * Groups all the sub-links in replay mode
 */
public class YgwReplayLink extends AbstractLink implements AggregatedDataLink {
    final static String MEMENTO_KEY = "YgwReplayLink.intervals";
    final String host;
    final int port;
    final YgwLink ygwLink;
    ReplayMemento intervalQueue = new ReplayMemento();

    Map<Integer, YgwNodeLink> nodes = new HashMap<>();
    YgwChannelHandler replayHandler;
    Interval currentReplayedInterval;

    List<Link> sublinks = new ArrayList<>();
    long lastRn;
    long reconnectionDelay;
    Stream eventReplayStream;

    public YgwReplayLink(YgwLink ygwLink, String host, int replayPort) {
        this.host = host;
        this.port = replayPort;
        this.ygwLink = ygwLink;
        this.reconnectionDelay = ygwLink.reconnectionDelay;
    }

    public void init(String instance, String name, YConfiguration config) {
        super.init(instance, name, config);
        var mementoDb = MementoDb.getInstance(ygwLink.getYamcsInstance());
        intervalQueue = mementoDb.getObject(MEMENTO_KEY, ReplayMemento.class)
                .orElse(new ReplayMemento());
        if (config.containsKey("eventReplayStream")) {
            String eventReplayStreamName = config.getString("eventReplayStream");
            var ydb = YarchDatabase.getInstance(yamcsInstance);
            eventReplayStream = ydb.getStream(config.getString(eventReplayStreamName));

            if (eventReplayStream == null) {
                throw new ConfigurationException("Cannot find stream '" + eventReplayStreamName + "'");
            }
        }

        log.debug("Read from MementoDB intervalQueue: {}", intervalQueue);
    }

    @Override
    public List<Link> getSubLinks() {
        return sublinks;
    }

    @Override
    protected Status connectionStatus() {
        return ygwLink.connectionStatus();
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }

    public void addSubLink(YgwNodeLink nodeLink) {
        nodes.put(nodeLink.nodeId, nodeLink);
        sublinks.add(nodeLink);
    }

    @Override
    public AggregatedDataLink getParent() {
        return ygwLink;
    }

    public void scheduleReplay(long startRn, long stopRn) {
        NioEventLoopGroup workerGroup = getEventLoop();
        workerGroup.submit(() -> {
            intervalQueue.addLast(new Interval(startRn, stopRn, 0));
            var mementoDb = MementoDb.getInstance(ygwLink.getYamcsInstance());
            mementoDb.putObject(MEMENTO_KEY, intervalQueue);

            runNextReplay();
        });
    }

    private void runNextReplay() {
        if (isEffectivelyDisabled()) {
            return;
        }

        if (currentReplayedInterval != null) {
            return;
        }

        currentReplayedInterval = intervalQueue.poll();
        if (currentReplayedInterval == null) {
            return;
        }

        replayHandler = new YgwChannelHandler();
        NioEventLoopGroup workerGroup = getEventLoop();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(YgwLink.MAX_PACKET_LENGTH, 0, 4));
                ch.pipeline().addLast(replayHandler);
            }
        });
        b.connect(host, port).addListener(f -> {
            if (!f.isSuccess()) {
                eventProducer
                        .sendWarning("Failed to connect to the Yamcs gateway for replay: " + f.cause().getMessage());
                intervalQueue.addFirst(currentReplayedInterval);
                if (reconnectionDelay > 0) {
                    workerGroup.schedule(() -> runNextReplay(), reconnectionDelay,
                            TimeUnit.MILLISECONDS);
                }
            } else {
                log.info("Connected to the Yamcs gateway for replay at {}:{}", host, port);
                replayHandler.sendReplayRequestMessage(currentReplayedInterval);
            }
        });
    }

    /**
     * Called by {@link YgwLink} when a list of nodes is received from the Yamcs Gateway
     */
    public void cleanNodes() {
        sublinks.clear();
        nodes.clear();
    }

    public void cleanReplays() {
        var rh = replayHandler;
        if (rh != null) {
            rh.ctx.close();
        }
        NioEventLoopGroup workerGroup = getEventLoop();
        workerGroup.submit(() -> {
            intervalQueue.clear();
            var mementoDb = MementoDb.getInstance(ygwLink.getYamcsInstance());
            mementoDb.putObject(MEMENTO_KEY, intervalQueue);
        });
    }

    @Override
    public long getDataInCount() {
        return sublinks.stream().mapToLong(l -> l.getDataInCount()).sum();
    }

    class YgwChannelHandler extends ChannelInboundHandlerAdapter {
        ChannelHandlerContext ctx;

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("Yamcs gateway replay finished");
            replayHandler = null;
            if (lastRn < currentReplayedInterval.stopRn()) {
                log.warn("The replay stopped before retrieving all records: lastRn={}, expected stop: {}", lastRn,
                        currentReplayedInterval.stopRn());
                if (currentReplayedInterval.numFailures() < 3) {
                    intervalQueue.addFirst(new Interval(lastRn + 1, currentReplayedInterval.stopRn(),
                            currentReplayedInterval.numFailures() + 1));
                }
            }
            var mementoDb = MementoDb.getInstance(ygwLink.getYamcsInstance());
            mementoDb.putObject(MEMENTO_KEY, intervalQueue);

            currentReplayedInterval = null;
            runNextReplay();
        }

        public ChannelFuture sendMessage(byte msgType, int nodeId, int linkId, byte[] data) {
            ByteBuf buf = Unpooled.buffer(14 + data.length);
            buf.writeInt(10 + data.length);
            buf.writeByte(YgwLink.VERSION);
            buf.writeByte(msgType);

            buf.writeInt(nodeId);
            buf.writeInt(linkId);
            buf.writeBytes(data);

            return ctx.writeAndFlush(buf);
        }

        private ChannelFuture sendReplayRequestMessage(Interval interval) {
            var rr = org.yamcs.ygw.protobuf.Ygw.ReplayRequest.newInstance().setStartRn(interval.startRn())
                    .setStopRn(interval.stopRn());
            var data = rr.toByteArray();

            ByteBuf buf = Unpooled.buffer(5 + data.length);
            buf.writeInt(data.length + 1);
            buf.writeByte(YgwLink.VERSION);
            buf.writeBytes(data);
            log.info("Sending replay request for {}", interval);
            return ctx.writeAndFlush(buf);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buf = (ByteBuf) msg;
            buf.readInt();// length
            byte version = buf.readByte();
            if (version != YgwLink.VERSION) {
                log.warn("Got mesage with version {}, expected {}; closing connection", version, YgwLink.VERSION);
                ctx.close();
                return;
            }

            // recording number
            long rn = buf.readLong();
            lastRn = rn;

            byte type = buf.readByte();

            try {
                if (type == MessageType.TM_PACKET_VALUE) {
                    processTmPacket(buf);
                } else if (type == MessageType.TM_FRAME_VALUE) {
                    processTmFrame(buf);
                } else if (type == MessageType.PARAMETER_DATA_VALUE) {
                    processParameters(buf);
                } else if (type == MessageType.EVENT_VALUE) {
                    processEvent(buf);
                } // else ignore
            } catch (Exception e) {
                log.error("Exception processing message", e);
            }
            buf.release();
        }

        /**
         * Called when TM packet messages are received from YGW
         */
        private void processTmPacket(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            node.processTmPacket(linkId, buf);
        }

        /**
         * Called when TM frame messages are received from YGW
         */
        private void processTmFrame(ByteBuf buf) {
            int nodeId = buf.readInt();
            int linkId = buf.readInt();

            YgwNodeLink node = nodes.get(nodeId);
            if (node == null) {
                log.warn("Got message for unknown node {}", nodeId);
                return;
            }
            node.processTmFrame(linkId, buf);
        }

        /**
         * Called when event messages are received from YGW
         */
        private void processEvent(ByteBuf buf) {
            if (eventReplayStream == null) {
                log.debug("Received replay event but no event stream is defined; ignored");
                return;
            }
            /* int nodeId = */ buf.readInt();
            /* int linkId = */ buf.readInt();

            try {
                Event ev = ProtoBufUtils.fromByteBuf(buf, Event.newInstance());
                var yev = ProtoConverter.toYamcsEvent(timeService, ev);

                TupleDefinition tdef = eventReplayStream.getDefinition();
                Tuple t = new Tuple(tdef, new Object[] { yev.getGenerationTime(),
                        yev.getSource(), yev.getSeqNumber(), yev });
                eventReplayStream.emitTuple(t);

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
            var pvList = ygwLink.processParameters(nodeId, pdata);

            node.processParameters(linkId, pdata.getGroup(), pdata.getSeqNum(), pvList);
        }

        public boolean isConnected() {
            return ctx != null && ctx.channel().isOpen();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            log.warn("Caught exception {}", cause.getMessage());
        }

        public void stop() {
            ctx.close();
        }
    }

}
