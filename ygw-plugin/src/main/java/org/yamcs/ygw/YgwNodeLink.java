package org.yamcs.ygw;

import static org.yamcs.cmdhistory.CommandHistoryPublisher.AcknowledgeSent_KEY;
import static org.yamcs.tctm.ccsds.AbstractTmFrameLink.TM_FRAME_CONFIG_SECTION;
import static org.yamcs.tctm.ccsds.AbstractTcFrameLink.TC_FRAME_CONFIG_SECTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.tctm.AbstractTcTmParamLink;
import org.yamcs.tctm.AggregatedDataLink;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.TcTmException;
import org.yamcs.tctm.ccsds.MasterChannelFrameHandler;
import org.yamcs.tctm.ccsds.MasterChannelFrameMultiplexer;
import org.yamcs.tctm.ccsds.VcDownlinkHandler;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.ygw.protobuf.Ygw.CommandAck;
import org.yamcs.ygw.protobuf.Ygw.LinkStatus;
import org.yamcs.ygw.protobuf.Ygw.MessageType;
import org.yamcs.ygw.protobuf.Ygw.Node;
import org.yamcs.ygw.protobuf.Ygw.TcFrame;
import org.yamcs.cmdhistory.StreamCommandHistoryPublisher;

import io.netty.buffer.ByteBuf;

/**
 * Corresponds to a Node in the Yamcs Gateway or to a sub-link (in the gateway each node can have zero or more
 * sub-links)
 * <p>
 * Each sub-link has an id and the node itself functions as a link with id 0
 */
public class YgwNodeLink extends AbstractTcTmParamLink implements AggregatedDataLink {
    /** node id */
    final int nodeId;

    /** link id - by convention it is 0 for the node itself */
    final int linkId;

    /** node or sub-link name */
    final String name;

    /** node or sub-link description */
    final String description;

    // if true, this is a replay link
    final boolean replay;

    /** the top link known by Yamcs - corresponds to the connection to the gateway */
    final YgwLink ygwLink;

    /** the parent node link */
    final AggregatedDataLink parent;

    final boolean tmPacketEnabled;
    final boolean tcEnabled;
    final boolean tmFrameEnabled;
    final boolean tcFrameEnabled;

    String tmStreamName;
    String ppStreamName;

    final Map<Integer, YgwNodeLink> subLinks = new HashMap<>();

    // stores the link status received from the gateway
    volatile LinkStatus linkStatus;

    MasterChannelFrameHandler tmFrameHandler;
    ArrayList<Link> vcSublinks;
    MasterChannelFrameMultiplexer tmFrameMultiplexer;

    TcFrameHandler tcFrameHandler;

    /**
     * 
     * Builds an object corresponding to a gateway node.
     */
    public YgwNodeLink(YgwLink ygwLink, Node node, AggregatedDataLink parent, boolean replay) {
        this.ygwLink = ygwLink;
        this.nodeId = node.getId();
        this.replay = replay;
        this.parent = parent;

        this.name = node.getName();
        this.description = node.getDescription();
        this.linkId = 0;
        this.tmPacketEnabled = node.hasTmPacket() && node.getTmPacket();
        this.tcEnabled = !replay && node.hasTc() && node.getTc();
        this.tmFrameEnabled = node.hasTmFrame() && node.getTmFrame();
        this.tcFrameEnabled = node.hasTcFrame() && node.getTcFrame();

        if (this.tmPacketEnabled && this.tmFrameEnabled) {
            throw new IllegalArgumentException(
                    "Cannot enable both TM packet and frames for link " + name + ". Check the ygw configuration");
        }

        if (this.tcEnabled && this.tcFrameEnabled) {
            throw new IllegalArgumentException(
                    "Cannot enable both TC packet and frames for link " + name + ". Check the ygw configuration");
        }

    }

    /**
     * 
     * Builds an object corresponding to a gateway sub-link.
     * <p>
     * If replayLink is not null, this is used for replays
     */
    public YgwNodeLink(YgwLink ygwLink, YgwNodeLink parent, org.yamcs.ygw.protobuf.Ygw.Link link,
            boolean replay) {
        this.ygwLink = ygwLink;
        this.nodeId = parent.nodeId;
        this.parent = parent;
        this.linkId = link.getId();
        this.name = link.getName();
        this.replay = replay;
        this.description = link.getDescription();
        this.tmPacketEnabled = link.hasTmPacket() && link.getTmPacket();
        this.tcEnabled = link.hasTc() && link.getTc();
        this.tmFrameEnabled = link.hasTmFrame() && link.getTmFrame();
        this.tcFrameEnabled = link.hasTcFrame() && link.getTcFrame();
        if (this.tmPacketEnabled && this.tmFrameEnabled) {
            throw new IllegalArgumentException(
                    "Cannot enable both TM packet and frames for link " + name + ". Check the ygw configuration");
        }

        if (this.tcEnabled && this.tcFrameEnabled) {
            throw new IllegalArgumentException(
                    "Cannot enable both TC packet and frames for link " + name + ". Check the ygw configuration");
        }
    }

    @Override
    public void init(String instance, String name, YConfiguration config) {
        super.init(instance, name, config);
        if (tmPacketEnabled || tmFrameEnabled) {
            tmStreamName = replay ? config.getString("tmReplayStream") : config.getString("tmStream");
        }
        ppStreamName = replay ? config.getString("ppReplayStream", null) : config.getString("ppStream", null);

        if (tmFrameEnabled) {
            if (!config.containsKey(TM_FRAME_CONFIG_SECTION)) {
                log.warn("Node " + name
                        + " wants to send TM frames but there is no configuration under the " + TM_FRAME_CONFIG_SECTION
                        + " keyword in the link section");
                return;
            }
            var tmFrameConfig = config.getConfig("tmFrameConfig");
            tmFrameHandler = new MasterChannelFrameHandler(yamcsInstance, name + ".tm", tmFrameConfig);
            vcSublinks = new ArrayList<Link>();
            for (VcDownlinkHandler vch : tmFrameHandler.getVcHandlers()) {
                if (vch instanceof Link) {
                    Link l = (Link) vch;
                    vcSublinks.add(l);
                    l.setParent(this);
                }
            }
        }
        if (tcFrameEnabled) {
            if (!config.containsKey(TC_FRAME_CONFIG_SECTION)) {
                log.warn("Node " + name
                        + " wants to receive TC frames but there is no configuration under the "
                        + TC_FRAME_CONFIG_SECTION + " keyword in the link section");
                return;
            }
            var tcFrameConfig = config.getConfig("tcFrameConfig");
            tcFrameHandler = new TcFrameHandler(this);
            tcFrameHandler.init(instance, name + ".tc", tcFrameConfig);
            for (var l : tcFrameHandler.getSubLinks()) {
                vcSublinks.add(l);
                l.setParent(this);
            }
            if (commandHistoryPublisher == null) {
                commandHistoryPublisher = new StreamCommandHistoryPublisher(yamcsInstance);
            }
            tcFrameHandler.setCommandHistoryPublisher(commandHistoryPublisher);
            tcFrameHandler.startAsync();
        }
    }

    @Override
    protected Status connectionStatus() {
        if (ygwLink.connectionStatus() != Status.OK) {
            return ygwLink.connectionStatus();
        }
        if (replay) {
            return parent.getLinkStatus();
        } else {
            if (linkStatus == null) {
                return Status.UNAVAIL;
            }

            switch (linkStatus.getState()) {
            case DISABLED:
                return Status.DISABLED;
            case FAILED:
                return Status.FAILED;
            case OK:
                return Status.OK;
            case UNAVAIL:
                return Status.UNAVAIL;
            default:
                throw new IllegalStateException();
            }
        }
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }

    public void processParameters(int linkId, String group, int seqNum, List<ParameterValue> pvList) {
        if (linkId != this.linkId) {
            var subLink = subLinks.get(linkId);
            if (subLink == null) {
                log.warn("Received parameters packet on an unexisting sublink {}; ignoring", linkId);
            } else {
                subLink.processParameters(linkId, group, seqNum, pvList);
            }
            return;
        }
        if (parameterSink == null) {
            log.warn(
                    "Received parameters on a link {} not enabled for parameters; ignoring."
                            + "To enable parameters, please use ppStream: pp_realtime in the link configuration",
                    getName());
            return;
        }

        if (replay) {
            dataIn(pvList.size(), pvList.size());
        }

        pvList.stream().collect(Collectors.groupingBy(ParameterValue::getGenerationTime))
                .forEach((t, l) -> parameterSink.updateParameters(t, group, seqNum, pvList));
    }

    public void processTmPacket(int linkId, ByteBuf buf) {
        if (linkId != this.linkId) {
            var subLink = subLinks.get(linkId);
            if (subLink == null) {
                log.warn("Received TM packet on an unexisting sublink {}; ignoring", linkId);
            } else {
                subLink.processTmPacket(linkId, buf);
            }
            return;
        }

        if (!tmPacketEnabled) {
            log.warn("Received TM packet on a link {} not enabled for TM packets; ignoring", getName());
            return;
        }
        if (replay) {
            dataIn(1, buf.readableBytes());
        }

        long rectime = timeService.getMissionTime();

        long millis = buf.readLong();
        int picos = buf.readInt();

        org.yamcs.time.Instant ert = TimeEncoding.fromUnixPicos(millis, picos);

        byte[] pktData = new byte[buf.readableBytes()];
        buf.readBytes(pktData);

        TmPacket pkt = new TmPacket(rectime, pktData);
        pkt.setEarthReceptionTime(ert);

        packetCount.incrementAndGet();
        pkt = packetPreprocessor.process(pkt);
        if (pkt != null) {
            processPacket(pkt);
        }
    }

    public void processTmFrame(int linkId, ByteBuf buf) {
        if (linkId != this.linkId) {
            var subLink = subLinks.get(linkId);
            if (subLink == null) {
                log.warn("Received TM frame on an unexisting sublink {}; ignoring", linkId);
            } else {
                subLink.processTmFrame(linkId, buf);
            }
            return;
        }

        if (!tmFrameEnabled) {
            log.warn("Received TM frame on a link {} not enabled for TM frames; ignoring", getName());
            return;
        }
        if (tmFrameHandler == null) {
            log.warn("Received frame but have no handler");
            return;
        }

        if (replay) {
            dataIn(1, buf.readableBytes());
        }

        long millis = buf.readLong();
        int picos = buf.readInt();

        org.yamcs.time.Instant ert = TimeEncoding.fromUnixPicos(millis, picos);

        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        try {
            tmFrameHandler.handleFrame(ert, data, 0, data.length);
        } catch (TcTmException e) {
            eventProducer.sendWarning("Error processing frame: " + e.toString());
        }
    }

    public YgwNodeLink getSublink(int linkId) {
        if (linkId == 0) {
            return this;
        }
        return subLinks.get(linkId);
    }

    public void processLinkStatus(int linkId, LinkStatus lstatus) {
        assert (!replay);

        if (linkId != this.linkId) {
            var subLink = subLinks.get(linkId);
            if (subLink == null) {
                log.warn("Received link status on an unexisting sublink {}; ignoring", linkId);
            } else {
                subLink.processLinkStatus(linkId, lstatus);
            }
            return;
        }

        if (linkStatus == null) {
            // first link status received
            // cannot use the dataIn and dataOut because cannot infer the data rates
            this.dataInCount.set(lstatus.getDataInCount());
            this.dataOutCount.set(lstatus.getDataOutCount());
        } else {
            var inCount = lstatus.getDataInCount() - linkStatus.getDataInCount();
            var inSize = lstatus.getDataInSize() - linkStatus.getDataInSize();
            if (inCount < 0) {
                dataInCount.set(0);
                dataIn(lstatus.getDataInCount(), inSize);
            } else {
                dataIn(inCount, lstatus.getDataInSize());
            }

            var outCount = lstatus.getDataOutCount() - linkStatus.getDataOutCount();
            var outSize = lstatus.getDataOutSize() - linkStatus.getDataOutSize();
            if (outCount < 0) {
                dataOutCount.set(0);
                dataOut(lstatus.getDataOutCount(), lstatus.getDataOutSize());
            } else {
                dataOut(outCount, outSize);
            }
        }
        this.linkStatus = lstatus;
    }

    @Override
    public boolean sendCommand(PreparedCommand pc) {
        return sendCommand(pc, null);
    }

    /**
     * 
     * send a command
     * <p>
     * relativeName is set for the commands registered from the gateway
     */
    public boolean sendCommand(PreparedCommand pc, Integer ygwCmdId) {
        if (!ygwLink.isConnected()) {
            return false;
        }

        byte[] binary = pc.getBinary();
        if (binary != null && !pc.disablePostprocessing()) {
            binary = cmdPostProcessor.process(pc);
            if (binary == null) {
                log.warn("command postprocessor did not process the command");
            }
            pc.setBinary(binary);
        }

        var protoPc = ProtoConverter.toProto(pc, ygwCmdId);

        long time = getCurrentTime();

        ygwLink.sendMessage((byte) MessageType.TC_VALUE, nodeId, linkId, protoPc.toByteArray())
                .whenComplete((c, t) -> {

                    if (t != null) {
                        log.warn("Error sending command ", t);
                        failedCommand(pc.getCommandId(), t.getMessage());
                    } else {
                        commandHistoryPublisher.publishAck(pc.getCommandId(), AcknowledgeSent_KEY, time, AckStatus.OK);
                    }
                });

        return true;
    }

    public CompletableFuture<Void> sendTcFrame(byte[] data) {
        var tcFrameMsg = TcFrame.newInstance().setBinary(data);
        return ygwLink.sendMessage((byte) MessageType.TC_FRAME_VALUE, nodeId, linkId, tcFrameMsg.toByteArray());
    }

    public void processCommandAck(int linkId, CommandAck cmdAck) {
        var cmdId = ProtoConverter.fromProto(cmdAck.getCommandId());
        var time = ProtoConverter.fromProtoMillis(cmdAck.getTime());
        var ack = ProtoConverter.fromProto(cmdAck.getAck());
        String message = cmdAck.hasMessage() ? cmdAck.getMessage() : null;
        ParameterValue returnPv = null;
        if (cmdAck.hasReturnPv()) {
            var qpv = cmdAck.getReturnPv();
            var paramMgr = ygwLink.getParameterManager();
            var ygwp = paramMgr.getYgwParameter(ygwLink, nodeId, qpv.getId());
            if (ygwp == null) {
                log.warn("Received a returnPv for an unkown parameter: {}", qpv);
            } else {
                returnPv = ProtoConverter.fromProto(ygwp.p, qpv, time, timeService.getMissionTime());
            }

        }

        commandHistoryPublisher.publishAck(cmdId, cmdAck.getKey(), time, ack,
                message, returnPv);
    }

    @Override
    public AggregatedDataLink getParent() {
        return ygwLink;
    }

    @Override
    public boolean isTmPacketDataLinkImplemented() {
        return tmPacketEnabled;
    }

    @Override
    public boolean isTcDataLinkImplemented() {
        return tcEnabled;
    }

    @Override
    public boolean isParameterDataLinkImplemented() {
        return ppStreamName != null;
    }

    public void addSublink(int linkId, YgwNodeLink nodeSublink) {
        subLinks.put(linkId, nodeSublink);

    }

    @Override
    public List<Link> getSubLinks() {
        var l = new ArrayList<Link>(subLinks.values());
        if (vcSublinks != null) {
            l.addAll(vcSublinks);
        }
        return l;
    }

    @Override
    public String getTmStreamName() {
        return tmStreamName;
    }

    @Override
    public String getPpStreamName() {
        return ppStreamName;
    }
}
