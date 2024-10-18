package org.yamcs.ygw;

import static org.yamcs.cmdhistory.CommandHistoryPublisher.AcknowledgeSent_KEY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.tctm.AbstractTcTmParamLink;
import org.yamcs.tctm.AggregatedDataLink;
import org.yamcs.tctm.Link;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.ygw.protobuf.Ygw.CommandAck;
import org.yamcs.ygw.protobuf.Ygw.LinkStatus;
import org.yamcs.ygw.protobuf.Ygw.MessageType;
import org.yamcs.ygw.protobuf.Ygw.Node;

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

    /** the top link known by Yamcs - corresponds to the connection to the gateway */
    final YgwLink ygwLink;

    /** only for sub-links - the parent node link */
    final YgwNodeLink parent;

    final boolean tmEnabled;
    final boolean tcEnabled;
    final Map<Integer, YgwNodeLink> subLinks = new HashMap<>();

    // stores the link status received from the gateway
    volatile LinkStatus linkStatus;

    /**
     * 
     * Builds an object corresponding to a gateway node.
     */
    public YgwNodeLink(YgwLink ygwLink, Node node) {
        this.ygwLink = ygwLink;
        this.nodeId = node.getId();
        this.name = node.getName();
        this.description = node.getDescription();
        this.linkId = 0;
        this.tmEnabled = node.hasTm() && node.getTm();
        this.tcEnabled = node.hasTc() && node.getTc();
        this.parent = null;
    }

    /**
     * 
     * Builds an object corresponding to a gateway sub-link.
     */
    public YgwNodeLink(YgwLink ygwLink, YgwNodeLink parent, org.yamcs.ygw.protobuf.Ygw.Link link) {
        this.ygwLink = ygwLink;
        this.nodeId = parent.nodeId;
        this.parent = parent;
        this.linkId = link.getId();
        this.name = link.getName();
        this.description = link.getDescription();
        this.tmEnabled = link.hasTm() && link.getTm();
        this.tcEnabled = link.hasTc() && link.getTc();
    }

    @Override
    public void init(String instance, String name, YConfiguration config) {
        super.init(instance, name, config);
    }

    @Override
    protected Status connectionStatus() {
        if (ygwLink.connectionStatus() != Status.OK) {
            return ygwLink.connectionStatus();
        }
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
        pvList.stream().collect(Collectors.groupingBy(ParameterValue::getGenerationTime))
                .forEach((t, l) -> parameterSink.updateParameters(t, group, seqNum, pvList));
    }

    public void processTm(int linkId, ByteBuf buf) {
        if (linkId != this.linkId) {
            var subLink = subLinks.get(linkId);
            if (subLink == null) {
                log.warn("Received TM packet on an unexisting sublink {}; ignoring", linkId);
            } else {
                subLink.processTm(linkId, buf);
            }
            return;
        }

        if (!tmEnabled) {
            log.warn("Received TM packet on a link {} not enabled for TM packets; ignoring", getName());
            return;
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

    public YgwNodeLink getSublink(int linkId) {
        if (linkId == 0) {
            return this;
        }
        return subLinks.get(linkId);
    }

    public void processLinkStatus(int linkId, LinkStatus lstatus) {
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
        return tmEnabled;
    }

    @Override
    public boolean isTcDataLinkImplemented() {
        return tcEnabled;
    }

    @Override
    public boolean isParameterDataLinkImplemented() {
        return true;
    }

    public void addSublink(int linkId, YgwNodeLink nodeSublink) {
        subLinks.put(linkId, nodeSublink);

    }

    @Override
    public List<Link> getSubLinks() {
        return new ArrayList<Link>(subLinks.values());
    }
}
