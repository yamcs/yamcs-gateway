package org.yamcs.ygw;

import org.yamcs.tctm.ccsds.AbstractTcFrameLink;
import org.yamcs.tctm.ccsds.TcTransferFrame;
import org.yamcs.utils.StringConverter;

/**
 * This is used as a member in the YgwNodeLink in order to inherit all the functionality from the AbstractTcFrameLink
 */
public class TcFrameHandler extends AbstractTcFrameLink implements Runnable {
    final YgwNodeLink nodeLink;

    TcFrameHandler(YgwNodeLink nodeLink) {
        this.nodeLink = nodeLink;
    }

    public void run() {
        while (isRunningAndEnabled()) {
            TcTransferFrame tf = multiplexer.getFrame();
            if (tf != null) {
                byte[] data = tf.getData();
                if (log.isTraceEnabled()) {
                    log.trace("Outgoing frame data: {}", StringConverter.arrayToHexString(data, true));
                }

                if (cltuGenerator != null) {
                    data = encodeCltu(tf.getVirtualChannelId(), data);

                    if (log.isTraceEnabled()) {
                        log.trace("Outgoing CLTU: {}", StringConverter.arrayToHexString(data, true));
                    }
                }

                nodeLink.sendTcFrame(data).whenComplete((c, t) -> {
                    if (t != null) {
                        if (tf.isBypass()) {
                            failBypassFrame(tf, t.getMessage());
                        }
                        log.warn("Error sending command frame", t);
                    } else {
                        ackBypassFrame(tf);
                    }
                });
                frameCount++;
            }
        }
    }

    @Override
    protected Status connectionStatus() {
        return nodeLink.connectionStatus();
    }

    @Override
    protected void doStart() {
        new Thread(this).start();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        notifyStopped();
    }
}
