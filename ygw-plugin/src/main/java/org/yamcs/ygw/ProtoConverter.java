package org.yamcs.ygw;

import java.util.Map.Entry;

import org.yamcs.commanding.ArgumentValue;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.time.TimeService;
import org.yamcs.xtce.Argument;
import org.yamcs.ygw.protobuf.Ygw.AggregateValue;
import org.yamcs.ygw.protobuf.Ygw.ArrayValue;
import org.yamcs.ygw.protobuf.Ygw.CommandAssignment;
import org.yamcs.ygw.protobuf.Ygw.EnumeratedValue;
import org.yamcs.ygw.protobuf.Ygw.Event;
import org.yamcs.ygw.protobuf.Ygw.Timestamp;
import org.yamcs.ygw.protobuf.Ygw.Value;

public class ProtoConverter {
    static org.yamcs.ygw.protobuf.Ygw.PreparedCommand toProto(PreparedCommand pc) {
        var qpc = org.yamcs.ygw.protobuf.Ygw.PreparedCommand.newInstance()
                .setCommandId(toProto(pc.getCommandId()));

        if (pc.getBinary() != null) {
            qpc.setBinary(pc.getBinary());
        }
        if (pc.getArgAssignment() != null) {
            for (Entry<Argument, ArgumentValue> entry : pc.getArgAssignment().entrySet()) {
                qpc.addAssignments(toProto(entry.getKey().getName(), entry.getValue()));
            }
        }

        return qpc;

    }

    static org.yamcs.ygw.protobuf.Ygw.CommandId toProto(CommandId cmdId) {
        var qcmdId = org.yamcs.ygw.protobuf.Ygw.CommandId.newInstance()
                .setGenerationTime(cmdId.getGenerationTime())
                .setOrigin(cmdId.getOrigin())
                .setSequenceNumber(cmdId.getSequenceNumber());
        if (cmdId.hasCommandName()) {
            qcmdId.setCommandName(cmdId.getCommandName());
        }
        return qcmdId;
    }

    static CommandAssignment toProto(String argName, ArgumentValue argValue) {
        var qca = CommandAssignment.newInstance().setName(argName);
        if (argValue.getEngValue() != null) {
            qca.setEngValue(toProto(argValue.getEngValue()));
        }
        if (argValue.getRawValue() != null) {
            qca.setEngValue(toProto(argValue.getRawValue()));
        }
        return qca;

    }

    static Value toProto(org.yamcs.parameter.Value v) {
        var qv = Value.newInstance();

        if (v instanceof org.yamcs.parameter.AggregateValue) {
            qv.setAggregateValue(toProto((org.yamcs.parameter.AggregateValue) v));
        } else if (v instanceof org.yamcs.parameter.ArrayValue) {
            qv.setArrayValue(toProto((org.yamcs.parameter.ArrayValue) v));
        } else if (v instanceof org.yamcs.parameter.BinaryValue) {
            qv.setBinaryValue(v.getBinaryValue());
        } else if (v instanceof org.yamcs.parameter.BooleanValue) {
            qv.setBooleanValue(v.getBooleanValue());
        } else if (v instanceof org.yamcs.parameter.DoubleValue) {
            qv.setDoubleValue(v.getDoubleValue());
        } else if (v instanceof org.yamcs.parameter.EnumeratedValue) {
            qv.setEnumeratedValue(toProto((org.yamcs.parameter.EnumeratedValue) v));
        } else if (v instanceof org.yamcs.parameter.FloatValue) {
            qv.setFloatValue(v.getFloatValue());
        } else if (v instanceof org.yamcs.parameter.SInt32Value) {
            qv.setSint32Value(v.getSint32Value());
        } else if (v instanceof org.yamcs.parameter.SInt64Value) {
            qv.setSint64Value(v.getSint64Value());
        } else if (v instanceof org.yamcs.parameter.StringValue) {
            qv.setStringValue(v.getStringValue());
        } else if (v instanceof org.yamcs.parameter.TimestampValue) {
            qv.setTimestampValue(toProto((org.yamcs.parameter.TimestampValue) v));
        } else if (v instanceof org.yamcs.parameter.UInt32Value) {
            qv.setUint32Value(v.getUint32Value());
        } else if (v instanceof org.yamcs.parameter.UInt64Value) {
            qv.setUint64Value(v.getUint64Value());
        } else {
            throw new IllegalStateException("Unknown value type " + v.getClass());
        }

        return qv;
    }

    static AggregateValue toProto(org.yamcs.parameter.AggregateValue av) {
        var qav = AggregateValue.newInstance();
        int n = av.numMembers();
        for (int i = 0; i < n; i++) {
            var mv = av.getMemberValue(i);
            if (mv != null) {
                qav.addName(av.getMemberName(i));
                qav.addValue(toProto(mv));
            }
        }
        return qav;
    }

    static org.yamcs.parameter.Value fromProto(Value qv) {
        if (qv.hasBooleanValue()) {
            return new org.yamcs.parameter.BooleanValue(qv.getBooleanValue());
        } else if (qv.hasDoubleValue()) {
            return new org.yamcs.parameter.DoubleValue(qv.getDoubleValue());
        } else if (qv.hasFloatValue()) {
            return new org.yamcs.parameter.FloatValue(qv.getFloatValue());
        } else if (qv.hasSint32Value()) {
            return new org.yamcs.parameter.SInt32Value(qv.getSint32Value());
        } else if (qv.hasSint64Value()) {
            return new org.yamcs.parameter.SInt64Value(qv.getSint64Value());
        } else if (qv.hasStringValue()) {
            return new org.yamcs.parameter.StringValue(qv.getStringValue());
        } else if (qv.hasUint32Value()) {
            return new org.yamcs.parameter.UInt32Value(qv.getUint32Value());
        } else if (qv.hasUint64Value()) {
            return new org.yamcs.parameter.UInt64Value(qv.getUint64Value());
        } else {
            throw new IllegalStateException("TODO " + qv);
        }
    }

    static EnumeratedValue toProto(org.yamcs.parameter.EnumeratedValue ev) {
        return EnumeratedValue.newInstance().setSint64Value(ev.getSint64Value()).setStringValue(ev.getStringValue());
    }

    static ArrayValue toProto(org.yamcs.parameter.ArrayValue av) {
        var qav = ArrayValue.newInstance();

        int n = av.flatLength();
        for (int i = 0; i < n; i++) {
            qav.addValue(toProto(av.getElementValue(i)));
        }
        return qav;
    }

    static Timestamp toProto(org.yamcs.parameter.TimestampValue tv) {
        return Timestamp.newInstance().setMillis(tv.millis()).setPicos(tv.picos());
    }

    static org.yamcs.yarch.protobuf.Db.Event toYamcsEvent(TimeService timeService, Event ev) {
        org.yamcs.yarch.protobuf.Db.Event.Builder yevb = org.yamcs.yarch.protobuf.Db.Event.newBuilder()
                .setMessage(ev.getMessage())
                .setGenerationTime(ev.getGenerationTime())
                .setSeqNumber(ev.getSeqNumber());

        if (ev.hasSource()) {
            yevb.setSource(ev.getSource());

        }
        yevb.setGenerationTime(timeService.getMissionTime());

        return yevb.build();
    }

    public static long fromProtoMillis(Timestamp t) {
        // FIXME
        return t.getMillis() + 37000;
    }

    public static Timestamp toProtoTimestamp(long instant) {
        return Timestamp.newInstance().setMillis(instant - 37000);
    }
}
