package org.yamcs.ygw;

import java.lang.invoke.MethodHandles.Lookup.ClassOption;
import java.util.Map.Entry;

import org.yamcs.CommandOption.CommandOptionType;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.ArgumentValue;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.protobuf.Commanding.CommandHistoryAttribute;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.time.TimeService;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.xtce.Argument;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.util.AggregateMemberNames;
import org.yamcs.ygw.ParameterPool.YgwParameter;
import org.yamcs.ygw.protobuf.Ygw.AggregateValue;
import org.yamcs.ygw.protobuf.Ygw.ArrayValue;

import org.yamcs.ygw.protobuf.Ygw.CommandAssignment;
import org.yamcs.ygw.protobuf.Ygw.CommandOption;
import org.yamcs.ygw.protobuf.Ygw.EnumeratedValue;
import org.yamcs.ygw.protobuf.Ygw.Event;
import org.yamcs.ygw.protobuf.Ygw.PreparedCommand.ExtraEntry;
import org.yamcs.ygw.protobuf.Ygw.Timestamp;
import org.yamcs.ygw.protobuf.Ygw.Value;

public class ProtoConverter {
    static org.yamcs.ygw.protobuf.Ygw.PreparedCommand toProto(PreparedCommand pc, Integer ygwCmdId) {
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
        if (ygwCmdId != null) {
            qpc.setYgwCmdId(ygwCmdId);
        }

        if (pc.getAttributes() != null && !pc.getAttributes().isEmpty()) {
            for (var cha : pc.getAttributes()) {
                qpc.addExtra(toProto(cha));
            }
        }
        return qpc;
    }

    private static ExtraEntry toProto(CommandHistoryAttribute cha) {
        return ExtraEntry.newInstance().setKey(cha.getName()).setValue(toProto(cha.getValue()));
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

    static CommandId fromProto(org.yamcs.ygw.protobuf.Ygw.CommandId cmdId) {
        var cmdIdb = CommandId.newBuilder().setGenerationTime(cmdId.getGenerationTime())
                .setOrigin(cmdId.getOrigin()).setSequenceNumber(cmdId.getSequenceNumber());

        if (cmdId.hasCommandName()) {
            cmdIdb.setCommandName(cmdId.getCommandName());
        }

        return cmdIdb.build();
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

    private static Value toProto(org.yamcs.protobuf.Yamcs.Value v) {
        var qv = Value.newInstance();
        if (v.getType() == Type.STRING) {
            qv.setStringValue(v.getStringValue());
        } else if (v.getType() == Type.BINARY) {
            qv.setBinaryValue(v.getBinaryValue().toByteArray());
        } else if (v.getType() == Type.BOOLEAN) {
            qv.setBooleanValue(v.getBooleanValue());
        } else if (v.getType() == Type.DOUBLE) {
            qv.setDoubleValue(v.getDoubleValue());
        } else if (v.getType() == Type.FLOAT) {
            qv.setFloatValue(v.getFloatValue());
        } else if (v.getType() == Type.SINT32) {
            qv.setSint32Value(v.getSint32Value());
        } else if (v.getType() == Type.SINT64) {
            qv.setSint64Value(v.getSint64Value());
        } else if (v.getType() == Type.UINT32) {
            qv.setUint32Value(v.getUint32Value());
        } else if (v.getType() == Type.UINT64) {
            qv.setUint64Value(v.getUint64Value());
        } else {
            throw new IllegalStateException("Value type " + v.getType() + " not supported");
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
        } else if (qv.hasEnumeratedValue()) {
            var pev = qv.getEnumeratedValue();
            return new org.yamcs.parameter.EnumeratedValue(pev.getSint64Value(), pev.getStringValue());
        } else if (qv.hasAggregateValue()) {
            return fromProto(qv.getAggregateValue());
        } else if (qv.hasArrayValue()) {
            return fromProto(qv.getArrayValue());
        } else {
            throw new IllegalStateException("TODO " + qv);
        }
    }

    private static org.yamcs.parameter.AggregateValue fromProto(AggregateValue aggregateValue) {
        var names = aggregateValue.getName();
        var values = aggregateValue.getValue();
        if (names.length() != aggregateValue.getValue().length()) {
            throw new IllegalArgumentException("Invalid aggregate value, name count different than value count");
        }
        String[] memberNames = new String[names.length()];
        for (int i = 0; i < names.length(); i++) {
            memberNames[i] = names.get(i);
        }

        AggregateMemberNames amn = AggregateMemberNames.get(memberNames);
        var av = new org.yamcs.parameter.AggregateValue(amn);
        for (int i = 0; i < names.length(); i++) {
            av.setMemberValue(names.get(i), fromProto(values.get(i)));
        }

        return av;
    }

    private static org.yamcs.parameter.ArrayValue fromProto(ArrayValue arrayValue) {
        var vlist = arrayValue.getValue();
        if (vlist.length() == 0) {
            return new org.yamcs.parameter.ArrayValue(new int[] { 0 }, Type.UINT32);// FIXME
        }

        Value v0 = vlist.get(0);
        int n = vlist.length();
        var av = new org.yamcs.parameter.ArrayValue(new int[] { n }, getType(v0));

        for (int i = 0; i < n; i++) {
            var vi = vlist.get(i);
            if (getType(vi) != av.getElementType()) {
                throw new IllegalArgumentException("Array elements have all to be of the same type");
            }
            av.setElementValue(i, fromProto(vi));
        }
        return av;
    }

    private static Type getType(Value qv) {
        if (qv.hasBooleanValue()) {
            return Type.BOOLEAN;
        } else if (qv.hasDoubleValue()) {
            return Type.DOUBLE;
        } else if (qv.hasFloatValue()) {
            return Type.FLOAT;
        } else if (qv.hasSint32Value()) {
            return Type.SINT32;
        } else if (qv.hasSint64Value()) {
            return Type.SINT64;
        } else if (qv.hasStringValue()) {
            return Type.STRING;
        } else if (qv.hasUint32Value()) {
            return Type.UINT32;
        } else if (qv.hasUint64Value()) {
            return Type.UINT64;
        } else if (qv.hasEnumeratedValue()) {
            return Type.ENUMERATED;
        } else if (qv.hasAggregateValue()) {
            return Type.AGGREGATE;
        } else if (qv.hasArrayValue()) {
            return Type.ARRAY;
        } else {
            throw new IllegalStateException("Unknown value type " + qv);
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
                .setMessage(ev.getMessage());

        org.yamcs.protobuf.Event.EventSeverity sev = org.yamcs.protobuf.Event.EventSeverity
                .forNumber(ev.getSeverityValue());
        yevb.setSeverity(sev);

        if (ev.hasSeqNumber()) {
            yevb.setSeqNumber(ev.getSeqNumber());
        }

        if (ev.hasSource()) {
            yevb.setSource(ev.getSource());
        }

        if (ev.hasGenerationTime()) {
            yevb.setGenerationTime(fromProtoMillis(ev.getGenerationTime()));
        } else {
            yevb.setGenerationTime(timeService.getMissionTime());
        }

        if (ev.hasAcquisitionTime()) {
            yevb.setReceptionTime(fromProtoMillis(ev.getAcquisitionTime()));
        } else {
            yevb.setReceptionTime(timeService.getMissionTime());
        }

        if (ev.hasExtra()) {
            for (var evextr : ev.getExtra()) {
                yevb.putExtra(evextr.getKey(), evextr.getValue());
            }
        }

        return yevb.build();
    }

    public static long fromProtoMillis(Timestamp t) {
        return t.getMillis();
    }

    public static Timestamp toProtoTimestamp(long instant) {
        return Timestamp.newInstance().setMillis(instant - 37000);
    }

    public static AckStatus fromProto(org.yamcs.ygw.protobuf.Ygw.CommandAck.AckStatus ackStatus) {
        return switch (ackStatus) {
        case NA -> AckStatus.NA;
        case CANCELLED -> AckStatus.CANCELLED;
        case DISABLED -> AckStatus.DISABLED;
        case NOK -> AckStatus.NOK;
        case OK -> AckStatus.OK;
        case PENDING -> AckStatus.PENDING;
        case SCHEDULED -> AckStatus.SCHEDULED;
        case TIMEOUT -> AckStatus.TIMEOUT;
        default -> throw new IllegalArgumentException("Unexpected value: " + ackStatus);
        };
    }

    static ParameterValue fromProto(Parameter p, org.yamcs.ygw.protobuf.Ygw.ParameterValue qpv, long genTime,
            long acqTime) {
        ParameterValue pv = new ParameterValue(p);
        if (qpv.hasEngValue()) {
            pv.setEngValue(ProtoConverter.fromProto(qpv.getEngValue()));
        }

        if (qpv.hasRawValue()) {
            pv.setRawValue(ProtoConverter.fromProto(qpv.getRawValue()));
        }

        if (qpv.hasGenerationTime()) {
            pv.setGenerationTime(ProtoConverter.fromProtoMillis(qpv.getGenerationTime()));
        } else {
            pv.setGenerationTime(genTime);
        }

        if (qpv.hasAcquisitionTime()) {
            pv.setAcquisitionTime(ProtoConverter.fromProtoMillis(qpv.getAcquisitionTime()));
        } else {
            pv.setAcquisitionTime(acqTime);
        }

        if (qpv.hasExpireMillis()) {
            pv.setExpireMillis(qpv.getExpireMillis());
        }

        return pv;
    }

    static org.yamcs.ygw.protobuf.Ygw.ParameterValue toProto(YgwParameter ygwp, ParameterValue pv) {
        var qpv = org.yamcs.ygw.protobuf.Ygw.ParameterValue.newInstance().setId(ygwp.id);

        if (pv.getGenerationTime() != TimeEncoding.INVALID_INSTANT) {
            qpv.setGenerationTime(ProtoConverter.toProtoTimestamp(pv.getGenerationTime()));
        }

        if (pv.getAcquisitionTime() != TimeEncoding.INVALID_INSTANT) {
            qpv.setGenerationTime(ProtoConverter.toProtoTimestamp(pv.getAcquisitionTime()));
        }

        if (pv.getEngValue() != null) {
            qpv.setEngValue(ProtoConverter.toProto(pv.getEngValue()));
        }

        if (pv.getRawValue() != null) {
            qpv.setRawValue(ProtoConverter.toProto(pv.getRawValue()));
        }
        if (pv.getExpireMillis() > 0) {
            qpv.setExpireMillis(pv.getExpireMillis());
        }

        return qpv;
    }

    public static org.yamcs.CommandOption fromProto(CommandOption opt) {
        CommandOptionType type = switch (opt.getType()) {
        case BOOLEAN -> CommandOptionType.BOOLEAN;
        case NUMBER -> CommandOptionType.NUMBER;
        case STRING -> CommandOptionType.STRING;
        case TIMESTAMP -> CommandOptionType.TIMESTAMP;
        default -> throw new IllegalArgumentException("Unexpected value: " + opt.getType());
        };

        var co = new org.yamcs.CommandOption(opt.getId(), opt.getVerboseName(), type);
        if (opt.hasHelp()) {
            co = co.withHelp(opt.getHelp());
        }
        return co;
    }

}
