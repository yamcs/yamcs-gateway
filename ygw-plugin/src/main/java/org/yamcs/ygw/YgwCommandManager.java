package org.yamcs.ygw;

import static org.yamcs.cmdhistory.CommandHistoryPublisher.AcknowledgeSent_KEY;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.yamcs.ConfigurationException;
import org.yamcs.Processor;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.StreamTcCommandReleaser;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.cmdhistory.StreamCommandHistoryPublisher;
import org.yamcs.cmdhistory.CommandHistoryPublisher.AckStatus;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.logging.Log;
import org.yamcs.mdb.Mdb;
import org.yamcs.mdb.MdbFactory;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.utils.parser.ParseException;
import org.yamcs.xtce.Argument;
import org.yamcs.xtce.ArgumentType;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.UnitType;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.streamsql.StreamSqlException;
import org.yamcs.ygw.protobuf.Ygw.CommandArgument;
import org.yamcs.ygw.protobuf.Ygw.CommandDefinition;
import org.yamcs.ygw.protobuf.Ygw.CommandDefinitionList;

/**
 * Handles direct commands registered by YGW nodes by sending {@link CommandDefinitionList}
 * <p>
 * Creates a stream and registers it in the processor using
 * {@link StreamTcCommandReleaser#registerOutStream(int, Stream, org.yamcs.StreamTcCommandReleaser.CommandMatcher)}
 * <p>
 * The stream is given top priority and it verifies the command against all command registered from the YGW nodes and
 * recorded in {@link #commands}
 * 
 */
public class YgwCommandManager implements StreamSubscriber {
    static final Log log = new Log(YgwCommandManager.class);
    final Processor processor;
    final Mdb mdb;
    // maps command FQN to the node/link that has registered the command
    final Map<String, LinkCommandId> commands = new ConcurrentHashMap<>();
    final YgwLink parentLink;
    final CommandHistoryPublisher cmdHistPublisher;

    public YgwCommandManager(YgwLink parentLink, Processor processor, String yamcsInstance) {
        this.mdb = MdbFactory.getInstance(yamcsInstance);
        this.processor = processor;
        this.parentLink = parentLink;
        String linkName = parentLink.getName();
        this.cmdHistPublisher = new StreamCommandHistoryPublisher(yamcsInstance);

        var cmdr = processor.getCommandReleaser();
        if (cmdr instanceof StreamTcCommandReleaser) {
            var cmdReleaser = (StreamTcCommandReleaser) cmdr;
            YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);
            String streamName = linkName + "_tc";
            try {
                ydb.execute("create stream " + streamName + StandardTupleDefinitions.TC.getStringDefinition());
            } catch (StreamSqlException | ParseException e) {
                throw new ConfigurationException("Cannto create stream: " + e);
            }
            var stream = ydb.getStream(streamName);
            stream.addSubscriber(this);
            cmdReleaser.registerOutStream(0, stream, pc -> commands.containsKey(pc.getCommandName()));

        } else {
            log.warn("Unexpected command releaser {}, no Gateway registered commanding possible", cmdr.getClass());
        }
    }

    public void addCommandDefs(String namespace, CommandDefinitionList cdefs, YgwNodeLink nodeLink) {
        for (var cdef : cdefs.getDefinitions()) {
            addCommand(namespace, cdef, nodeLink);
        }
    }

    void addCommand(String namespace, CommandDefinition cdef, YgwNodeLink nodeLink) {
        if (cdef.getRelativeName().contains("..")) {
            log.warn("Invalid name {} for command, ignored", cdef.getRelativeName());
            return;
        }
        String fqn = namespace + NameDescription.PATH_SEPARATOR + cdef.getRelativeName();
        if (mdb.getMetaCommand(fqn) != null) {
            log.debug("Command {} already exists in the MDB, not adding it", fqn);
            return;
        }

        String name = NameDescription.getName(fqn);
        var mc = new MetaCommand(name);
        mc.setQualifiedName(fqn);
        if (cdef.hasDescription()) {
            mc.setShortDescription(cdef.getDescription());
        }
        for (var argdef : cdef.getArguments()) {
            argdef.getArgtype();
            Argument arg = new Argument(argdef.getName());

            ArgumentType atype = null;
            var unit = getUnit(argdef);
            Type basicType = YgwParameterManager.getBasicType(argdef.getArgtype());
            if (basicType == null) {
                // not a basic type, it must exist in the MDB
                atype = mdb.getArgumentType(argdef.getArgtype());
                if (atype == null) {
                    log.warn("Argument type {} is not basic and could not be found in the MDB; command {} ignored",
                            argdef.getArgtype(), cdef.getRelativeName());
                    return;
                }
            } else {
                try {
                    atype = mdb.getOrCreateBasicArgumentType(namespace, basicType, unit);
                } catch (IOException e) {
                    log.error("Error adding argument type to the MDB, command {} ignored", cdef.getRelativeName(), e);
                    return;
                }
            }

            arg.setArgumentType(atype);
            mc.addArgument(arg);

        }
        commands.put(fqn, new LinkCommandId(cdef.getYgwCmdId(), nodeLink));

        mdb.addMetaCommand(mc);
    }

    private UnitType getUnit(CommandArgument cdef) {
        if (cdef.hasUnit()) {
            return new UnitType(cdef.getUnit());
        } else {
            return null;
        }
    }

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        PreparedCommand pc = PreparedCommand.fromTuple(tuple, mdb);

        // if we reached this method it is because the StreamTcCommandReleaser.CommandMatcher created in the constructor
        // cmdReleaser.registerOutStream(...) verified that the command is in the command map, so we are sure nodeLink
        // is not null
        var lcmdid = commands.get(pc.getCommandName());
        System.out.println("aici lcmid: " + lcmdid);

        if (!lcmdid.nodeLink.sendCommand(pc, lcmdid.ygwCmdId)) {
            CommandId commandId = pc.getCommandId();
            String reason = "YGW link not connected";
            log.info("Failing command cmdId: {}, reason: {}", pc.getCommandId(), reason);
            long currentTime = processor.getCurrentTime();
            cmdHistPublisher.publishAck(commandId, AcknowledgeSent_KEY,
                    currentTime, AckStatus.NOK, reason);
            cmdHistPublisher.commandFailed(commandId, currentTime, reason);
        }
    }

    static record LinkCommandId(int ygwCmdId, YgwNodeLink nodeLink) {
    };
}
