package org.yamcs.ygw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.yamcs.Processor;
import org.yamcs.YamcsServer;
import org.yamcs.logging.Log;
import org.yamcs.mdb.Mdb;
import org.yamcs.mdb.MdbFactory;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SoftwareParameterManager;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.time.TimeService;
import org.yamcs.xtce.DataSource;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.UnitType;
import org.yamcs.ygw.ParameterPool.YgwParameter;
import org.yamcs.ygw.protobuf.Ygw.MessageType;
import org.yamcs.ygw.protobuf.Ygw.ParameterData;
import org.yamcs.ygw.protobuf.Ygw.ParameterUpdates;
import org.yamcs.ygw.protobuf.Ygw.ParameterDefinition;
import org.yamcs.ygw.protobuf.Ygw.ParameterDefinitionList;

/**
 * Handles parameter updates from the clients.
 * <p>
 * When a YgwLink is configured, one instance of this is created and registered to the realtime processor of the Yamcs
 * instance where the link belongs.
 * <p>
 * Objects of this class are linked to the processor so they may be used by multiple gateways. Each gateway should
 * configure their own namespace to avoid clashing of parameters.
 */
public class YgwParameterManager implements SoftwareParameterManager {
    static final Log log = new Log(YgwParameterManager.class);

    final ParameterPool pool = new ParameterPool();

    final DataSource dataSource;
    final Mdb mdb;
    final TimeService timeService;
    final Processor processor;

    public YgwParameterManager(Processor processor, String yamcsInstance, DataSource dataSource) {
        this.dataSource = dataSource;
        this.mdb = MdbFactory.getInstance(yamcsInstance);
        this.processor = processor;
        this.timeService = YamcsServer.getTimeService(yamcsInstance);
    }

    /**
     * called from Yamcs API -> send all parameter values to YGW
     */
    @Override
    public void updateParameters(List<ParameterValue> pvals) {

        // first sanity check and value transformation to the target type
        List<ParameterValue> pvals1 = new ArrayList<>();
        var lvc = processor.getLastValueCache();

        for (var pv : pvals) {
            var ygwp = pool.getByParameter(pv.getParameter());
            if (ygwp == null) {
                throw new IllegalArgumentException(
                        "The parameter " + pv.getParameterQualifiedName()
                                + " is not currently register with any gateway");
            }
            if (!ygwp.writable) {
                throw new IllegalArgumentException(
                        "Parameter " + pv.getParameterQualifiedName() + " is not writable");
            }
            pvals1.add(SoftwareParameterManager.transformValue(lvc, pv));
        }

        // now set the parameters grouping per linke and node
        List<ParameterValue> remaining = null;
        YgwLink link = null;
        int nodeId = -1;

        while (pvals1 != null) {
            ParameterUpdates pdata = ParameterUpdates.newInstance();
            for (var pv : pvals1) {
                var ygwp = pool.getByParameter(pv.getParameter());

                if (link == null) {
                    link = ygwp.link;
                    nodeId = ygwp.nodeId;
                    pdata.addParameters(ProtoConverter.toProto(ygwp, pv));
                } else if (link == ygwp.link && nodeId == ygwp.nodeId) {
                    pdata.addParameters(ProtoConverter.toProto(ygwp, pv));
                } else {
                    if (remaining == null) {
                        remaining = new ArrayList<>();
                        remaining.add(pv);
                    }
                }
            }
            link.sendMessage((byte) MessageType.PARAMETER_UPDATES_VALUE, nodeId, 0, pdata.toByteArray())
                    .whenComplete((c, t) -> {
                        if (t != null) {
                            log.warn("Error sending parameters ", t);
                        }
                    });
            pvals1 = remaining;
        }

    }

    /**
     * called when parameters are coming from the gateway
     * 
     */
    public List<ParameterValue> processParameters(YgwLink ygwLink, int nodeId,
            ParameterData pdata) {
        long now = timeService.getMissionTime();
        long genTime = now;
        long acqTime = now;
        if (pdata.hasGenerationTime()) {
            genTime = ProtoConverter.fromProtoMillis(pdata.getGenerationTime());
        }
        if (pdata.hasAcquisitionTime()) {
            acqTime = ProtoConverter.fromProtoMillis(pdata.getAcquisitionTime());
        }
        List<ParameterValue> plist = new ArrayList<>(pdata.getParameters().length());
        for (var qpv : pdata.getParameters()) {
            YgwParameter ygwp = pool.getById(ygwLink, nodeId, qpv.getId());
            if (ygwp == null) {
                log.warn("No parameter found for link: {}, node: {}, pid: {}; ignoring", ygwLink.getName(), nodeId,
                        qpv.getId());
                continue;
            }
            plist.add(ProtoConverter.fromProto(ygwp.p, qpv, genTime, acqTime));

        }
        return plist;

    }

    /**
     * Called when receiving a list of parameter definitions from YGW.
     * <p>
     * Adds the definitions to the MDB after filtering out the invalid ones.
     */
    public void addParameterDefs(YgwLink link, int nodeId, String namespace, ParameterDefinitionList pdefs) {
        List<Parameter> plist = new ArrayList<>();
        List<YgwParameter> ygwPlist = new ArrayList<>();

        for (ParameterDefinition pdef : pdefs.getDefinitions()) {
            if (pdef.getRelativeName().contains("..")) {
                log.warn("Invalid name {} for parameter, ignored", pdef.getRelativeName());
                continue;
            }
            String fqn = namespace + NameDescription.PATH_SEPARATOR + pdef.getRelativeName();
            Parameter mdbParam = mdb.getParameter(fqn);
            if (mdbParam == null) {
                ParameterType ptype = null;
                try {
                    ptype = getParameterType(namespace, pdef);
                } catch (IOException e) {
                    log.error("Error adding parameter type to the MDB", e);
                    continue;
                }

                if (ptype == null) {
                    log.warn("Parameter type {} is not basic and could not be found in the MDB; parameter ignored",
                            pdef.getPtype());
                    continue;
                }
                String name = NameDescription.getName(fqn);
                mdbParam = new Parameter(name);
                if (pdef.hasDescription()) {
                    mdbParam.setShortDescription(pdef.getDescription());
                }
                mdbParam.setQualifiedName(fqn);
                mdbParam.setDataSource(dataSource);

                mdbParam.setParameterType(ptype);
                plist.add(mdbParam);
            } else {
                log.debug("Parameter {} already exists in the MDB, not adding it", fqn);
                // TODO check type compatibility
            }
            ygwPlist.add(new YgwParameter(link, nodeId, mdbParam, pdef.getId(), !pdef.hasWritable() || pdef.getWritable()));
        }

        try {
            mdb.addParameters(plist, true, false);
        } catch (IOException | IllegalArgumentException e) {
            log.error("Error adding parameters to the MDB", e);
            return;
        }
        pool.add(link, ygwPlist);
    }

    private ParameterType getParameterType(String namespace, ParameterDefinition pdef) throws IOException {
        var unit = getUnit(pdef);
        Type basicType = getBasicType(pdef.getPtype());
        if (basicType == null) {
            // not a basic type, it must exist in the MDB
            return mdb.getParameterType(pdef.getPtype());
        } else {
            return mdb.getOrCreateBasicParameterType(namespace, basicType, unit);
        }
    }

    private UnitType getUnit(ParameterDefinition pdef) {
        if (pdef.hasUnit()) {
            return new UnitType(pdef.getUnit());
        } else {
            return null;
        }
    }

    // get the basic type (sint32, string, etc) corresponding to the string
    static Type getBasicType(String pt) {
        try {
            Type t = Type.valueOf(pt.toUpperCase());
            if (t == Type.AGGREGATE || t == Type.ARRAY || t == Type.NONE) {
                return null;
            } else {
                return t;
            }
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public YgwParameter getYgwParameter(YgwLink ygwLink, int nodeId, int pid) {
        return pool.getById(ygwLink, nodeId, pid);
    }
}
