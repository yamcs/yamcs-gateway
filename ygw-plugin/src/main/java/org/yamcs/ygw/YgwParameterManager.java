package org.yamcs.ygw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.yamcs.logging.Log;
import org.yamcs.mdb.Mdb;
import org.yamcs.mdb.MdbFactory;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SoftwareParameterManager;
import org.yamcs.parameter.Value;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.xtce.DataSource;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.UnitType;
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
    final DataSource dataSource;
    static final Log log = new Log(YgwParameterManager.class);
    final Mdb mdb;

    public YgwParameterManager(String yamcsInstance, DataSource dataSource) {
        this.dataSource = dataSource;
        this.mdb = MdbFactory.getInstance(yamcsInstance);

    }

    @Override
    public void updateParameters(List<ParameterValue> pvals) {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateParameter(Parameter p, Value v) {
        // TODO Auto-generated method stub

    }

    public void addParameterDefs(String namespace, ParameterDefinitionList pdefs) {
        log.debug("Got parameter definitions: {}", pdefs);
        List<Parameter> plist = new ArrayList<>();
        for (ParameterDefinition pdef : pdefs.getDefinitions()) {
            String fqn = namespace + NameDescription.PATH_SEPARATOR + pdef.getRelativeName();
            if (mdb.getParameter(fqn) != null) {
                log.debug("Parameter {} already exists in the MDB, not adding it", fqn);
                continue;
            }

            var  ptype = getParameterType(pdef);
            if(ptype == null) {
                log.warn("Parameter type {} is not basic and could not be found in the MDB; parameter ignored",
                        pdef.getType());
                continue;
            }
            Parameter p = new Parameter(fqn);
            p.setParameterType(ptype);
            plist.add(p);
        }

        try {
            mdb.addParameters(plist, true, false);
        } catch (IOException e) {
            log.error("Got error adding parameters to the MDB", e);
        } catch (IllegalArgumentException e) {
            log.warn("Error adding parameters to the MDB: " + e);
        }

    }

    private ParameterType getParameterType(ParameterDefinition pdef) {
        var unit = getUnit(pdef);
        Type basicType = getBasicType(pdef.getType());
        if (basicType == null) {
            // not a basic type, it must exist in the MDB
            return mdb.getParameterType(pdef.getType());
        } else {
            return mdb.getOrCreateBasicType(basicType, unit);
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
    private Type getBasicType(String pt) {
        try {
            Type t = Type.valueOf(pt);
            if (t == Type.AGGREGATE || t == Type.ARRAY || t == Type.NONE) {
                return null;
            } else {
                return t;
            }
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
