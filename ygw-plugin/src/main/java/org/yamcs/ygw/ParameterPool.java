package org.yamcs.ygw;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.yamcs.xtce.Parameter;

class ParameterPool {
    final HashMap<Parameter, YgwParameter> byParam = new HashMap<>();
    final HashMap<Key, YgwParameter> byId = new HashMap<>();
    ReadWriteLock rwLock = new ReentrantReadWriteLock();

    void add(YgwLink link, List<YgwParameter> ygwPlist) {
        rwLock.writeLock().lock();
        try {
            ygwPlist.forEach(p -> {
                byParam.put(p.p, p);
                byId.put(key(link, p.nodeId, p.id), p);
            });
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    YgwParameter getByParameter(Parameter p) {
        rwLock.readLock().lock();
        try {
            return byParam.get(p);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    YgwParameter getById(YgwLink link, int nodeId, int pid) {
        rwLock.readLock().lock();
        try {
            return byId.get(key(link, nodeId, pid));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    static Key key(YgwLink link, int nodeId, int pid) {
        return new Key(link, nodeId, pid);
    }

    static class Key {

        final YgwLink link;
        final int nodeId;
        final int pid;

        public Key(YgwLink link, int nodeId, int pid) {
            super();
            this.link = link;
            this.nodeId = nodeId;
            this.pid = pid;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((link == null) ? 0 : link.hashCode());
            result = prime * result + nodeId;
            result = prime * result + pid;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ParameterPool.Key other = (ParameterPool.Key) obj;
            if (link == null) {
                if (other.link != null)
                    return false;
            } else if (!link.equals(other.link))
                return false;
            if (nodeId != other.nodeId)
                return false;
            if (pid != other.pid)
                return false;
            return true;
        }

    }

    static class YgwParameter {
        final Parameter p;
        final YgwLink link;
        final int nodeId;
        final int id;
        final boolean writable;

        public YgwParameter(YgwLink link, int nodeId, Parameter p, int id, boolean writable) {
            super();
            this.p = p;
            this.link = link;
            this.nodeId = nodeId;
            this.id = id;
            this.writable = writable;
        }
    }
}
