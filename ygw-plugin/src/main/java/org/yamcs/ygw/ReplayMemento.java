package org.yamcs.ygw;

import java.util.ArrayDeque;
import java.util.Deque;

import com.google.gson.annotations.SerializedName;

public class ReplayMemento {
    @SerializedName("enabled")
    private Deque<Interval> intervals = new ArrayDeque<>();

    public void addFirst(Interval interval) {
        intervals.addFirst(interval);
    }

    public void addLast(Interval interval) {
        intervals.addLast(interval);
    }

    public Interval poll() {
        return intervals.poll();
    }

    public void clear() {
        intervals.clear();
    }

    static record Interval(long startRn, long stopRn, int numFailures) {
    }

    @Override
    public String toString() {
        return "ReplayMemento [intervals=" + intervals + "]";
    }

}

