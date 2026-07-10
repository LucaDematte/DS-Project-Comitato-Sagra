package it.unitn.ds.cs.messages;

import java.io.Serializable;

public class CSCrashNotice implements Serializable {
    public final int crashed_id;
    public final int previous;
    public final int next;

    public CSCrashNotice(int crashed_id, int previous, int next) {
        this.crashed_id = crashed_id;
        this.previous = previous;
        this.next = next;
    }
}
