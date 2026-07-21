package it.unitn.ds.cs.messages.coordinator;

import java.io.Serializable;

public class CSCrashNotice implements Serializable {
    public final int next;
    
    public CSCrashNotice(int next) {
        this.next = next;
    }
}
