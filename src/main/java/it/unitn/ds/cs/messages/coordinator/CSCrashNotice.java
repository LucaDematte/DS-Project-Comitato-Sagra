package it.unitn.ds.cs.messages.coordinator;

import java.io.Serializable;

/**
 * Message used by the coordinator to share with other replicas the information that a replica has
 * crashed.
 * More specifically, this message is used to update the {@code next} pointer used by replicas to
 * represent the ring topology used during elections.
 * This way, elections are generally faster because replicas keep a pointer to an active replica and
 * don't loose time waiting for ACKs by crashed replicas.
 */
public class CSCrashNotice implements Serializable {
    public final int next;
    
    public CSCrashNotice(int next) {
        this.next = next;
    }
}
