package it.unitn.ds.cs.messages.coordinator;

import java.io.Serializable;

/**
 * Synchronization message sent by the elected coordinator to end the election process.
 * The message contains the ID of the new coordinator.
 */
public class CSSynchronization implements Serializable {
    private final int newCoordinatorId;
    
    public CSSynchronization(int newCoordinatorId) {
        this.newCoordinatorId = newCoordinatorId;
    }
    
    public int getNewCoordinatorId() {
        return newCoordinatorId;
    }
}
