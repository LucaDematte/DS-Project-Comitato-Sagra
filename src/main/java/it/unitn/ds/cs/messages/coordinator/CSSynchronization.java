package it.unitn.ds.cs.messages.coordinator;

import java.io.Serializable;

public class CSSynchronization implements Serializable {
    public final int newCoordinatorId;
    
    public CSSynchronization(int newCoordinatorId) {
        this.newCoordinatorId = newCoordinatorId;
    }
}
