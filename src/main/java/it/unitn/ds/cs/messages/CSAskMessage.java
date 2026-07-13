package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

public abstract class CSAskMessage implements Serializable {
    public final UUID askUUID;
    
    public CSAskMessage() {
        this.askUUID = UUID.randomUUID();
    }
    
    public CSAskMessage(UUID uuid) {
        this.askUUID = uuid;
    }
}
