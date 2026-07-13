package it.unitn.ds.cs;

import java.io.Serializable;
import java.util.UUID;

public abstract class CSAsk implements Serializable {
    public final UUID uuid;
    
    public CSAsk() {
        this.uuid = UUID.randomUUID();
    }
    
    public CSAsk(UUID uuid) {
        this.uuid = uuid;
    }
}
