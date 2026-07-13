package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

public final class CSAskTimeout implements Serializable {
    public final UUID uuid;
    
    public CSAskTimeout(UUID uuid) {
        this.uuid = uuid;
    }
}
