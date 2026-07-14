package it.unitn.ds.cs.messages.coordinator;

import it.unitn.ds.cs.CSUpdateData;
import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.io.Serializable;
import java.util.UUID;

public class CSUpdate extends CSAskMessage implements Serializable {
    public final CSUpdateKey key;
    public final CSUpdateData data;
    public final UUID writeRequestUUID;
    
    public CSUpdate(CSUpdateKey key, CSUpdateData data, UUID writeRequestUUID, UUID uuid) {
        super(uuid);
        this.key = key;
        this.data = data;
        this.writeRequestUUID = writeRequestUUID;
    }
    
    public CSUpdate(CSUpdateKey key, CSUpdateData data, UUID writeRequestUUID) {
        super();
        this.key = key;
        this.data = data;
        this.writeRequestUUID = writeRequestUUID;
    }
}
