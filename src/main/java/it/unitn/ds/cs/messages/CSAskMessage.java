package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * This class serves as a base class for all the messages that are exchanged using the custom ask
 * system ({@link it.unitn.ds.cs.CSAsk}).
 * The only field is a UUID that is used to correlate a response with its request.
 * If the message that is being built is a response, is must be constructed by passing the right
 * UUID, otherwise the receiver won't be able to recognize that it is the response to a pending
 * request.
 */
public abstract class CSAskMessage implements Serializable {
    public final UUID askUUID;
    
    public CSAskMessage() {
        this.askUUID = UUID.randomUUID();
    }
    
    public CSAskMessage(UUID uuid) {
        this.askUUID = uuid;
    }
}
