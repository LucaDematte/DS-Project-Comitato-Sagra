package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

/**
 * Message used as ACK in various instances of the project.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSAck extends CSAskMessage {
    public CSAck(UUID uuid) {
        super(uuid);
    }
}
