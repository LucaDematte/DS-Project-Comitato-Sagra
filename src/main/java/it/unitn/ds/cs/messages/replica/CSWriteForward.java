package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.CSClientData;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.client.CSWriteRequest;

import java.util.UUID;

/**
 * Message used by replicas to forward a write request to the coordinator.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSWriteForward extends CSAskMessage {
    /** The message containing the write request received by the replica. */
    public final CSWriteRequest request;
    /**
     * An ID that the replica assigns to this update so that, when the UPDATE message with the
     * update key is received, this replica can bind the update key to the right update data in its
     * log.
     */
    public final UUID writeRequestUUID;
    /** Information about the client that sent the request (used to send back the response later). */
    public final CSClientData clientData;
    
    public CSWriteForward(CSWriteRequest request, UUID writeRequestUUID, CSClientData clientData) {
        super();
        this.writeRequestUUID = writeRequestUUID;
        this.request = request;
        this.clientData = clientData;
    }
}
