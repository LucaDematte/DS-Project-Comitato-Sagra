package it.unitn.ds.cs;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.UUID;

/**
 * This class contains information about the client that sent a write request to a replica.
 * This information is used when it's time to send a response back to the client.
 */
public class CSClientData implements Serializable {
    /** The actor reference of the client. */
    private final ActorRef actor;
    /**
     * The ID of the replica that the client interacted with (and also the one that will send the
     * response).
     */
    private final int contactedReplicaId;
    /**
     * The UUID used by the ask system for the write request.
     * The response that is sent to the client must have this UUID so that the client considers the
     * message as the response it is waiting for.
     */
    private final UUID askUUID;
    
    public CSClientData(ActorRef actor, int contactedReplicaId, UUID askUUID) {
        this.actor = actor;
        this.contactedReplicaId = contactedReplicaId;
        this.askUUID = askUUID;
    }
    
    public CSClientData(CSClientData data) {
        this(data.actor, data.contactedReplicaId, data.askUUID);
    }
    
    public ActorRef getActor() {
        return actor;
    }
    
    public int getContactedReplicaId() {
        return contactedReplicaId;
    }
    
    public UUID getAskUUID() {
        return askUUID;
    }
}
