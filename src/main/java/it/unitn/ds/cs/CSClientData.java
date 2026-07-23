package it.unitn.ds.cs;

import akka.actor.ActorRef;

import java.util.UUID;

/**
 * This class contains information about the client that sent a write request to a replica.
 * This information is used when it's time to send a response back to the client.
 */
public class CSClientData {
    /** The actor reference of the client. */
    public final ActorRef actor;
    /**
     * The ID of the replica that the client interacted with (and also the one that will send the
     * response).
     */
    public final int contactedReplicaId;
    /**
     * The UUID used by the ask system for the write request.
     * The response that is sent to the client must have this UUID so that the client considers the
     * message as the response it is waiting for.
     */
    public final UUID askUUID;
    
    public CSClientData(ActorRef actor, int contactedReplicaId, UUID askUUID) {
        this.actor = actor;
        this.contactedReplicaId = contactedReplicaId;
        this.askUUID = askUUID;
    }
}
