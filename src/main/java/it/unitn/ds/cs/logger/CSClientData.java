package it.unitn.ds.cs.logger;

import akka.actor.ActorRef;

import java.util.UUID;

public class CSClientData {
    public final ActorRef actor;
    public final int contactedReplicaId;
    public final UUID askUUID;
    
    public CSClientData(ActorRef actor, int contactedReplicaId, UUID askUUID) {
        this.actor = actor;
        this.contactedReplicaId = contactedReplicaId;
        this.askUUID = askUUID;
    }
}
