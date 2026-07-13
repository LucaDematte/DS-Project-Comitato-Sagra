package it.unitn.ds.cs;

import akka.actor.ActorRef;

public class UpdateData {
    public final int index;
    public final int value;
    public UpdateStatus status;
    public final boolean localRequest;      // int localReplicaId
    public final ActorRef client;
    
    public UpdateData(
            int index, int value, UpdateStatus status, boolean localRequest, ActorRef client) {
        this.index = index;
        this.value = value;
        this.localRequest = localRequest;
        this.client = client;
        this.status = status;
    }
}
