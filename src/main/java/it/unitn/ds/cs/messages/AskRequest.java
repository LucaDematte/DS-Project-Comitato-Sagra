package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Involucro generico usato per "taggare" un messaggio in uscita con un identificativo
 * di correlazione, in modo che la risposta possa essere abbinata alla callback corretta.
 * <p>
 * Chi riceve una AskRequest deve rispondere con una AskResponse che porta lo stesso
 * correlationId (vedi AskResponseSystem.reply(...)).
 */
public final class AskRequest implements Serializable {
    
    private final UUID correlationId;
    private final Serializable payload;
    
    public AskRequest(UUID correlationId, Serializable payload) {
        this.correlationId = correlationId;
        this.payload = payload;
    }
    
    public UUID getCorrelationId() {
        return correlationId;
    }
    
    public Serializable getPayload() {
        return payload;
    }
    
    @Override
    public String toString() {
        return "AskRequest{correlationId=" + correlationId + ", payload=" + payload + '}';
    }
}
