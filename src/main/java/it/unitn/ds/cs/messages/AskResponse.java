package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Involucro generico per la risposta a una AskRequest: porta lo stesso correlationId
 * della richiesta, così AskResponseSystem può abbinarla alla callback corretta.
 * <p>
 * Si costruisce tipicamente con AskResponseSystem.reply(request, payload).
 */
public final class AskResponse implements Serializable {
    
    private final UUID correlationId;
    private final Serializable payload;
    
    public AskResponse(UUID correlationId, Serializable payload) {
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
        return "AskResponse{correlationId=" + correlationId + ", payload=" + payload + '}';
    }
}
