package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Messaggio che l'attore invia a sé stesso, schedulato al momento della ask,
 * per garantire l'esecuzione della callback anche in assenza di risposta entro il timeout.
 */
public final class AskTimeout implements Serializable {
    
    private final UUID correlationId;
    
    public AskTimeout(UUID correlationId) {
        this.correlationId = correlationId;
    }
    
    public UUID getCorrelationId() {
        return correlationId;
    }
    
    @Override
    public String toString() {
        return "AskTimeout{correlationId=" + correlationId + '}';
    }
}
