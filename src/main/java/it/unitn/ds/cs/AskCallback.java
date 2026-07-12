package it.unitn.ds.cs;

import java.io.Serializable;

/**
 * Callback invocata al termine di una "ask" simulata: sia in caso di risposta
 * ricevuta, sia in caso di scadenza del timeout.
 * <p>
 * Viene sempre eseguita nel thread di elaborazione dei messaggi dell'attore che
 * ha effettuato la ask (perché invocata da handleResponse/handleTimeout, agganciati
 * al receiveBuilder), quindi è sicuro leggere e modificare lo stato dell'attore
 * al suo interno.
 *
 * @param <T> tipo del payload di risposta atteso
 */
@FunctionalInterface
public interface AskCallback<T extends Serializable> {
    
    /**
     * @param response il payload della risposta ricevuta, oppure {@code null} se è scaduto il
     *                 timeout
     * @param timedOut {@code true} se la callback è scattata per timeout, {@code false} se è
     *                 arrivata una risposta
     */
    void onComplete(T response, boolean timedOut);
}
