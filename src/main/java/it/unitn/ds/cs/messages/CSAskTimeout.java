package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * This message is used by the custom ask system ({@link it.unitn.ds.cs.CSAsk}) to schedule the
 * timeout for a response.
 * Through the ask system, replicas will send this message to themselves every time they send a
 * request (scheduled with a given delay).
 * This way, if the response message doesn't arrive before this message, the callback registered for
 * the request is fired for the "timeout" case.
 */
public final class CSAskTimeout implements Serializable {
    public final UUID uuid;
    
    public CSAskTimeout(UUID uuid) {
        this.uuid = uuid;
    }
}
