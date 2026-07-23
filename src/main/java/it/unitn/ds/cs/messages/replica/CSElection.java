package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Election message that is passed to each replica during the election process.
 * This method extends {@link CSAskMessage} so that it can be used with the custom ask system (more
 * info at {@link it.unitn.ds.cs.CSAsk}).
 */
public class CSElection extends CSAskMessage {
    /** A map that stores the key of the most recent update known by each replica. */
    public final Map<Integer, CSUpdateKey> lastUpdates;
    /** A map that stores the key of the most recent update applied by each replica. */
    public final Map<Integer, CSUpdateKey> lastCompleteUpdates;
    /** The ID of the replica that started the election process related to this message. */
    public final int initiatorId;
    /** The ID of the coordinator that, by crashing, caused this election process. */
    public final int crashedCoordinatorId;
    
    public CSElection(
            Map<Integer, CSUpdateKey> lastUpdates, Map<Integer, CSUpdateKey> lastCompleteUpdates,
            int initiatorId, int crashedCoordinatorId
    ) {
        super();
        this.lastUpdates = Collections.unmodifiableMap(new HashMap<>(lastUpdates));
        this.lastCompleteUpdates = Collections.unmodifiableMap(new HashMap<>(lastCompleteUpdates));
        this.initiatorId = initiatorId;
        this.crashedCoordinatorId = crashedCoordinatorId;
    }
    
    public CSElection(CSElection msg) {
        super();
        this.lastUpdates = Collections.unmodifiableMap(new HashMap<>(msg.lastUpdates));
        this.lastCompleteUpdates = Collections.unmodifiableMap(new HashMap<>(msg.lastCompleteUpdates));
        this.initiatorId = msg.initiatorId;
        this.crashedCoordinatorId = msg.crashedCoordinatorId;
    }
    
    @Override
    public String toString() {
        StringBuilder rtn = new StringBuilder();
        for (var lastUpdate : lastUpdates.entrySet())
            rtn.append(" ")
               .append(lastUpdate.getKey())
               .append(": ")
               .append(lastUpdate.getValue().toString());
        
        return rtn.toString();
    }
}
