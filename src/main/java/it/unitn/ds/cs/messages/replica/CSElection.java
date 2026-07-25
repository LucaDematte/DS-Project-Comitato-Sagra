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
    private final Map<Integer, CSUpdateKey> lastUpdates;
    /** A map that stores the key of the most recent update applied by each replica. */
    private final Map<Integer, CSUpdateKey> lastCompleteUpdates;
    /** The ID of the replica that started the election process related to this message. */
    private final int initiatorId;
    /**
     * The number of times the initiator has tried to start an election. Normally has value 1, but
     * increases when the election gets stuck.
     */
    private final int electionAttempt;
    /** The ID of the coordinator that, by crashing, caused this election process. */
    private final int crashedCoordinatorId;
    
    public CSElection(
            Map<Integer, CSUpdateKey> lastUpdates, Map<Integer, CSUpdateKey> lastCompleteUpdates,
            int initiatorId, int electionAttempt, int crashedCoordinatorId
    ) {
        super();
        this.lastUpdates = Collections.unmodifiableMap(new HashMap<>(lastUpdates));
        this.lastCompleteUpdates = Collections.unmodifiableMap(new HashMap<>(lastCompleteUpdates));
        this.initiatorId = initiatorId;
        this.electionAttempt = electionAttempt;
        this.crashedCoordinatorId = crashedCoordinatorId;
    }
    
    public CSElection(CSElection msg) {
        this(Collections.unmodifiableMap(new HashMap<>(msg.lastUpdates)),
             Collections.unmodifiableMap(new HashMap<>(msg.lastCompleteUpdates)),
             msg.initiatorId,
             msg.electionAttempt,
             msg.crashedCoordinatorId
        );
    }
    
    public Map<Integer, CSUpdateKey> getLastUpdates() {
        return lastUpdates;
    }
    
    public Map<Integer, CSUpdateKey> getLastCompleteUpdates() {
        return lastCompleteUpdates;
    }
    
    public int getInitiatorId() {
        return initiatorId;
    }
    
    public int getElectionAttempt() {
        return electionAttempt;
    }
    
    public int getCrashedCoordinatorId() {
        return crashedCoordinatorId;
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
