package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CSElection extends CSAskMessage {
    public final Map<Integer, CSUpdateKey> lastUpdates;
    public final Map<Integer, CSUpdateKey> lastCompleteUpdates;
    public final int initiatorId;
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
