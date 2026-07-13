package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.CSAskMessage;

import java.util.UUID;

public class CSAck extends CSAskMessage {
    public CSAck(UUID uuid) {
        super(uuid);
    }
}
