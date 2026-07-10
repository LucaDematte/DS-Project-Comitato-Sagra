package it.unitn.ds.cs.messages;

import it.unitn.ds.cs.CSUpdateKey;
import it.unitn.ds.cs.CSUpdateValue;

import java.io.Serializable;
import java.util.UUID;

public record CSUpdate(CSUpdateKey key, CSUpdateValue update, UUID uuid) implements Serializable {
}
