package it.unitn.ds.cs.messages;

import java.io.Serializable;
import java.util.UUID;

public record CSCallbackMessage(UUID id, Object response,
                                Throwable failure) implements Serializable {}
