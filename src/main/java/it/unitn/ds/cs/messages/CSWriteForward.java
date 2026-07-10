package it.unitn.ds.cs.messages;

import java.util.UUID;

public record CSWriteForward(CSWriteRequest request, UUID uuid) {
}
