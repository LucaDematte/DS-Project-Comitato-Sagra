package it.unitn.ds.cs.messages.replica;

import it.unitn.ds.cs.messages.client.CSWriteRequest;

import java.util.UUID;

public record CSWriteForward(CSWriteRequest request, UUID uuid) {}
