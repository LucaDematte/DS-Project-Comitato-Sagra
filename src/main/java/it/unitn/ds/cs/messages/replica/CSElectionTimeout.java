package it.unitn.ds.cs.messages.replica;

import java.io.Serializable;

/**
 * Message sent by replicas to themselves when they enter the election state.
 * This message is scheduled well into the future, way after the election should be complete.
 * Receiving this message while still in election state means that the election process must have
 * halted (this can happen if the replica holding an election message crashes after ACKing the
 * previous one but before sending the message to the next).
 */
public class CSElectionTimeout implements Serializable {}
