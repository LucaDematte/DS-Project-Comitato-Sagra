package it.unitn.ds.cs.messages.client;

import java.io.Serializable;

/**
 * Message sent by replicas to themselves every second to trigger the check for the correct
 * HEARTBEAT reception from the coordinator.
 */
public class CSHeartBeatCheck implements Serializable {}
