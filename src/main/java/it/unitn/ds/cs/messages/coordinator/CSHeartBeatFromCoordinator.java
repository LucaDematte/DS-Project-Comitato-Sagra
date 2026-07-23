package it.unitn.ds.cs.messages.coordinator;

import java.io.Serializable;

/**
 * Message sent by the coordinator to replicas every second to let them know that it's not crashed.
 * Replicas check if they receive this message regularly, and if the message doesn't arrive, a new
 * election is started.
 */
public class CSHeartBeatFromCoordinator implements Serializable {}
