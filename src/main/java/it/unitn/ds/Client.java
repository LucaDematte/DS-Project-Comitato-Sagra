package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.cs.AskResponseSystem;
import it.unitn.ds.cs.messages.AskResponse;
import it.unitn.ds.cs.messages.AskTimeout;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.replica.CSReadResult;
import it.unitn.ds.cs.messages.replica.CSWriteResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public class Client extends AbstractClient {
    private final AskResponseSystem askSupport = new AskResponseSystem(getContext(), this::tell);
    
    Client(
            long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
            Optional<ActorRef> listener
    ) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }
    
    public static Props props(
            long readTimeoutDelay, long writeTimeoutDelay,
            Optional<ActorRef> defaultTargetReplica
    ) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
                        Optional.empty()));
    }
    
    // Props method for automated tests
    public static Props propsWithListener(
            long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
            ActorRef listener
    ) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
                        Optional.ofNullable(listener)));
    }
    
    /**
     * Wrapper for tell with the same signature from AbstractReplica.tell
     * Needed to enable the client to use the custom ask system.
     */
    void tell(Serializable m, ActorRef dst) {
        dst.tell(m, getSelf());
    }
    
    @Override
    public void sendRead(ActorRef replica, int index) {
        Duration timeout = Duration.ofMillis(getReadTimeoutDelay());
        
        askSupport.<CSReadResult>ask(new CSReadRequest(index), replica, timeout,
                (res, timedOut) -> {
                    if (!timedOut) {
                        // call when the result is received
                        callbackOnReadResult(
                                new ReadResult(res.success, res.index, res.value, res.replicaId));
                    } else {
                        // call when the timeout expires
                        callbackOnReadTimeout(new ReadTimeout(getSelf(), replica, index));
                    }
                });
    }
    
    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("Sending write request to " + replica.path()
                                                 .name() + ": set P[" + index + "] to " + value);
        
        Duration timeout = Duration.ofMillis(getWriteTimeoutDelay());
        
        askSupport.<CSWriteResult>ask(new CSWriteRequest(index, value), replica, timeout,
                (res, timedOut) -> {
                    if (!timedOut) {
                        // call when the result is received
                        log("Received write result: P[" + res.index + "] = " + res.value + " (success = " + res.success + ")");
                        callbackOnWriteResult(
                                new WriteResult(res.success, res.index, res.value, res.replicaId));
                    } else {
                        // call when the timeout expires
                        log("Write timeout expired");
                        callbackOnWriteTimeout(new WriteTimeout(getSelf(), replica, index, value));
                    }
                });
    }
    
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // handlers for messages in the ask-response system
                .match(AskResponse.class, askSupport::handleResponse)
                .match(AskTimeout.class, askSupport::handleTimeout)
                .build();
    }
    
}
