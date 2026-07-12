package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import it.unitn.ds.cs.ActorCallbacks;
import it.unitn.ds.cs.messages.CSCallbackMessage;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.replica.CSReadResult;
import it.unitn.ds.cs.messages.replica.CSWriteResult;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Client extends AbstractClient {
    private final ActorCallbacks callbacks = new ActorCallbacks(getSelf());
    
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
    
    @Override
    public void sendRead(ActorRef replica, int index) {
        Duration timeout = Duration.ofMillis(getReadTimeoutDelay());
        
        CompletionStage<Object> future = Patterns.ask(replica, new CSReadRequest(index), timeout);
        
        callbacks.attach(future, (res, e) -> {
            if (e == null) {
                CSReadResult readResult = (CSReadResult) res;
                // call when the result is received
                callbackOnReadResult(
                        new ReadResult(readResult.success, readResult.index, readResult.value,
                                readResult.replicaId));
            } else {
                if (e instanceof AskTimeoutException) {
                    // call when the timeout expires
                    callbackOnReadTimeout(new ReadTimeout(getSelf(), replica, index));
                } else {
                    log("Unknown exception of type " + e.getClass()
                                                        .getName() + " on sendRead future: " + e.getMessage());
                }
            }
        });
    }
    
    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("Sending write request to " + replica.path()
                                                 .name() + ": set P[" + index + "] to " + value);
        
        Duration timeout = Duration.ofMillis(getWriteTimeoutDelay());
        CompletionStage<Object> future = Patterns.ask(replica, new CSWriteRequest(index, value),
                timeout);
        
        callbacks.attach(future, (res, e) -> {
            if (e == null) {
                CSWriteResult writeResult = (CSWriteResult) res;
                // call when the result is received
                log("Received write result: P[" + writeResult.index + "] = " + writeResult.value + " (success = " + writeResult.success + ")");
                callbackOnWriteResult(
                        new WriteResult(writeResult.success, writeResult.index, writeResult.value,
                                writeResult.replicaId));
            } else {
                if (e instanceof AskTimeoutException) {
                    // call when the timeout expires
                    log("Write timeout expired");
                    callbackOnWriteTimeout(new WriteTimeout(getSelf(), replica, index, value));
                } else {
                    log("Unknown exception of type " + e.getClass()
                                                        .getName() + " on sendWrite future: " + e.getMessage());
                }
            }
        });
    }
    
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // handler for callbacks sent to itself
                .match(CSCallbackMessage.class, callbacks::handle).build();
    }
    
}
