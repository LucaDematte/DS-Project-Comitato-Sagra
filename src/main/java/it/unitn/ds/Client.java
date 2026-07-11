package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import it.unitn.ds.cs.messages.CSReadRequest;
import it.unitn.ds.cs.messages.CSReadResult;
import it.unitn.ds.cs.messages.CSWriteRequest;
import it.unitn.ds.cs.messages.CSWriteResult;
//import scala.concurrent.duration.Duration;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Client extends AbstractClient {

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        Duration timeout = Duration.ofMillis(getReadTimeoutDelay());

        CompletionStage<Object> future = Patterns.ask(replica, new CSReadRequest(index), timeout);

        future.thenAccept(result -> {
            CSReadResult readResult = (CSReadResult) result;
            // call when the result is received
            callbackOnReadResult(new ReadResult(readResult.success, readResult.index, readResult.value, readResult.replicaId));
        }).exceptionally(ex -> {
            if (ex instanceof AskTimeoutException) {
                // call when the timeout expires
                callbackOnReadTimeout(new ReadTimeout(getSelf(), replica, index));
            }

            return null;
        });
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        Duration timeout = Duration.ofMillis(getWriteTimeoutDelay());
        CompletionStage<Object> future = Patterns.ask(replica, new CSWriteRequest(index, value), timeout);

        log("Sending write request to " + replica.path().name() + ": set P[" + index + "] to " + value);

        future.thenAccept(result -> {
            CSWriteResult writeResult = (CSWriteResult) result;
            // call when the result is received
            log("Received write result: P[" + writeResult.index + "] = " + writeResult.value + " (success = " + writeResult.success + ")");
            callbackOnWriteResult(new WriteResult(writeResult.success, writeResult.index, writeResult.value, writeResult.replicaId));
        }).exceptionally(ex -> {
            if (ex.getCause() instanceof AskTimeoutException) {
                // call when the timeout expires
                log("Write timeout expired");
                callbackOnWriteTimeout(new WriteTimeout(getSelf(), replica, index, value));
            }else{
                log("Unknown exception on future: " + ex.getMessage());
            }

            return null;
        });
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .build();
    }

}
