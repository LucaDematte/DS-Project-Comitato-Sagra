package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import it.unitn.ds.cs.messages.CSRead;
import it.unitn.ds.cs.messages.CSReadResult;
import it.unitn.ds.cs.messages.CSWrite;
import it.unitn.ds.cs.messages.CSWriteResult;
//import scala.concurrent.duration.Duration;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

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
        // TODO: implement
        // - Send message to replica with request details
        // - Set up timeout mechanism
        Duration timeout = Duration.ofMillis(getReadTimeoutDelay());

        CompletionStage<Object> future = Patterns.ask(replica, new CSRead(index), timeout);

        future.thenAccept(result -> {
            CSReadResult readResult = (CSReadResult) result;
            // call when the result is received
            callbackOnReadResult(new ReadResult(readResult.success, readResult.index, readResult.value, readResult.replicaId));
        }).exceptionally(ex -> {
            if(ex instanceof AskTimeoutException) {
                // call when the timeout expires
                callbackOnReadTimeout(new ReadTimeout(getSelf(), replica, index));
            }

            return null;
        });
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
        // - Send message to replica with request details
        // - Set up timeout mechanism

        Duration timeout = Duration.ofMillis(getWriteTimeoutDelay());
        CompletionStage<Object> future = Patterns.ask(replica, new CSWrite(index, value), timeout);

        future.thenAccept(result -> {
            CSWriteResult writeResult = (CSWriteResult) result;
            // call when the result is received
            callbackOnWriteResult(new WriteResult(writeResult.success, writeResult.index, writeResult.value, writeResult.replicaId));
        }).exceptionally(ex -> {
            if(ex instanceof AskTimeoutException) {
                // call when the timeout expires
                callbackOnWriteTimeout(new WriteTimeout(getSelf(), replica, index, value));
            }

            return null;
        });
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
