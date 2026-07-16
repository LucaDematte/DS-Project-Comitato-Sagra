package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.cs.CSAsk;
import it.unitn.ds.cs.messages.CSAskTimeout;
import it.unitn.ds.cs.messages.client.CSReadRequest;
import it.unitn.ds.cs.messages.client.CSWriteRequest;
import it.unitn.ds.cs.messages.replica.CSReadResult;
import it.unitn.ds.cs.messages.replica.CSWriteResult;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public class Client extends AbstractClient {
    /**
     * Wrapper for {@code tell} with the same signature from {@code AbstractReplica.tell}
     * Needed to enable the client to use the custom ask system.
     *
     * @param m   The message to be sent.
     * @param dst The reference to the destination actor.
     */
    void tell(Serializable m, ActorRef dst) {
        dst.tell(m, getSelf());
    }
    
    /**
     * System to send messages (with ask) that expect a response within a given timeout.
     * More info at {@link CSAsk}.
     */
    CSAsk askSystem = new CSAsk(getContext(), this::tell);
    
    // =================================================================================
    // Builder methods & initialization
    // =================================================================================
    
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
                            () -> new Client(readTimeoutDelay,
                                             writeTimeoutDelay,
                                             defaultTargetReplica,
                                             Optional.empty()
                            )
        );
    }
    
    // Props method for automated tests
    public static Props propsWithListener(
            long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
            ActorRef listener
    ) {
        return Props.create(Client.class,
                            () -> new Client(readTimeoutDelay,
                                             writeTimeoutDelay,
                                             defaultTargetReplica,
                                             Optional.ofNullable(listener)
                            )
        );
    }
    
    // =================================================================================
    // READ & WRITE REQUESTS
    // =================================================================================
    
    @Override
    public void sendRead(ActorRef replica, int index) {
        Duration timeout = Duration.ofMillis(super.getReadTimeoutDelay());
        
        askSystem.<CSReadResult>ask(new CSReadRequest(index), replica, timeout, (res, timedOut) -> {
                                        if (!timedOut) {
                                            // call when the result is received
                                            callbackOnReadResult(new ReadResult(res.success,
                                                                                res.index,
                                                                                res.value,
                                                                                res.replicaId
                                            ));
                                        } else {
                                            // call when the timeout expires
                                            callbackOnReadTimeout(new ReadTimeout(getSelf(), replica, index));
                                        }
                                    }
        );
    }
    
    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        super.debug("Sending write request to " + replica.path()
                                                         .name() + ": set P[" + index + "] to " + value);
        
        Duration timeout = Duration.ofMillis(getWriteTimeoutDelay());
        
        askSystem.<CSWriteResult>ask(new CSWriteRequest(index, value),
                                     replica,
                                     timeout,
                                     (res, timedOut) -> {
                                         if (!timedOut) {
                                             // call when the result is received
                                             super.debug("Received WriteResult: P[" + res.index + "] = " + res.value + " (success = " + res.success + ")");
                                             callbackOnWriteResult(new WriteResult(res.success,
                                                                                   res.index,
                                                                                   res.value,
                                                                                   res.replicaId
                                             ));
                                         } else {
                                             // call when the timeout expires
                                             super.debug("Write timeout expired");
                                             callbackOnWriteTimeout(new WriteTimeout(getSelf(),
                                                                                     replica,
                                                                                     index,
                                                                                     value
                                             ));
                                         }
                                     }
        );
    }
    
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder().match(CSReadResult.class, askSystem::handleResponse)
                                         .match(CSWriteResult.class, askSystem::handleResponse)
                                         // ask handlers
                                         .match(CSAskTimeout.class, askSystem::handleTimeout)
                                         .build();
    }
}
