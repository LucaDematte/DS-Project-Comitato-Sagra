package it.unitn.ds.cs;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.cs.messages.CSAskMessage;
import it.unitn.ds.cs.messages.CSAskTimeout;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The ask system enables an actor to send messages and execute an operation when a response is
 * received or when a specified timeout expires.
 * The behavior is similar to {@code Patterns.ask} (provided by Akka), but this implementation uses
 * the custom {@code tell} method (of {@link it.unitn.ds.AbstractReplica}) that all replicas in this
 * project are required to use.
 * <p>
 * To make this work, the class {@link CSAskMessage} must be extended by messages that are sent
 * with this system.
 * This class contains a {@link UUID} that is used to correlate a request with a response.
 * Therefore, a response must be sent with the same {@link UUID} as the request.
 * </p>
 * When asking a message, a callback can be registered to be executed when the response is received
 * or when the specified timeout expires.
 */
public class CSAsk {
    /**
     * Context of the actor using the ask system.
     * Used to schedule a reminder for the callback in case the timeout expires.
     */
    private final AbstractActor.ActorContext context;
    /**
     * The underlying method that actors use when sending a normal message (for replicas it's the
     * custom {@code tell}).
     */
    private final Sender sender;
    /** The map that keeps all the pending ask operations and the timeout timers. */
    private final Map<UUID, PendingAsk> pending = new HashMap<>();
    
    public CSAsk(AbstractActor.ActorContext context, Sender sender) {
        this.context = context;
        this.sender = sender;
    }
    
    /** This interface defines the shape of the method to be used to send messages. */
    @FunctionalInterface
    public interface Sender {
        void send(Serializable msg, ActorRef dst);
    }
    
    /**
     * This interface defines the shape of the callback that can be registered to be executed when
     * a response is received or when the timeout expires.
     */
    @FunctionalInterface
    public interface Callback<T extends CSAskMessage> {
        /**
         * The shape of the callback for the response of an ask message.
         *
         * @param response The message received as a response.
         * @param timedOut Flag set to {@code true} if the callback was called after the timeout
         *                 expired.
         */
        void handle(T response, boolean timedOut);
    }
    
    /**
     * This method is the core functionality of this class: sends a request (ask), registers a
     * callback to be executed when the response is received or when the timeout expires.
     *
     * @param msg         The message to be sent as request.
     * @param destination The actor that will receive the message.
     * @param timeout     The timeout after which the callback is executed if a response is not
     *                    received.
     * @param callback    The callback to be executed when the response is received or when the
     *                    timeout expires.
     * @param <T>         The type of the expected response.
     */
    public <T extends CSAskMessage> void ask(
            CSAskMessage msg, ActorRef destination,
            Duration timeout, Callback<T> callback
    ) {
        Cancellable timer = this.context.system()
                                        .scheduler()
                                        .scheduleOnce(timeout,
                                                      this.context.self(),
                                                      new CSAskTimeout(msg.askUUID),
                                                      this.context.dispatcher(),
                                                      ActorRef.noSender()
                                        );
        
        pending.put(msg.askUUID, new PendingAsk<T>(callback, timer));
        this.sender.send(msg, destination);
    }
    
    /**
     * This method must be registered in the actor's receive bindings so that, when a response is
     * received, the ask system can execute the right callback.
     *
     * @param msg The message received (representing a response to a previous request).
     */
    public void handleResponse(CSAskMessage msg) {
        PendingAsk p = pending.remove(msg.askUUID);
        if (p == null) {
            // If there is no pending ask for this id: the timeout must have already expired or
            // this is a duplicate/unexpected response. It can be safely ignored
            return;
        }
        p.timer.cancel();   // Canceling the timer
        p.callback.handle(msg, false);
    }
    
    /**
     * This method must be registered in the actor's receive bindings to enable timeout expiration
     * tracking.
     * A {@link CSAskTimeout} message is scheduled for every request, and when it is received before
     * the response the callback is fired.
     *
     * @param timeout The timeout message.
     */
    public void handleTimeout(CSAskTimeout timeout) {
        PendingAsk p = pending.remove(timeout.uuid);
        if (p == null) {
            // If there is no pending ask for this id: the response arrived on time, so nothing to do
            return;
        }
        p.callback.handle(null, true);
    }
    
    public void cancelAllCallbacks() {
        this.pending.forEach((uuid, p) -> {
            p.timer.cancel();
        });
        this.pending.clear();

//        for (var p : this.pending.entrySet()) {
//            p.getValue().timer.cancel();
//            this.pending.remove(p.getKey());
//        }
    }
    
    /**
     * An object representing a pending request. It stores the callback to be executed and the timer
     * after which the response is considered out of time.
     *
     * @param <T> The type of the expected response.
     */
    private final class PendingAsk<T extends CSAskMessage> {
        final Callback<T> callback;
        final Cancellable timer;
        
        PendingAsk(Callback<T> callback, Cancellable timer) {
            this.callback = callback;
            this.timer = timer;
        }
    }
}
