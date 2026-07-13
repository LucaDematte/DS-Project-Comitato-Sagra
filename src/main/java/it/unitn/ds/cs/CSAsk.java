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

public class CSAsk {
    private static final Map<UUID, PendingAsk> pending = new HashMap<>();
    
    @FunctionalInterface
    public interface Sender {
        void send(Serializable msg, ActorRef dst);
    }
    
    @FunctionalInterface
    public interface Callback<T extends CSAskMessage> {
        void handle(T response, boolean timedOut);
    }
    
    public static <T extends CSAskMessage> void ask(
            AbstractActor.ActorContext context, Sender sender, CSAskMessage msg,
            ActorRef destination, Duration timeout, Callback<T> callback
    ) {
        Cancellable timer = context.system()
                                   .scheduler()
                                   .scheduleOnce(timeout, context.self(),
                                           new CSAskTimeout(msg.askUUID), context.dispatcher(),
                                           ActorRef.noSender());
        
        pending.put(msg.askUUID, new PendingAsk<T>(callback, timer));
        sender.send(msg, destination);
    }
    
    public static void handleResponse(CSAskMessage msg) {
        PendingAsk p = pending.remove(msg.askUUID);
        if (p == null) {
            // Nessuna ask in sospeso con questo id: il timeout è già scattato prima,
            // oppure è una risposta duplicata/inattesa. La ignoriamo in sicurezza.
            return;
        }
        p.timer.cancel();
        p.callback.handle(msg, false);
    }
    
    public static void handleTimeout(CSAskTimeout timeout) {
        PendingAsk p = pending.remove(timeout.uuid);
        if (p == null) {
            // La risposta era già arrivata prima dello scadere del timeout: niente da fare.
            return;
        }
        p.callback.handle(null, true);
    }
    
    private static final class PendingAsk<T extends CSAskMessage> {
        final Callback<T> callback;
        final Cancellable timer;
        
        PendingAsk(Callback<T> callback, Cancellable timer) {
            this.callback = callback;
            this.timer = timer;
        }
    }
}
