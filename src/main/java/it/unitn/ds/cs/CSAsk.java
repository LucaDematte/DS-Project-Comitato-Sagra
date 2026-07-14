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
    private final AbstractActor.ActorContext context;
    private final Sender sender;
    private final Map<UUID, PendingAsk> pending = new HashMap<>();
    
    public CSAsk(AbstractActor.ActorContext context, Sender sender) {
        this.context = context;
        this.sender = sender;
    }
    
    @FunctionalInterface
    public interface Sender {
        void send(Serializable msg, ActorRef dst);
    }
    
    @FunctionalInterface
    public interface Callback<T extends CSAskMessage> {
        void handle(T response, boolean timedOut);
    }
    
    public <T extends CSAskMessage> void ask(
            CSAskMessage msg, ActorRef destination,
            Duration timeout, Callback<T> callback
    ) {
        Cancellable timer = this.context.system()
                                        .scheduler()
                                        .scheduleOnce(timeout, this.context.self(),
                                                new CSAskTimeout(msg.askUUID),
                                                this.context.dispatcher(), ActorRef.noSender());
        
        pending.put(msg.askUUID, new PendingAsk<T>(callback, timer));
        this.sender.send(msg, destination);
    }
    
    public void handleResponse(CSAskMessage msg) {
        PendingAsk p = pending.remove(msg.askUUID);
        if (p == null) {
            // Nessuna ask in sospeso con questo id: il timeout è già scattato prima,
            // oppure è una risposta duplicata/inattesa. La ignoriamo in sicurezza.
            return;
        }
        p.timer.cancel();
        p.callback.handle(msg, false);
    }
    
    public void handleTimeout(CSAskTimeout timeout) {
        PendingAsk p = pending.remove(timeout.uuid);
        if (p == null) {
            // La risposta era già arrivata prima dello scadere del timeout: niente da fare.
            return;
        }
        p.callback.handle(null, true);
    }
    
    private final class PendingAsk<T extends CSAskMessage> {
        final Callback<T> callback;
        final Cancellable timer;
        
        PendingAsk(Callback<T> callback, Cancellable timer) {
            this.callback = callback;
            this.timer = timer;
        }
    }
}
