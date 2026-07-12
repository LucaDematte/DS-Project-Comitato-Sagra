package it.unitn.ds.cs;

import akka.actor.ActorRef;
import it.unitn.ds.cs.messages.CSCallbackMessage;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class ActorCallbacks {
    
    @FunctionalInterface
    public interface ActorCallback {
        void execute(Object response, Throwable failure);
    }
    
    private final ActorRef self;
    
    private final Map<UUID, ActorCallback> callbacks = new ConcurrentHashMap<>();
    
    public ActorCallbacks(ActorRef self) {
        this.self = self;
    }
    
    public void attach(CompletionStage<?> future, ActorCallback callback) {
        UUID id = UUID.randomUUID();
        
        callbacks.put(id, callback);
        
        future.whenComplete((result, error) -> {
            self.tell(new CSCallbackMessage(id, result, error), ActorRef.noSender());
        });
    }
    
    public void handle(CSCallbackMessage message) {
        ActorCallback callback = callbacks.remove(message.id());
        
        if (callback == null)
            return;
        
        callback.execute(message.response(), message.failure());
    }
}