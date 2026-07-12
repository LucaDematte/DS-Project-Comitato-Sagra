package it.unitn.ds.cs;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.cs.messages.AskRequest;
import it.unitn.ds.cs.messages.AskResponse;
import it.unitn.ds.cs.messages.AskTimeout;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Simula il pattern "ask" di Akka appoggiandosi al metodo tell custom dell'attore,
 * garantendo che la callback (sia in caso di risposta che di timeout) venga eseguita
 * nel thread di elaborazione dei messaggi dell'attore, così da poter agire in sicurezza
 * sul suo stato interno.
 * <p>
 * Uso previsto: un'istanza per attore, tenuta come campo dell'attore stesso, es.:
 * <p>
 * private final AskResponseSystem askSupport = new AskResponseSystem(getContext(), this::tell);
 * <p>
 * L'attore deve inoltre agganciare handleResponse/handleTimeout al proprio receiveBuilder:
 * <p>
 * .match(AskResponse.class, askSupport::handleResponse)
 * .match(AskTimeout.class, askSupport::handleTimeout)
 * <p>
 * Nota: il riferimento this::tell nel costruttore richiede che il metodo tell sia
 * visibile alla sottoclasse (quindi almeno protected, non private) nella superclasse
 * comune dei vostri attori.
 */
public class AskResponseSystem {
    
    private final AbstractActor.ActorContext context;
    private final BiConsumer<Serializable, ActorRef> sender;
    private final Map<UUID, PendingAsk> pending = new HashMap<>();
    
    /**
     * @param context il context dell'attore proprietario (getContext())
     * @param sender  riferimento al metodo tell dell'attore proprietario (es. this::tell),
     *                usato per inviare effettivamente il messaggio di andata
     */
    public AskResponseSystem(
            AbstractActor.ActorContext context, BiConsumer<Serializable, ActorRef> sender) {
        this.context = context;
        this.sender = sender;
    }
    
    /**
     * Invia payload a destination avvolto in una AskRequest, registra la callback da
     * eseguire alla ricezione della AskResponse corrispondente (stesso correlationId),
     * e schedula un AskTimeout a sé stesso in modo che la callback scatti comunque
     * anche se non arriva nessuna risposta entro il timeout.
     */
    public <T extends Serializable> void ask(
            Serializable payload, ActorRef destination,
            java.time.Duration timeout, AskCallback<T> callback
    ) {
        UUID id = UUID.randomUUID();
        
        FiniteDuration delay = FiniteDuration.create(timeout.toMillis(), TimeUnit.MILLISECONDS);
        Cancellable timeoutTask = context.system()
                                         .scheduler()
                                         .scheduleOnce(delay, context.self(), new AskTimeout(id),
                                                 context.dispatcher(), ActorRef.noSender());
        
        @SuppressWarnings("unchecked") AskCallback<Serializable> erased = (AskCallback<Serializable>) callback;
        pending.put(id, new PendingAsk(erased, timeoutTask));
        
        sender.accept(new AskRequest(id, payload), destination);
    }
    
    /**
     * Da agganciare al receiveBuilder per gestire le risposte in arrivo:
     * .match(AskResponse.class, askSupport::handleResponse)
     */
    public void handleResponse(AskResponse response) {
        PendingAsk p = pending.remove(response.getCorrelationId());
        if (p == null) {
            // Nessuna ask in sospeso con questo id: il timeout è già scattato prima,
            // oppure è una risposta duplicata/inattesa. La ignoriamo in sicurezza.
            return;
        }
        p.timeoutTask.cancel();
        p.callback.onComplete(response.getPayload(), false);
    }
    
    /**
     * Da agganciare al receiveBuilder per gestire i timeout:
     * .match(AskTimeout.class, askSupport::handleTimeout)
     */
    public void handleTimeout(AskTimeout timeout) {
        PendingAsk p = pending.remove(timeout.getCorrelationId());
        if (p == null) {
            // La risposta era già arrivata prima dello scadere del timeout: niente da fare.
            return;
        }
        p.callback.onComplete(null, true);
    }
    
    /**
     * Costruisce la AskResponse da rimandare al mittente originale di una AskRequest,
     * preservando il correlationId. Da inviare con il proprio tell:
     * <p>
     * tell(AskResponseSystem.reply(request, myPayload), getSender());
     */
    public static AskResponse reply(AskRequest request, Serializable responsePayload) {
        return new AskResponse(request.getCorrelationId(), responsePayload);
    }
    
    /**
     * Cancella tutti i timeout ancora pendenti. Utile da chiamare in postStop()
     * quando l'attore viene fermato, per non lasciare task schedulati inutili.
     */
    public void cancelAll() {
        pending.values().forEach(p -> p.timeoutTask.cancel());
        pending.clear();
    }
    
    private static final class PendingAsk {
        final AskCallback<Serializable> callback;
        final Cancellable timeoutTask;
        
        PendingAsk(AskCallback<Serializable> callback, Cancellable timeoutTask) {
            this.callback = callback;
            this.timeoutTask = timeoutTask;
        }
    }
}
