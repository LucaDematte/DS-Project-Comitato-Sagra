package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 4;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // TODO: Create your clients
        ActorRef client1 = system.actorOf(
                Client.props(AbstractReplica.MAX_LATENCY, 0, Optional.of(replicas.get(0)))
        );
        
        // TODO: Implement your main logic
        client1.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
