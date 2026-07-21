package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Main {
    
    public static void main(String[] args) throws IOException {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");
        
        final int N_REPLICAS = 5;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");
        
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        
        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                         system.actorOf(Replica.props(i,
                                                      AbstractReplica.MIN_LATENCY,
                                                      AbstractReplica.MAX_LATENCY,
                                                      AbstractReplica.COORDINATOR_BEAT_INTERVAL
                                        ), "Replica_" + i
                         )
            );
        }
        
        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }
        
        // TODO: Create your clients
        ActorRef client1 = system.actorOf(Client.props(1000, 1000, Optional.of(replicas.get(0))));
        
        // TODO: Implement your main logic
        client1.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        System.in.read();
        
        replicas.get(0)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 0),
                      ActorRef.noSender()
                );
        client1.tell(new AbstractClient.WriteRequest(0, 10, replicas.get(1)), ActorRef.noSender());
//        client1.tell(new AbstractClient.WriteRequest(0, 20, replicas.get(1)), ActorRef.noSender());
//        client1.tell(new AbstractClient.WriteRequest(0, 30, replicas.get(1)), ActorRef.noSender());
//        client1.tell(new AbstractClient.WriteRequest(0, 40, replicas.get(1)), ActorRef.noSender());
//        client1.tell(new AbstractClient.WriteRequest(0, 50, replicas.get(1)), ActorRef.noSender());

//        System.in.read();
//
//        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(0)), ActorRef.noSender());
//
//        System.in.read();
//
//        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(1)), ActorRef.noSender());
//
//        System.in.read();
//
//        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(2)), ActorRef.noSender());
//
//        System.in.read();
//
//        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(3)), ActorRef.noSender());
//
//        System.in.read();
//
//        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(4)), ActorRef.noSender());
        
        System.in.read();
        system.terminate();
        
        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }
}
