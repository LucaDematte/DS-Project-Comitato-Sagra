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
        
        final int N_REPLICAS = 20;
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
        
        // Client creation
        ActorRef client1 = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))));
        ActorRef client2 = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))));
        ActorRef client3 = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))));
        ActorRef client4 = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))));
        ActorRef client5 = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))));
        
        // Main logic
        System.out.println("========================================");
        System.out.println("SINGLE CLIENT WRITE");
        System.out.println("========================================\n");
        
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(1)), ActorRef.noSender());
        System.in.read();
        client1.tell(new AbstractClient.WriteRequest(0, 10, replicas.get(1)), ActorRef.noSender());
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(0, replicas.get(1)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("MULTIPLE CLIENT WRITES");
        System.out.println("========================================\n");
        
        System.in.read();
        client1.tell(new AbstractClient.WriteRequest(1, 10, replicas.get(1)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(1, 20, replicas.get(1)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(1, 30, replicas.get(1)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(1, 40, replicas.get(1)), ActorRef.noSender());
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(0)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(1)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(2)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(3)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(4)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(1, replicas.get(5)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("MULTIPLE CLIENT WRITES SENT TO DIFFERENT REPLICAS");
        System.out.println("========================================\n");
        
        System.in.read();
        client1.tell(new AbstractClient.WriteRequest(2, 10, replicas.get(0)), ActorRef.noSender());
        client2.tell(new AbstractClient.WriteRequest(2, 20, replicas.get(1)), ActorRef.noSender());
        client3.tell(new AbstractClient.WriteRequest(2, 30, replicas.get(2)), ActorRef.noSender());
        client4.tell(new AbstractClient.WriteRequest(2, 40, replicas.get(3)), ActorRef.noSender());
        client5.tell(new AbstractClient.WriteRequest(2, 50, replicas.get(4)), ActorRef.noSender());
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(0)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(1)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(2)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(3)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(4)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(2, replicas.get(5)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("ELECTION DURING CLIENT WRITE");
        System.out.println("========================================\n");
        
        System.in.read();
        client1.tell(new AbstractClient.WriteRequest(3, 10, replicas.get(2)), ActorRef.noSender());
        replicas.get(0)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                      ActorRef.noSender()
                );
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(3, replicas.get(2)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("TWO CONSECUTIVE REPLICAS CRASH DURING ELECTION");
        System.out.println("========================================\n");
        
        System.in.read();
        replicas.get(2)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                      ActorRef.noSender()
                );
        replicas.get(3)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                      ActorRef.noSender()
                );
        replicas.get(1)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                      ActorRef.noSender()
                );
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("COORDINATOR CRASHES DURING UPDATE PROPAGATION");
        System.out.println("========================================\n");
        
        System.in.read();
        replicas.get(4)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, 4),
                      ActorRef.noSender()
                );
        client1.tell(new AbstractClient.WriteRequest(5, 10, replicas.get(5)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("WINNER OF ELECTION CRASHES BEFORE BECOMING COORDINATOR");
        System.out.println("========================================\n");
        
        System.in.read();
        replicas.get(5)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                      ActorRef.noSender()
                );
        // Make 19 initiate a write so that it should be the first one to realize that the coordinator crashed
        // This way 6 will receive only 1 incomplete election message before the election ends
        client1.tell(new AbstractClient.WriteRequest(6, 10, replicas.get(19)), ActorRef.noSender());
        replicas.get(6)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Election, 0),
                      ActorRef.noSender()
                );
        // Uncomment this to also crash the following replica (7)
        replicas.get(7)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Election, 0),
                      ActorRef.noSender()
                );
        System.in.read();
        
        System.out.println("========================================");
        System.out.println("COORDINATOR CRASHES WHILE SENDING WRITEOK");
        System.out.println("========================================\n");
        
        System.in.read();
        replicas.get(8)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 3),
                      ActorRef.noSender()
                );
        client1.tell(new AbstractClient.WriteRequest(7, 10, replicas.get(10)), ActorRef.noSender());
        System.in.read();
        
        System.out.println("========================================");
        System.out.println(
                "COORDINATOR CRASHES WHILE SENDING WRITEOK & UPDATES ARRIVE DURING ELECTION");
        System.out.println("========================================\n");
        
        System.in.read();
        replicas.get(9)
                .tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 3),
                      ActorRef.noSender()
                );
        // The last 2 or 3 updates should arrive after the coordinator has crashed, will be processed after the election
        client1.tell(new AbstractClient.WriteRequest(8, 10, replicas.get(10)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(8, 20, replicas.get(10)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(8, 30, replicas.get(10)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(8, 40, replicas.get(10)), ActorRef.noSender());
        client1.tell(new AbstractClient.WriteRequest(8, 50, replicas.get(10)), ActorRef.noSender());
        System.in.read();
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(10)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(11)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(12)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(13)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(14)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(15)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(16)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(17)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(18)), ActorRef.noSender());
        client1.tell(new AbstractClient.ReadRequest(8, replicas.get(19)), ActorRef.noSender());
        System.in.read();
        
        system.terminate();
        
        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }
}
