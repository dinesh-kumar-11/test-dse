package test;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TestDse {

  private static DseCluster dseCluster = null;

  static {

  }

  public static void main(String [] args) throws Exception {

    try {
      String[] localDse = {"localhost"};
      String[] dse = {"10.219.250.21", "10.219.250.84", "10.219.250.188"};

      DseCluster.Builder dseBuilder = DseCluster.builder();
      if("dse".equals(args[0])) {
        dseBuilder.addContactPoints(dse);
      }else {
        dseBuilder.addContactPoints(localDse);
      }
      dseBuilder
          .withPort(9042)
          .withProtocolVersion(ProtocolVersion.V1)
          .withGraphOptions(new GraphOptions().setGraphName("profilex_dev"));

      if("pool".equals(args[2])) {
         dseBuilder.withPoolingOptions(new PoolingOptions().setIdleTimeoutSeconds(1000000).setConnectionsPerHost(HostDistance.REMOTE, 100, 100)
            .setMaxConnectionsPerHost(HostDistance.LOCAL, 100).setMaxConnectionsPerHost(HostDistance.REMOTE, 100));
      }

      dseCluster = dseBuilder.build();

    }
    catch(Exception ex) {
      System.out.println("Dse Cluster Initialization failed"+ ex);
      throw new RuntimeException("Exception in Dse Cluster", ex);
    }
    DseSession session = dseCluster.connect();
    List<Thread> threads =null;
    for(int ir=0;ir<10;ir++) {
      threads =new ArrayList<Thread>();
      int count =100;
      for(int i=0;i<count;i++) {
        DSEThread dseThread = new DSEThread(dseCluster, session, args[1]);
        threads.add(dseThread);
      }
      for(int i=0;i<count;i++) {
        threads.get(i).start();
      }
      for(int i=0;i<count;i++) {
        threads.get(i).join();
      }
      System.out.println("End of iteration:"+ir);
      Thread.sleep(100);
    }
    System.out.println("End of Main");
    //dseCluster.close();
  }
}

class DSEThread extends Thread{
  DseCluster cluster = null;
  DseSession session;
  String method = "executeGraph";
  public DSEThread(DseCluster cluster, DseSession session, String methods) {
    this.cluster = cluster;
    this.session = session;
    method = methods;
  }
  public void run() {
    if("traversal".equals(method)) {
      System.out.println(Thread.currentThread().getName() + " started");
      long a = System.currentTimeMillis();
      //DseSession localSession = cluster.connect();
      GraphTraversalSource g = DseGraph.traversal(session);
      g.addV("company").property("id", Thread.currentThread().getName() + System
          .currentTimeMillis()).property("objectLabel", "company")
          .property("name", "Trimble").property("locations", "home", "type", "home").next();
      long b = System.currentTimeMillis();
      //localSession.close();
      if (cluster.getMetrics() != null) {
        System.out.println(
            (new Date()) + " " + Thread.currentThread().getName() + " total time:" + (b - a)
                + ", open conenctions:" + (cluster.getMetrics().getOpenConnections().getValue()));
      }
    }else {
      System.out.println(Thread.currentThread().getName() + " started");
      long a = System.currentTimeMillis();
      DseSession session = cluster.connect();
      String query =
          "g.addV(\"company\").property(\"id\", '" + Thread.currentThread().getName() + System
              .currentTimeMillis() + "').property(\"objectLabel\", \"company\")\n"
              + "        .property(\"name\", \"Trimble\").property(\"locations\", \"home\", \"type\", \"home\")";
      session.executeGraph(query).all();
      session.close();
      long b = System.currentTimeMillis();
      if (cluster.getMetrics() != null) {
        System.out.println(
            (new Date()) + " " + Thread.currentThread().getName() + " total time:" + (b - a)
                + ", open conenctions:" + (cluster.getMetrics().getOpenConnections().getValue()));
      }
    }

  }

}