package test;


import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;
import java.util.ArrayList;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * Created by sseshac on 15/11/17.
 */

public class Main {


  public static void main(String[] args) {
    System.out.println("args = [" + args + "]");
    DseCluster.Builder dseBuilder = DseCluster.builder();
    String[] localDse = {"localhost"};
    String[] dse = {"10.219.250.21", "10.219.250.84", "10.219.250.188"};
    if("dse".equals(args[0])) {
      dseBuilder.addContactPoints(dse);
    }else {
      dseBuilder.addContactPoints(localDse);
    }
    dseBuilder.withGraphOptions(new GraphOptions().setGraphName("profilex_dev"));
    dseBuilder.withPoolingOptions(new PoolingOptions().setIdleTimeoutSeconds(1000000));
    long starSessionTime = System.currentTimeMillis();
    DseCluster dseCluster = dseBuilder.build();

    long EndSessionTime = System.currentTimeMillis();

    ArrayList<Thread> threadList = new ArrayList();
    for (int i = 0; i < 100; i++) {

      Thread dseThread = new DseClusterThread(dseCluster, i);
      threadList.add(dseThread);
    }

    for (int i = 0; i < 100; i++) {
      threadList.get(i).start();
    }


    for (int i = 0; i < 100; i++) {
      try {
        threadList.get(i).wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    System.out.println("Time for Session => " + (EndSessionTime - starSessionTime));


  }




}

class DseClusterThread extends Thread {

  DseCluster cluster;
  int i;

  public DseClusterThread(DseCluster cluster, int i) {
    this.cluster = cluster;
    this.i = i;
  }

  @Override
  public void run() {
    long startime = System.currentTimeMillis();
    DseSession dseSession = cluster.connect();
    GraphTraversalSource g = DseGraph.traversal(dseSession);
    g.addV("company").property("id", i).property("objectLabel", "company")
        .property("name", "Trimble").property("locations", "home", "type", "home").next();
    long endPoint = System.currentTimeMillis();
    long extec = endPoint - startime;
    System.out.println("Time for Query  => " + extec);
    System.out.println("open conenctions:"+(cluster.getMetrics().getOpenConnections().getValue()));
    dseSession.close();
  }
}
