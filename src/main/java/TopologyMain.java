import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[]args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("split-bolt", new SplitBolt())
                .shuffleGrouping("word-reader");
        builder.setBolt("count-bolt", new CountBolt(), 2)
                .fieldsGrouping("split-bolt", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("fileToRead", "/Users/Ahmad/Desktop/quran-simple-min.txt");
        conf.put("dirToWrite", "/Users/Ahmad/Desktop/");

        conf.setDebug(true);

        //Topology run

        LocalCluster cluster = new LocalCluster();

        try{
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            Thread.sleep(30000);
        } finally {
            cluster.shutdown();
        }
    }
}
