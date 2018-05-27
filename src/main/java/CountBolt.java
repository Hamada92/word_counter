import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;
    String fileName;

    public void prepare(Map stormConf, TopologyContext context){
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.fileName = stormConf.get("dirToWriteT").toString()+
                "output" + "-" + context.getThisComponentId() + ".txt";
    }

    public void execute(Tuple input, BasicOutputCollector collector){
        String str = input.getString(0);

        if(!counters.containsKey(str)){
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
    }

    public void cleanup(){
        try{
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            for(Map.Entry<String, Integer> entry : counters.entrySet()){
                writer.println(entry.getKey() + ": " + entry.getValue());
            }
            writer.close();
        } catch (Exception e) {}
    }
}
