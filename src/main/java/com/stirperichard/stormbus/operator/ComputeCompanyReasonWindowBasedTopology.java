package com.stirperichard.stormbus.operator;


/*  To observe: - BaseWindowedBolt instead BaseRichBolt;
                - tick mechanism managed directly by Storm 1.0 (execution time, NOT event time)
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComputeCompanyReasonWindowBasedTopology extends BaseWindowedBolt {


    Map<String, Integer> counts = new HashMap<>();
    OutputCollector collector;

    public ComputeCompanyReasonWindowBasedTopology(){

    }

    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tuples) {

        // Get the list of newly added events in the window since the last time the window was generated.
        List<Tuple> incoming = tuples.getNew();
        String companyName = null;

        for (Tuple tuple : incoming){

            String reason = tuple.getString(0);
            companyName = tuple.getString(3);
            Integer count = counts.get(reason);
            if (count == null)
                count = 0;
            count++;
            counts.put(reason, count);

        }

        // Get the list of events expired from the window since the last time the window was generated.
        List<Tuple> expired = tuples.getExpired();

        for (Tuple tuple : expired){

            String reason = tuple.getString(0);
            Integer count = counts.get(reason);
            if (count != null){
                count--;
                if (count > 0)
                    counts.put(reason, count);
                else
                    counts.remove(reason);
            }

        }

        for (String reason : counts.keySet()){
            Integer count = counts.get(reason);
            collector.emit(new Values(reason, count));

            System.out.println("\033[0;32m" + new Values(companyName, reason, count) + "\033[0m");

        }
        System.out.println("********");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("reason", "count"));
    }
}