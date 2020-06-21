package com.stirperichard.stormbus.operator;


/*  To observe: - BaseWindowedBolt instead BaseRichBolt;
                - tick mechanism managed directly by Storm 1.0 (execution time, NOT event time)
 */

import com.stirperichard.stormbus.enums.Reason;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterReason extends BaseRichBolt {


    Map<String, Integer> counts = new HashMap<>();
    OutputCollector collector;

    public FilterReason() {

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


    @Override
    public void execute(Tuple tuple) {

        String busBreakdownId = tuple.getStringByField(DataGenerator.BUS_BREAKDOWN_ID);
        String reason = tuple.getStringByField(DataGenerator.REASON);
        String occurredOn = tuple.getStringByField(DataGenerator.OCCURRED_ON);
        String boro = tuple.getStringByField(DataGenerator.BORO);
        String busCompanyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
        String howLongDelayed = tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

        Values values = new Values();
        values.add(busBreakdownId);
        values.add(computeReason(reason));
        values.add(occurredOn);
        values.add(busCompanyName);
        values.add(howLongDelayed);

        collector.emit(values);
        collector.ack(tuple);

    }


    private String computeReason(String reason) {

        if (reason.equals("Heavy Traffic")) {
            return "Heavy Traffic";
        } else if (reason.equals("Mechanical Problem")) {
            return "Mechanical Problem";
        } else
            return "Other Reason";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(DataGenerator.BUS_BREAKDOWN_ID, DataGenerator.REASON,
                DataGenerator.OCCURRED_ON, DataGenerator.BUS_COMPANY_NAME,
                DataGenerator.HOW_LONG_DELAYED));
    }
}