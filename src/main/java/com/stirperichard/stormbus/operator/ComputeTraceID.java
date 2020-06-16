package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Map;

public class ComputeTraceID extends BaseRichBolt {

    public static final String F_MSGID				= "MSGID";
    public static final String F_TIME 				= "time";
    public static final String F_TIMESTAMP			= "timestamp";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String OCCURRED_ON       	= "occurredOn";

    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public ComputeTraceID() {

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String msgId 		  = input.getStringByField(ConvertDatetime.F_MSGID);
        String boro           = input.getStringByField(ConvertDatetime.BORO);
        String howLongDelayed = input.getStringByField(ConvertDatetime.HOW_LONG_DELAYED);
        String occurredOn     = input.getStringByField(ConvertDatetime.OCCURRED_ON);
        String timestamp	  = input.getStringByField(ConvertDatetime.F_TIMESTAMP);
        String dropoff_timestamp       = input.getStringByField(ConvertDatetime.DROPOFF_TIMESTAMP);

        Values values = new Values();
        values.add(msgId);
        values.add(boro);
        values.add(howLongDelayed);
        values.add(occurredOn);
        values.add(dropoff_timestamp);
        values.add(timestamp);

        collector.emit(values);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(F_MSGID, BORO, HOW_LONG_DELAYED, OCCURRED_ON, F_TIME, F_TIMESTAMP));

    }
}
