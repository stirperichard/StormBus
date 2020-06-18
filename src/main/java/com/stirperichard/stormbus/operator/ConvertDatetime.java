package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ConvertDatetime extends BaseRichBolt {

    public static final String F_MSGID				= "MSGID";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_TIMESTAMP    = "dropoff_datetime";

    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public ConvertDatetime(){
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple input) {

        String msgId 	        = input.getStringByField(ParseCSV.F_MSGID);
        String boro             = input.getStringByField(ParseCSV.BORO);
        String howLongDelayed   = input.getStringByField(ParseCSV.HOW_LONG_DELAYED);
        String occurredOn       = input.getStringByField(ParseCSV.OCCURRED_ON);
        String timestamp        = input.getStringByField(ParseCSV.F_TIMESTAMP);

        String dTimestamp = null;
        try {

            //Converto la data in un formato
            Date dDate = sdf.parse(occurredOn);

            //Timestamp della data
            dTimestamp = String.valueOf(dDate.getTime());

        } catch (ParseException e) {
            collector.ack(input);
            return;
        }

        Values values = new Values();
        values.add(msgId);
        values.add(boro);
        values.add(howLongDelayed);
        values.add(occurredOn);
        values.add(dTimestamp);
        values.add(timestamp);

        collector.emit(values);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(
                new Fields(
                        F_MSGID,
                        BORO, HOW_LONG_DELAYED, OCCURRED_ON, DROPOFF_TIMESTAMP,
                        F_TIMESTAMP));

    }
}
