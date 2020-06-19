package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.TimeUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;



public class Metronome extends BaseRichBolt {

    public static final String S_METRONOME 			= "sMetronome";
    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_TIMESTAMP    = "dropoff_datetime";

    private long currentTime;
    private OutputCollector collector;

    public static final long MILLIS_HOUR = 1000*60*60;

    public Metronome(){

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.currentTime = 0;
    }

    @Override
    public void execute(Tuple input) {

        String SMsgId                 = input.getStringByField(ParseCSV.F_MSGID);
        String occurredOn             = input.getStringByField(ParseCSV.OCCURRED_ON);
        String occurredOnMillis       = input.getStringByField(ParseCSV.OCCURRED_ON_MILLIS);
        String timestamp              = input.getStringByField(ParseCSV.F_TIMESTAMP);


        long hourTime   = TimeUtils.roundToCompletedHour(occurredOnMillis);


        if (this.currentTime < hourTime){

            this.currentTime = hourTime;

            Values values = new Values();
            values.add(SMsgId);
            values.add(hourTime);
            values.add(occurredOn);
            values.add(timestamp);
            collector.emit(S_METRONOME, values);   // To observe: event time

        }
        collector.ack(input);
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(F_MSGID, F_TIME, OCCURRED_ON, F_TIMESTAMP));

    }
}
