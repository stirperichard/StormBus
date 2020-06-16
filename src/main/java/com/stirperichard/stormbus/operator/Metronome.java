package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;



public class Metronome extends BaseRichBolt {

    public static final String S_METRONOME 			= "sMetronome";
    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_TIMESTAMP    = "dropoff_datetime";

    private long currentTime = 0;
    private long latestMsgId = 0;
    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public Metronome(){

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {

        String SMsgId         = input.getStringByField(ConvertDatetime.F_MSGID);
        String occurredOn     = input.getStringByField(ConvertDatetime.OCCURRED_ON);
        String datetime       = input.getStringByField(ConvertDatetime.DROPOFF_TIMESTAMP);
        String timestamp      = input.getStringByField(ConvertDatetime.F_TIMESTAMP);


        long IMsgId = Long.valueOf(SMsgId);
        long time = roundToCompletedMinute(datetime);

        if (this.latestMsgId < IMsgId && this.currentTime < time){

            this.latestMsgId = IMsgId;
            this.currentTime = time;

            Values values = new Values();
            values.add(SMsgId);
            values.add(String.valueOf(time));
            values.add(occurredOn);
            values.add(timestamp);
            collector.emit(S_METRONOME, values);   // To observe: event time

        } else {
            /* time did not go forward */

        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(F_MSGID, F_TIME, OCCURRED_ON, F_TIMESTAMP));

    }

    //Setto a 0 i millisecondi del timestamp
    private long roundToCompletedMinute(String timestamp) {

        Date d = new Date(Long.valueOf(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();

    }
}
