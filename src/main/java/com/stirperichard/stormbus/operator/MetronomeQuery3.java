package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class MetronomeQuery3 extends BaseRichBolt {

    public static final String S_METRONOME 			= "sMetronome";
    public static final String F_TIME				= "time";

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private long currentTime = 0;
    private long latestMsgId = 0;

    public MetronomeQuery3(){

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        /* Emit message every (simulated) minute */
        String busBreakdownId = tuple.getStringByField(DataGenerator.BUS_BREAKDOWN_ID);
        String reason 		= tuple.getStringByField(DataGenerator.REASON);
        String occurredOn 		= tuple.getStringByField(DataGenerator.OCCURRED_ON);
        String boro 	= tuple.getStringByField(DataGenerator.BORO);
        String busCompanyName 	= tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
        String howLongDelayed	= tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

        long msgId = Long.parseLong(busBreakdownId);
        long time = roundToCompletedMinute(occurredOn);

        if (this.latestMsgId < msgId &&
                this.currentTime < time){

            this.latestMsgId = msgId;
            this.currentTime = time;

            Values values = new Values();
            values.add(busBreakdownId);
            values.add(reason);
            values.add(String.valueOf(time));
            values.add(busCompanyName);
            values.add(howLongDelayed);

            collector.emit(S_METRONOME, values);   // To observe: event time

        } else {
            /* time did not go forward */
        }

        collector.ack(tuple);

    }


    private long roundToCompletedMinute(String timestamp) {

        Date d = new Date(Long.parseLong(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(DataGenerator.BUS_BREAKDOWN_ID,
                DataGenerator.REASON, F_TIME, DataGenerator.BUS_COMPANY_NAME,
                DataGenerator.HOW_LONG_DELAYED));

    }

}