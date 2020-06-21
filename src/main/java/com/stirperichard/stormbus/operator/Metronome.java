package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.*;


public class Metronome extends BaseRichBolt {

    public static final String S_METRONOME 			= "sMetronome";
    public static final String F_MSGID				= "msgId";
    public static final String OCCURREDON_MILLIS    = "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String METRONOME_H          = "metronome_hour";
    public static final String METRONOME_D          = "metronome_day";
    public static final String METRONOME_W          = "metronome_week";
    public static final String METRONOME_M          = "metronome_month";
    public static final String DAY_PER_MONTH        = "day_per_month";

    private long currentTime;
    private OutputCollector collector;

    private long elapsedTime_h;
    private long elapsedTime_d;
    private long elapsedTime_w;
    private long elapsedTime_m;


    public Metronome(){

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.currentTime = 0;
        this.elapsedTime_h = 0;
        this.elapsedTime_d = 0;
        this.elapsedTime_w = 0;
        this.elapsedTime_m = 0;
    }

    @Override
    public void execute(Tuple input) {

        String SMsgId                 = input.getStringByField(ParseCSV.F_MSGID);
        String occurredOn             = input.getStringByField(ParseCSV.OCCURRED_ON);
        long occurredOnMillis         = input.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        String timestamp              = input.getStringByField(ParseCSV.F_TIMESTAMP);

        int day;

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(occurredOnMillis);
        int dayMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);

        MILLIS_MONTH = dayMonth * 24 * 60 * 60 * 1000;

        if (this.elapsedTime_h == 0)
            this.elapsedTime_h = occurredOnMillis;
        if (this.elapsedTime_d == 0)
            this.elapsedTime_d = occurredOnMillis;
        if (this.elapsedTime_w == 0)
            this.elapsedTime_w = occurredOnMillis;
        if (this.elapsedTime_m == 0)
            this.elapsedTime_m = occurredOnMillis;

        else {
            // Metronome sends tick every hour
            if (occurredOnMillis - this.elapsedTime_h >= MILLIS_HOUR) {
                this.elapsedTime_h = 0;
                Values values = new Values();
                values.add(SMsgId);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(timestamp);
                collector.emit(METRONOME_H, values);
            }

            // Metronome sends tick every day
            if (occurredOnMillis - this.elapsedTime_d >= MILLIS_DAY) {
                this.elapsedTime_d = 0;
                Values values = new Values();
                values.add(SMsgId);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(timestamp);
                collector.emit(METRONOME_D, values);
            }

            // Metronome sends tick every week
            if (occurredOnMillis - this.elapsedTime_w >= MILLIS_WEEK) {
                this.elapsedTime_w = 0;
                Values values = new Values();
                values.add(SMsgId);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(timestamp);
                collector.emit(METRONOME_W, values);
            }

            // Metronome sends tick every month
            if (occurredOnMillis - this.elapsedTime_m >= MILLIS_MONTH) {
                this.elapsedTime_m = 0;
                Values values = new Values();
                values.add(SMsgId);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(timestamp);
                collector.emit(METRONOME_M, values);
            }

        }

        collector.ack(input);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(F_MSGID, DAY_PER_MONTH, OCCURREDON_MILLIS, OCCURRED_ON, F_TIMESTAMP));

    }
}
