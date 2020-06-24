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

import static com.stirperichard.stormbus.utils.Constants.*;
import static com.stirperichard.stormbus.utils.Constants.MILLIS_MONTH;


public class Metronome extends BaseRichBolt {

    public static final String S_METRONOME          = "sMetronome";
    public static final String F_MSGID              = "msgId";
    public static final String OCCURREDON_MILLIS    = "time";
    public static final String OCCURRED_ON          = "occurredOn";
    public static final String METRONOME_H          = "metronome_hour";
    public static final String METRONOME_D          = "metronome_day";
    public static final String METRONOME_W          = "metronome_week";
    public static final String METRONOME_M          = "metronome_month";
    public static final String DAY_IN_MONTH         = "day_in_month";
    public static final String TYPE_OF_METRONOME    = "type";
    public static final String METRONOME_ID         = "metronomeID";

    public static int prevIDMetronome;

    private OutputCollector collector;

    private long elapsedTime_d;
    private long elapsedTime_w;
    private long elapsedTime_m;

    private int metronomeID = 0;

    public Metronome() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.elapsedTime_d = 0;
        this.elapsedTime_w = 0;
        this.elapsedTime_m = 0;
    }

    @Override
    public void execute(Tuple input) {

        int ID                  = input.getIntegerByField(ParseCSV.F_MSGID);
        String occurredOn       = input.getStringByField(ParseCSV.OCCURRED_ON);
        long occurredOnMillis   = input.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        int dayMonth            = input.getIntegerByField(ParseCSV.DAY_IN_MONTH);


        if(ID > prevIDMetronome){

            prevIDMetronome = ID;

            MILLIS_MONTH = dayMonth * MILLIS_DAY;


            if (this.elapsedTime_d == 0)
                this.elapsedTime_d = TimeUtils.roundToCompletedDay(occurredOnMillis) ;
            if (this.elapsedTime_w == 0)
                this.elapsedTime_w = TimeUtils.lastWeek(occurredOnMillis);
            if (this.elapsedTime_m == 0)
                this.elapsedTime_m = TimeUtils.lastMonth(occurredOnMillis);


            // Metronome sends tick every day
            if (TimeUtils.roundToCompletedDay(occurredOnMillis) - this.elapsedTime_d >= MILLIS_DAY) {
                metronomeID++;
                this.elapsedTime_d = 0;
                Values values = new Values();
                values.add(METRONOME_D);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(metronomeID);
                collector.emit(S_METRONOME, values);
                System.out.println("\u001B[33m" + "SEND METRONOME DAY -" + " ID METRONOME: " + metronomeID + "\u001B[0m");
            }

            // Metronome sends tick every week
            if (TimeUtils.lastWeek(occurredOnMillis) - this.elapsedTime_w >= MILLIS_WEEK) {
                metronomeID++;
                this.elapsedTime_w = 0;
                Values values = new Values();
                values.add(METRONOME_W);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(metronomeID);
                collector.emit(S_METRONOME, values);
                System.out.println("\u001B[33m" + "SEND METRONOME WEEK -" + " ID METRONOME: " + metronomeID + "\u001B[0m");
            }

            // Metronome sends tick every month
            if (TimeUtils.lastMonth(occurredOnMillis) - this.elapsedTime_m >= MILLIS_MONTH) {
                metronomeID++;
                this.elapsedTime_m = 0;
                Values values = new Values();
                values.add(METRONOME_M);
                values.add(dayMonth);
                values.add(occurredOnMillis);
                values.add(occurredOn);
                values.add(metronomeID);
                collector.emit(S_METRONOME, values);
                System.out.println("\u001B[33m" + "SEND METRONOME MONTH -" + " ID METRONOME: " + metronomeID + "\u001B[0m");
            }

            collector.ack(input);
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(TYPE_OF_METRONOME, DAY_IN_MONTH, OCCURREDON_MILLIS, OCCURRED_ON, METRONOME_ID));

    }
}
