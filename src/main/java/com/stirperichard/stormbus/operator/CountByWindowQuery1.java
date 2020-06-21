package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.TimeUtils;
import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.MILLIS_HOUR;


public class CountByWindowQuery1 extends BaseRichBolt {

    public static final String F_MSGID				= "msgId";
    public static final String TIMESTAMP            = "timestamp";  //OccurredOn in millis
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp_real";
    public static final String AVG_DELAY            = "avg_delay";
    public static final String DAY_PER_MONTH        = "day_per_month";

    public static final String HOUR             = "hour";
    public static final String DAY              = "day";
    public static final String WEEK             = "week";
    public static final String MONTH            = "month";

    private static final long serialVersionUID  = 1L;
    private OutputCollector collector;
    private static final int WINDOW_SIZE 		= 24;  //hour
    private static final int WINDOWS_SIZE_WEEK  = 7;

    private long latestCompletedTimeframeHour, latestCompletedTimeframeDay,
            latestCompletedTimeframeWeek, latestCompletedTimeframeMonth;

    Map<String, Window> map_hour;
    Map<String, Window> map_day;
    Map<String, Window> map_week;
    Map<String, Window> map_month;


    /*
     * QUERY 1 :
     *
     *  Calcolare il ritardo medio degli autobus per quartiere nelle ultime 24 ore (di event time),
     *  7 giorni (di event time) e 1 mese (di event time).
     *
     *  IL CONTWGGIO AVVIENE ORA PER ORA
     *
     */

    public CountByWindowQuery1() {

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.latestCompletedTimeframeHour = 0;
        this.latestCompletedTimeframeMonth = 0;
        this.latestCompletedTimeframeDay = 0;
        this.latestCompletedTimeframeWeek = 0;
        this.map_hour = new HashMap<String, Window>();
        this.map_day = new HashMap<String, Window>();
        this.map_week = new HashMap<String, Window>();
        this.map_day = new HashMap<String, Window>();

    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Metronome.S_METRONOME)){

            handleMetronomeMessage(input);  //sliding window based on event time

        } else {

            handleBusData(input);
        }
    }

    private void handleMetronomeMessage(Tuple tuple){
        String msgType          = tuple.getSourceStreamId();
        String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
        Long time		 		= tuple.getLongByField(Metronome.OCCURREDON_MILLIS);
        long timestamp 		    = tuple.getLongByField(Metronome.F_TIMESTAMP);
        String occurredOn   	= tuple.getStringByField(Metronome.OCCURRED_ON);
        int dayPerMonth         = tuple.getIntegerByField(Metronome.DAY_IN_MONTH);

        if (msgType.equals(Metronome.METRONOME_H)) {

            long latestTimeframe = TimeUtils.roundToCompletedHour(time);

            if (this.latestCompletedTimeframeHour < latestTimeframe) {

                int elapsedHour = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeHour) / (MILLIS_HOUR));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_hour.keySet()) {

                    Window w = map_hour.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedHour);
                    long delayPerBoroPerHour = w.getEstimatedTotal();

                    /* Reduce memory by removing windows with no data */
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(delayPerBoroPerHour);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(HOUR, v);
                }

                /* Reduce memory by removing windows with no data */
                for (String r : expiredRoutes) {
                    map_hour.remove(r);
                }

                this.latestCompletedTimeframeHour = latestTimeframe;
            }
        }

        if (msgType.equals(Metronome.METRONOME_D)) {

            long latestTimeframe = TimeUtils.roundToCompletedDay(time);

            if (this.latestCompletedTimeframeDay < latestTimeframe) {

                int elapsedDay = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_day.keySet()) {

                    Window w = map_day.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedDay);
                    long delayPerBoroPerDay = w.getEstimatedTotal();
                    long avgPerBoroPerDay = delayPerBoroPerDay/24;  //Sommatoria giornaliera diviso il numero di ore

                    /* Reduce memory by removing windows with no data */
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgPerBoroPerDay);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(DAY, v);
                }

                /* Reduce memory by removing windows with no data */
                for (String r : expiredRoutes) {
                    map_day.remove(r);
                }

                this.latestCompletedTimeframeDay = latestTimeframe;
            }
        }

        if (msgType.equals(Metronome.METRONOME_W)) {

            long latestTimeframe = TimeUtils.lastWeek(time);

            if (this.latestCompletedTimeframeWeek < latestTimeframe) {

                int elapsedWeek = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_week.keySet()) {

                    Window w = map_week.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedWeek);
                    long delayPerBoroPerWeek = w.getEstimatedTotal();
                    long avgDelayPerBoroPerWeek = delayPerBoroPerWeek / 7;    //Media settimanale in base giornaliera

                    /* Reduce memory by removing windows with no data */
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgDelayPerBoroPerWeek);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(WEEK, v);
                }

                /* Reduce memory by removing windows with no data */
                for (String r : expiredRoutes) {
                    map_week.remove(r);
                }

                this.latestCompletedTimeframeWeek = latestTimeframe;
            }
        }

        if (msgType.equals(Metronome.METRONOME_M)) {

            long latestTimeframe = TimeUtils.lastMonth(time);

            if (this.latestCompletedTimeframeMonth < latestTimeframe) {

                int elapsedMonth = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeMonth) / (MILLIS_HOUR * 24 * dayPerMonth));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_month.keySet()) {

                    Window w = map_month.get(r);
                    if (w == null) {
                        continue;
                    }

                    w.moveForward(elapsedMonth);
                    long delayPerBoroPerMonth = w.getEstimatedTotal();
                    long avgDelayPerBoroPerMonth = delayPerBoroPerMonth / dayPerMonth;   //Media mensile su base giornaliera

                    /* Reduce memory by removing windows with no data */
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(msgId);
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgDelayPerBoroPerMonth);
                    v.add(time);
                    v.add(timestamp);
                    collector.emit(MONTH, v);
                }

                /* Reduce memory by removing windows with no data */
                for (String r : expiredRoutes) {
                    map_month.remove(r);
                }

                this.latestCompletedTimeframeMonth = latestTimeframe;
            }
        }

        collector.ack(tuple);

    }

    private void handleBusData(Tuple tuple){

        String msgId 			= tuple.getStringByField(ParseCSV.F_MSGID);
        String boro 			= tuple.getStringByField(ParseCSV.BORO);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        int howLongDelayed	    = tuple.getIntegerByField(ParseCSV.HOW_LONG_DELAYED);
        long occurredOnMillis   = tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        long timestamp 		    = tuple.getLongByField(ParseCSV.F_TIMESTAMP);
        int dayPerMonth         = tuple.getIntegerByField(ParseCSV.DAY_IN_MONTH);

        long latestTimeframeHour    = TimeUtils.roundToCompletedHour(occurredOnMillis);
        long latestTimeframeDay     = TimeUtils.roundToCompletedDay(occurredOnMillis);
        long latestTimeframeWeek    = TimeUtils.lastWeek(occurredOnMillis);
        long latestTimeframeMonth   = TimeUtils.lastMonth(occurredOnMillis);

        if(this.latestCompletedTimeframeHour == 0){
            this.latestCompletedTimeframeHour = TimeUtils.roundToCompletedHour(occurredOnMillis);
        }
        if(this.latestCompletedTimeframeDay == 0){
            this.latestCompletedTimeframeDay = TimeUtils.roundToCompletedHour(occurredOnMillis);
        }
        if(this.latestCompletedTimeframeWeek == 0){
            this.latestCompletedTimeframeWeek = TimeUtils.lastWeek(occurredOnMillis);
        }
        if(this.latestCompletedTimeframeMonth == 0){
            this.latestCompletedTimeframeMonth = TimeUtils.lastMonth(occurredOnMillis);
        }


        if (latestTimeframeHour > this.latestCompletedTimeframeHour) {     //Forse conviene mettere ==
            int elapsedHour = (int) Math.ceil((latestTimeframeHour - latestCompletedTimeframeHour) / (MILLIS_HOUR));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_hour.keySet()) {

                Window w = map_hour.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedHour);
                long delayPerBoroPerHour = w.getEstimatedTotal();

                /* Reduce memory by removing windows with no data */
                expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(occurredOn);
                v.add(r);
                v.add(delayPerBoroPerHour);
                v.add(occurredOnMillis);
                v.add(timestamp);
                collector.emit(HOUR, v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes) {
                map_hour.remove(r);
            }

            this.latestCompletedTimeframeHour = latestTimeframeHour;

        }

        if (latestTimeframeDay >= this.latestCompletedTimeframeDay) {
            int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_day.keySet()) {

                Window w = map_day.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedDay);
                long delayPerBoroPerDay = w.getEstimatedTotal();
                long avgPerBoroPerDay = delayPerBoroPerDay/24;  //Sommatoria giornaliera diviso il numero di ore

                /* Reduce memory by removing windows with no data */
                expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(occurredOn);
                v.add(r);
                v.add(avgPerBoroPerDay);
                v.add(occurredOnMillis);
                v.add(timestamp);
                collector.emit(DAY, v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes) {
                map_day.remove(r);
            }

            this.latestCompletedTimeframeDay = latestTimeframeDay;

        }

        if (latestTimeframeWeek >= this.latestCompletedTimeframeWeek) {
            int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_week.keySet()) {

                Window w = map_week.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedWeek);
                long delayPerBoroPerWeek = w.getEstimatedTotal();
                long avgDelayPerBoroPerWeek = delayPerBoroPerWeek / 7;    //Media settimanale in base giornaliera

                /* Reduce memory by removing windows with no data */
                expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(occurredOn);
                v.add(r);
                v.add(avgDelayPerBoroPerWeek);
                v.add(occurredOnMillis);
                v.add(timestamp);
                collector.emit(WEEK, v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes) {
                map_week.remove(r);
            }

            this.latestCompletedTimeframeWeek = latestTimeframeWeek;
        }

        if (latestTimeframeMonth >= this.latestCompletedTimeframeMonth) {
            int elapsedMonth = (int) Math.ceil((latestTimeframeMonth - this.latestCompletedTimeframeMonth) / (MILLIS_HOUR * 24 * dayPerMonth));
            List<String> expiredRoutes = new ArrayList<>();

            for (String r : map_month.keySet()) {

                Window w = map_month.get(r);
                if (w == null) {
                    continue;
                }

                w.moveForward(elapsedMonth);
                long delayPerBoroPerMonth = w.getEstimatedTotal();
                long avgDelayPerBoroPerMonth = delayPerBoroPerMonth / dayPerMonth;   //Media mensile su base giornaliera

                /* Reduce memory by removing windows with no data */
                expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(occurredOn);
                v.add(r);
                v.add(avgDelayPerBoroPerMonth);
                v.add(occurredOnMillis);
                v.add(timestamp);
                collector.emit(MONTH, v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes) {
                map_month.remove(r);
            }

            this.latestCompletedTimeframeMonth = latestTimeframeMonth;
        }


        //HOUR
        /* Time has not moved forward. Update and emit count */
        Window wH = map_hour.get(boro);
        if (wH == null) {
            wH = new Window(WINDOW_SIZE);
            map_hour.put(boro, wH);
        }
        wH.increment(howLongDelayed);
        this.latestCompletedTimeframeHour = latestTimeframeHour;


        //DAYS
        /* Time has not moved forward. Update and emit count */
        Window wD = map_day.get(boro);
        if (wD == null) {
            wD = new Window(WINDOW_SIZE);
            map_day.put(boro, wD);
        }
        wD.increment(howLongDelayed);
        this.latestCompletedTimeframeDay = latestTimeframeDay;


        //WEEK
        /* Time has not moved forward. Update and emit count */
        Window wW = map_week.get(boro);
        if (wW == null) {
            wW = new Window(WINDOWS_SIZE_WEEK);
            map_week.put(boro, wW);
        }
        wW.increment(howLongDelayed);
        this.latestCompletedTimeframeWeek = latestTimeframeWeek;


        //MONTH
        /* Time has not moved forward. Update and emit count */
        Window wM = map_month.get(boro);
        if (wM == null) {
            wM = new Window(dayPerMonth);
            map_month.put(boro, wM);
        }
        wM.increment(howLongDelayed);
        this.latestCompletedTimeframeMonth = latestTimeframeMonth;


        //ACK
        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, AVG_DELAY, TIMESTAMP, F_TIMESTAMP));

    }
}
