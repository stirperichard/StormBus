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

import static com.stirperichard.stormbus.utils.Constants.*;


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

    public static int ID_from_metronome = 0;
    public static int ID_from_parse = 0;


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
        this.map_month = new HashMap<String, Window>();

    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Metronome.S_METRONOME)){

            handleMetronomeMessage(input);

        } else {

            handleBusData(input);
        }
    }

    private void handleMetronomeMessage(Tuple tuple){

        String msgType              = tuple.getSourceStreamId();

        if (msgType.equals(Metronome.S_METRONOME)) {
            Long time		 		= tuple.getLongByField(Metronome.OCCURREDON_MILLIS);
            String occurredOn   	= tuple.getStringByField(Metronome.OCCURRED_ON);
            int dayPerMonth         = tuple.getIntegerByField(Metronome.DAY_IN_MONTH);
            String typeMetronome    = tuple.getStringByField(Metronome.TYPE_OF_METRONOME);
            int metronomeID         = tuple.getIntegerByField(Metronome.METRONOME_ID);

            if (metronomeID > ID_from_metronome) {
                ID_from_metronome = metronomeID;

                System.out.println("METRONOME ID: " + metronomeID + " METRONOME TYPE: " + typeMetronome);

                if (typeMetronome.equals(Metronome.METRONOME_H)) {
                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO HOUR" + "\u001B[0m");
                    long latestTimeframe = TimeUtils.roundToCompletedHour(time);

                    if (this.latestCompletedTimeframeHour < latestTimeframe) {

                        int elapsedHour = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeHour) / (MILLIS_HOUR));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String boro : map_hour.keySet()) {

                            Window w = map_hour.get(boro);
                            if (w == null) {
                                continue;
                            }

                            float delayPerBoroPerHour = w.getEstimatedTotal();

                            w.moveForward(elapsedHour);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values();
                            v.add(occurredOn);
                            v.add(boro);
                            v.add(delayPerBoroPerHour);
                            v.add(time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerHour + " latestCompletedTimeframeHour: "+ latestCompletedTimeframeHour + "]" + "\u001B[0m");

                            collector.emit(HOUR, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_hour.remove(r);
                        }

                        this.latestCompletedTimeframeHour = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(Metronome.METRONOME_D)) {
                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO DAY" + "\u001B[0m");
                    long latestTimeframe = TimeUtils.roundToCompletedDay(time);

                    if (this.latestCompletedTimeframeDay < latestTimeframe) {

                        int elapsedDay = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String boro : map_day.keySet()) {

                            Window w = map_day.get(boro);
                            if (w == null) {
                                continue;
                            }

                            long delayPerBoroPerDay = w.getEstimatedTotal();
                            float avgPerBoroPerDay = delayPerBoroPerDay / 24;  //Sommatoria giornaliera diviso il numero di ore
                            w.moveForward(elapsedDay);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values();
                            v.add(occurredOn);
                            v.add(boro);
                            v.add(avgPerBoroPerDay);
                            v.add(time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerDay + " TIME: " + time + "]" + "\u001B[0m");

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + avgPerBoroPerDay + "]" + "\u001B[0m");

                            collector.emit(DAY, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_day.remove(r);
                        }

                        this.latestCompletedTimeframeDay = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(Metronome.METRONOME_W)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO WEEK" + "\u001B[0m");
                    long latestTimeframe = TimeUtils.lastWeek(time);

                    if (this.latestCompletedTimeframeWeek < latestTimeframe) {

                        int elapsedWeek = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String boro : map_week.keySet()) {

                            Window w = map_week.get(boro);
                            if (w == null) {
                                continue;
                            }

                            long delayPerBoroPerWeek = w.getEstimatedTotal();
                            float avgDelayPerBoroPerWeek = delayPerBoroPerWeek / 7;    //Media settimanale in base giornaliera
                            w.moveForward(elapsedWeek);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values();
                            v.add(occurredOn);
                            v.add(boro);
                            v.add(avgDelayPerBoroPerWeek);
                            v.add(time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerWeek + " TIME: " + time + "]" + "\u001B[0m");

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + avgDelayPerBoroPerWeek + "]" + "\u001B[0m");

                            collector.emit(WEEK, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_week.remove(r);
                        }

                        this.latestCompletedTimeframeWeek = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(Metronome.METRONOME_M)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO MONTH" + "\u001B[0m");

                    long latestTimeframe = TimeUtils.lastMonth(time);

                    if (this.latestCompletedTimeframeMonth < latestTimeframe) {

                        int elapsedMonth = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeMonth) / (MILLIS_HOUR * 24 * dayPerMonth));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String boro : map_month.keySet()) {

                            Window w = map_month.get(boro);
                            if (w == null) {
                                continue;
                            }

                            long delayPerBoroPerMonth = w.getEstimatedTotal();
                            float avgDelayPerBoroPerMonth = delayPerBoroPerMonth / dayPerMonth;   //Media mensile su base giornaliera
                            w.moveForward(elapsedMonth);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values();
                            v.add(occurredOn);
                            v.add(boro);
                            v.add(avgDelayPerBoroPerMonth);
                            v.add(time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerMonth + " TIME: " + time + "]" + "\u001B[0m");

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + avgDelayPerBoroPerMonth + "]" + "\u001B[0m");


                            collector.emit(MONTH, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_month.remove(r);
                        }

                        this.latestCompletedTimeframeMonth = latestTimeframe;
                    }
                }
            }
        }

        collector.ack(tuple);

    }

    private void handleBusData(Tuple tuple){

        String boro 			= tuple.getStringByField(ParseCSV.BORO);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        int howLongDelayed	    = tuple.getIntegerByField(ParseCSV.HOW_LONG_DELAYED);
        long time               = tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        int dayPerMonth         = tuple.getIntegerByField(ParseCSV.DAY_IN_MONTH);
        int msgID               = tuple.getIntegerByField(ParseCSV.F_MSGID);



        if (msgID > ID_from_parse){

            long latestTimeframeHour = TimeUtils.roundToCompletedHour(time);
            long latestTimeframeDay = TimeUtils.roundToCompletedDay(time);
            long latestTimeframeWeek = TimeUtils.lastWeek(time);
            long latestTimeframeMonth = TimeUtils.lastMonth(time);

            if (this.latestCompletedTimeframeHour < latestTimeframeHour){

                int elapsedHour = (int) Math.ceil((latestTimeframeHour - this.latestCompletedTimeframeHour) / (MILLIS_HOUR));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_hour.keySet()){

                    Window w = map_hour.get(r);
                    if (w == null){
                        continue;
                    }

                    long delayPerBoroPerHour = w.getEstimatedTotal();

                    w.moveForward(elapsedHour);

                    // Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(occurredOn);
                    v.add(r);
                    v.add(delayPerBoroPerHour);
                    v.add(time);

                    System.out.println("\u001B[36m" + "[" + "MSG ID: " + msgID + " DATA HOUR" + "]" + "\u001B[0m");
                    System.out.println("\u001B[36m" + "[" + r + "," + delayPerBoroPerHour + "]" + "\u001B[0m");

                    collector.emit(HOUR, v);
                }

                Window wH = map_hour.get(boro);
                if (wH == null) {
                    wH = new Window(WINDOW_SIZE);
                    map_hour.put(boro, wH);
                }
                wH.increment(howLongDelayed);

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes){
                    map_hour.remove(r);
                }

                this.latestCompletedTimeframeHour = latestTimeframeHour;

            } else {
                //HOUR
                /* Time has not moved forward. Update and emit count */
                Window wH = map_hour.get(boro);
                if (wH == null) {
                    wH = new Window(WINDOW_SIZE);
                    map_hour.put(boro, wH);
                }
                wH.increment(howLongDelayed);
            }

            if (this.latestCompletedTimeframeDay < latestTimeframeDay){

                int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_DAY));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_day.keySet()){

                    Window w = map_day.get(r);
                    if (w == null){
                        continue;
                    }

                    long delayPerBoroPerDay = w.getEstimatedTotal();
                    long avgPerBoroPerDay = delayPerBoroPerDay / 24;  //Sommatoria giornaliera diviso il numero di ore

                    w.moveForward(elapsedDay);

                    // Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgPerBoroPerDay);
                    v.add(time);

                    System.out.println("\u001B[36m" + "[" + "DATA DAY" + "]" + "\u001B[0m");
                    System.out.println("\u001B[36m" + "[" + r + "," + avgPerBoroPerDay + "]" + "\u001B[0m");

                    collector.emit(DAY, v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes){
                    map_day.remove(r);
                }

                Window wD = map_day.get(boro);
                if (wD == null) {
                    wD = new Window(WINDOW_SIZE);
                    map_day.put(boro, wD);
                }
                wD.increment(howLongDelayed);

                this.latestCompletedTimeframeDay = latestTimeframeDay;

            } else {
                //DAYS
                /* Time has not moved forward. Update and emit count */
                Window wD = map_day.get(boro);
                if (wD == null) {
                    wD = new Window(WINDOW_SIZE);
                    map_day.put(boro, wD);
                }
                wD.increment(howLongDelayed);
            }

            if (this.latestCompletedTimeframeWeek < latestTimeframeWeek) {

                int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_WEEK));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_week.keySet()) {

                    Window w = map_week.get(r);
                    if (w == null) {
                        continue;
                    }

                    long delayPerBoroPerWeek = w.getEstimatedTotal();
                    long avgDelayPerBoroPerWeek = delayPerBoroPerWeek / 7;    //Media settimanale in base giornaliera
                    w.moveForward(elapsedWeek);

                    //Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgDelayPerBoroPerWeek);
                    v.add(time);

                    System.out.println("\u001B[36m" + "[" + "DATA WEEK" + "]" + "\u001B[0m");
                    System.out.println("\u001B[36m" + "METRONOME ID: " + "[" + r + "," + delayPerBoroPerWeek + " TIME: " + time + "]" + "\u001B[0m");
                    collector.emit(WEEK, v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_week.remove(r);
                }

                Window wW = map_week.get(boro);
                if (wW == null) {
                    wW = new Window(WINDOWS_SIZE_WEEK);
                    map_week.put(boro, wW);
                }
                wW.increment(howLongDelayed);

                this.latestCompletedTimeframeWeek = latestTimeframeWeek;
            } else {
                //WEEK
                /* Time has not moved forward. Update and emit count */
                Window wW = map_week.get(boro);
                if (wW == null) {
                    wW = new Window(WINDOWS_SIZE_WEEK);
                    map_week.put(boro, wW);
                }
                wW.increment(howLongDelayed);
            }

            if (this.latestCompletedTimeframeMonth < latestTimeframeMonth) {

                int elapsedMonth = (int) Math.ceil((latestTimeframeMonth - this.latestCompletedTimeframeMonth) / (MILLIS_DAY * dayPerMonth));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_month.keySet()) {

                    Window w = map_month.get(r);
                    if (w == null) {
                        continue;
                    }

                    long delayPerBoroPerMonth = w.getEstimatedTotal();
                    long avgDelayPerBoroPerMonth = delayPerBoroPerMonth / dayPerMonth;   //Media mensile su base giornaliera
                    w.moveForward(elapsedMonth);

                    // Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values();
                    v.add(occurredOn);
                    v.add(r);
                    v.add(avgDelayPerBoroPerMonth);
                    v.add(time);

                    System.out.println("\u001B[36m" + "[" + "DATA MESE" + "]" + "\u001B[0m");
                    System.out.println("\u001B[36m" + "[" + r + "," + avgDelayPerBoroPerMonth + "]" + "\u001B[0m");

                    collector.emit(MONTH, v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_month.remove(r);
                }

                Window wM = map_month.get(boro);
                if (wM == null) {
                    wM = new Window(dayPerMonth);
                    map_month.put(boro, wM);
                }
                wM.increment(howLongDelayed);

                this.latestCompletedTimeframeMonth = latestTimeframeMonth;

            } else {
                //MONTH
                /* Time has not moved forward. Update and emit count */
                Window wM = map_month.get(boro);
                if (wM == null) {
                    wM = new Window(dayPerMonth);
                    map_month.put(boro, wM);
                }
                wM.increment(howLongDelayed);
            }
        }

        //ACK
        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, AVG_DELAY, TIMESTAMP, F_TIMESTAMP));
    }
}
