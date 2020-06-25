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

    private OutputCollector collector;

    private long latestCompletedTimeframeHour, latestCompletedTimeframeDay, latestCompletedTimeframeWeek, latestCompletedTimeframeMonth;

    Map<String, Window> map_hour, map_day, map_week, map_month;

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

        this.collector = outputCollector;
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
        if (input.getSourceStreamId().equals(S_METRONOME)) {

            handleMetronomeMessage(input);

        } else {

            handleBusData(input);
        }
    }

    private void handleMetronomeMessage(Tuple tuple) {

        String msgType = tuple.getSourceStreamId();

        if (msgType.equals(S_METRONOME)) {
            Long time = tuple.getLongByField(OCCURREDON_MILLIS);
            String occurredOn = tuple.getStringByField(OCCURRED_ON);
            int dayPerMonth = tuple.getIntegerByField(DAY_IN_MONTH);
            String typeMetronome = tuple.getStringByField(TYPE_OF_METRONOME);
            int metronomeID = tuple.getIntegerByField(METRONOME_ID);

            if (metronomeID > ID_from_metronome) {
                ID_from_metronome = metronomeID;

                if (typeMetronome.equals(METRONOME_D)) {
                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO DAY" + " WITH ID: " + metronomeID + "\u001B[0m");
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
                            int numberOfDelays = w.getCounter();
                            float avgPerBoroPerDay = delayPerBoroPerDay / numberOfDelays;  //Sommatoria giornaliera diviso il numero di eventi nelle 24 ore
                            w.moveForward(elapsedDay);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values(DAY, occurredOn, boro, avgPerBoroPerDay, latestCompletedTimeframeDay);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerDay + " TIME BASE: " + latestCompletedTimeframeDay + "]" + "\u001B[0m");

                            collector.emit(v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_day.remove(r);
                        }

                        this.latestCompletedTimeframeDay = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(METRONOME_W)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO WEEK" + " WITH ID: " + metronomeID + "\u001B[0m");
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
                            int numberOfDelays = w.getCounter();
                            float avgDelayPerBoroPerWeek = delayPerBoroPerWeek / numberOfDelays;    //Media settimanale in base eventi
                            w.moveForward(elapsedWeek);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values(WEEK, occurredOn, boro, avgDelayPerBoroPerWeek, latestCompletedTimeframeWeek);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerWeek + " TIME BASE: " + latestCompletedTimeframeWeek + "]" + "\u001B[0m");

                            collector.emit(v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_week.remove(r);
                        }

                        this.latestCompletedTimeframeWeek = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(METRONOME_M)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO MONTH" + " WITH ID: " + metronomeID + "\u001B[0m");

                    long latestTimeframe = TimeUtils.lastMonth(time);

                    if (this.latestCompletedTimeframeMonth < latestTimeframe) {

                        int elapsedMonth = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeMonth) / (MILLIS_HOUR * 24 * dayPerMonth));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String boro : map_month.keySet()) {

                            Window w = map_month.get(boro);
                            if (w == null) {
                                continue;
                            }

                            int numberOfDelays = w.getCounter();

                            long delayPerBoroPerMonth = w.getEstimatedTotal();
                            float avgDelayPerBoroPerMonth = delayPerBoroPerMonth / numberOfDelays;   //Media mensile su base giornaliera
                            w.moveForward(elapsedMonth);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(boro);

                            Values v = new Values(MONTH, occurredOn, boro, avgDelayPerBoroPerMonth, latestCompletedTimeframeMonth);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + boro + "," + delayPerBoroPerMonth + " TIME BASE: " + latestCompletedTimeframeMonth + "]" + "\u001B[0m");

                            collector.emit(v);
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

    private void handleBusData(Tuple tuple) {

        String boro = tuple.getStringByField(BORO);
        String occurredOn = tuple.getStringByField(OCCURRED_ON);
        int howLongDelayed = tuple.getIntegerByField(HOW_LONG_DELAYED);
        long time = tuple.getLongByField(OCCURREDON_MILLIS);
        int dayPerMonth = tuple.getIntegerByField(DAY_IN_MONTH);
        int msgID = tuple.getIntegerByField(F_MSGID);

        if (boro.isEmpty()) {
            collector.ack(tuple);
            return;
        }

        if (msgID > ID_from_parse) {

            ID_from_parse = msgID;

            long latestTimeframeDay = TimeUtils.roundToCompletedDay(time);
            long latestTimeframeWeek = TimeUtils.lastWeek(time);
            long latestTimeframeMonth = TimeUtils.lastMonth(time);

            if (this.latestCompletedTimeframeDay < latestTimeframeDay) {
                System.out.println("\u001B[36m" + "[" + "DATA DAY" + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + map_day + "]" + "\u001B[0m");

                int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_DAY));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_day.keySet()) {

                    Window w = map_day.get(r);
                    if (w == null) {
                        continue;
                    }

                    long delayPerBoroPerDay = w.getEstimatedTotal();
                    int numberOfDelays = w.getCounter();
                    long avgPerBoroPerDay = delayPerBoroPerDay / numberOfDelays;  //Sommatoria giornaliera diviso il numero di eventi nelle 24 ore

                    w.moveForward(elapsedDay);

                    // Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values(DAY, occurredOn, r, avgPerBoroPerDay, time);

                    System.out.println("EVENT OCCURRED AT:" + occurredOn + " BORO: " + r + " AVG BORO PER DAY: " + avgPerBoroPerDay);

                    collector.emit(v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_day.remove(r);
                }


                this.latestCompletedTimeframeDay = latestTimeframeDay;

            }

            //DAYS
            /* Time has not moved forward. Update and emit count */
            Window wD = map_day.get(boro);
            if (wD == null) {
                wD = new Window(1);
                map_day.put(boro, wD);
            }
            wD.increment(howLongDelayed);


            if (this.latestCompletedTimeframeWeek < latestTimeframeWeek) {
                System.out.println("\u001B[36m" + "[" + "DATA WEEK" + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + map_week + "]" + "\u001B[0m");

                int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_WEEK));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_week.keySet()) {

                    Window w = map_week.get(r);
                    if (w == null) {
                        continue;
                    }

                    long delayPerBoroPerWeek = w.getEstimatedTotal();
                    int numberOfDelays = w.getCounter();
                    long avgDelayPerBoroPerWeek = delayPerBoroPerWeek / numberOfDelays;    //Media settimanale in base eventi
                    w.moveForward(elapsedWeek);

                    //Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values(WEEK, occurredOn, r, avgDelayPerBoroPerWeek, time);

                    collector.emit(v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_week.remove(r);
                }

                this.latestCompletedTimeframeWeek = latestTimeframeWeek;

            }

            //WEEK
            /* Time has not moved forward. Update and emit count */
            Window wW = map_week.get(boro);
            if (wW == null) {
                wW = new Window(1);
                map_week.put(boro, wW);
            }
            wW.increment(howLongDelayed);


            if (this.latestCompletedTimeframeMonth < latestTimeframeMonth) {
                System.out.println("\u001B[36m" + "[" + "DATA MESE" + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + map_month + "]" + "\u001B[0m");

                int elapsedMonth = (int) Math.ceil((latestTimeframeMonth - this.latestCompletedTimeframeMonth) / (MILLIS_DAY * dayPerMonth));
                List<String> expiredRoutes = new ArrayList<>();

                for (String r : map_month.keySet()) {

                    Window w = map_month.get(r);
                    if (w == null) {
                        continue;
                    }

                    long delayPerBoroPerMonth = w.getEstimatedTotal();
                    int numberOfDelays = w.getCounter();
                    long avgDelayPerBoroPerMonth = delayPerBoroPerMonth / numberOfDelays;   //Media mensile su base eventi
                    w.moveForward(elapsedMonth);

                    // Reduce memory by removing windows with no data
                    expiredRoutes.add(r);

                    Values v = new Values(MONTH, occurredOn, r, avgDelayPerBoroPerMonth, time);

                    collector.emit(v);
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_month.remove(r);
                }


                this.latestCompletedTimeframeMonth = latestTimeframeMonth;

            }

            //MONTH
            /* Time has not moved forward. Update and emit count */
            Window wM = map_month.get(boro);
            if (wM == null) {
                wM = new Window(1);
                map_month.put(boro, wM);
            }
            wM.increment(howLongDelayed);

        }

        //ACK
        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields(TYPE, F_MSGID, OCCURRED_ON, AVG_DELAY, OCCURREDON_MILLIS, F_TIMESTAMP));
    }
}
