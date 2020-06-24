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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.*;

public class CountByWindowQuery2 extends BaseRichBolt {


    public static final String F_MSGID = "msgId";
    public static final String OCCURRED_ON = "occurredOn";
    public static final String F_TIMESTAMP = "timestamp";
    public static final String REASON = "reason";
    public static final String TYPE = "type";

    public static final String DAY = "day";
    public static final String WEEK = "week";

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private long latestCompletedTimeframeDay, latestCompletedTimeframeWeek;

    private SimpleDateFormat sdf;

    Map<String, Window> map_day_morning, map_day_afternoon, map_week_morning, map_week_afternoon;

    public static int ID_from_metronomeQ2 = 0;
    public static int ID_from_parseQ2 = 0;


    /*
     * QUERY 2 :
     *
     *  Fornire la classifica delle tre cause di disservizio più frequenti
     * (ad esempio, Heavy Traffic, MechanicalProblem, Flat Tire)
     * nelle due fasce orarie di servizio 5:00-11:59 e 12:00-19:00.
     * Le tre cause sonoordinate dalla più frequente alla meno frequente.
     *
     */

    public CountByWindowQuery2() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.latestCompletedTimeframeDay = 0;
        this.latestCompletedTimeframeWeek = 0;
        this.map_day_afternoon = new HashMap<String, Window>();
        this.map_day_morning = new HashMap<String, Window>();
        this.map_week_morning = new HashMap<String, Window>();
        this.map_week_afternoon = new HashMap<String, Window>();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Metronome.S_METRONOME)) {

            handleMetronomeMessage(input);  //sliding window based on event time

        } else {

            handleBusData(input);

        }
    }

    private void handleMetronomeMessage(Tuple tuple) {

        String msgType = tuple.getSourceStreamId();

        if (msgType.equals(Metronome.S_METRONOME)) {
            Long time = tuple.getLongByField(Metronome.OCCURREDON_MILLIS);
            String occurredOn = tuple.getStringByField(Metronome.OCCURRED_ON);
            String typeMetronome = tuple.getStringByField(Metronome.TYPE_OF_METRONOME);
            int metronomeID = tuple.getIntegerByField(Metronome.METRONOME_ID);

            if (metronomeID > ID_from_metronomeQ2) {
                ID_from_metronomeQ2 = metronomeID;

                if (typeMetronome.equals(Metronome.METRONOME_D)) {
                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO DAY" + " WITH ID: " + metronomeID + "\u001B[0m");
                    long latestTimeframe = TimeUtils.roundToCompletedDay(time);

                    if (this.latestCompletedTimeframeDay < latestTimeframe) {

                        int elapsedDay = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String r : map_day_morning.keySet()) {

                            Window w = map_day_morning.get(r);
                            if (w == null) {
                                continue;
                            }

                            long numberOfDelays = w.getCounter();

                            w.moveForward(elapsedDay);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(r);

                            Values v = new Values(DAY, occurredOn, r, numberOfDelays, time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + r + "," + numberOfDelays + " TIME: " + time + "]" + "\u001B[0m");

                            collector.emit(DAY, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_day_morning.remove(r);
                        }

                        this.latestCompletedTimeframeDay = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(Metronome.METRONOME_W)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO WEEK" + " WITH ID: " + metronomeID + "\u001B[0m");
                    long latestTimeframe = TimeUtils.lastWeek(time);

                    if (this.latestCompletedTimeframeWeek < latestTimeframe) {

                        int elapsedWeek = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
                        List<String> expiredRoutes = new ArrayList<>();

                        for (String r : map_week_morning.keySet()) {

                            Window w = map_week_morning.get(r);
                            if (w == null) {
                                continue;
                            }

                            long numberOfDelays = w.getCounter();

                            w.moveForward(elapsedWeek);

                            /* Reduce memory by removing windows with no data */
                            expiredRoutes.add(r);

                            Values v = new Values(WEEK, occurredOn, r, numberOfDelays, time);

                            System.out.println("\u001B[36m" + "METRONOME ID: " + metronomeID + "   TYPE OF METRONOME: " + typeMetronome + "[" + r + "," + numberOfDelays + " TIME: " + time + "]" + "\u001B[0m");

                            collector.emit(WEEK, v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredRoutes) {
                            map_week_morning.remove(r);
                        }

                        this.latestCompletedTimeframeWeek = latestTimeframe;
                    }
                }
            }
        }

        collector.ack(tuple);
    }

    private void handleBusData(Tuple tuple) {

        String occurredOn = tuple.getStringByField(FilterByTime.OCCURRED_ON);
        long time = tuple.getLongByField(FilterByTime.OCCURRED_ON_MILLIS);
        int msgID = tuple.getIntegerByField(FilterByTime.F_MSGID);
        String reason = tuple.getStringByField(FilterByTime.REASON);
        String type = tuple.getStringByField(FilterByTime.TYPE);


        if (this.latestCompletedTimeframeWeek == 0)
            this.latestCompletedTimeframeWeek = time;
        if(this.latestCompletedTimeframeDay == 0)
            this.latestCompletedTimeframeDay = time;


        if (msgID > ID_from_parseQ2) {

            ID_from_parseQ2 = msgID;
            long latestTimeframeDay = TimeUtils.roundToCompletedDay(time);
            long latestTimeframeWeek = TimeUtils.lastWeek(time);

            if (this.latestCompletedTimeframeDay < latestTimeframeDay) {
                System.out.println("\u001B[36m" + "[" + "MAP DAY MORNING" + map_day_morning + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + "MAP DAY AFTERNOON" + map_week_afternoon + "]" + "\u001B[0m");

                int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_DAY));
                List<String> expiredRoutes = new ArrayList<>();

                if (type.equals(FilterByTime.MORNING)) {
                    for (String r : map_day_morning.keySet()) {

                        Window w = map_day_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        long numberOfDelays = w.getCounter();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredRoutes.add(r);

                        Values v = new Values(DAY, occurredOn, r, numberOfDelays, time);

                        collector.emit(v);
                    }

                } else if (type.equals(FilterByTime.AFTERNOON)) {
                    for (String r : map_week_afternoon.keySet()) {

                        Window w = map_week_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        long numberOfDelays = w.getCounter();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredRoutes.add(r);

                        Values v = new Values(DAY, occurredOn, r, numberOfDelays, time);

                        collector.emit(v);
                    }
                }


                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_week_morning.remove(r);
                    map_week_afternoon.remove(r);
                }

                this.latestCompletedTimeframeDay = latestTimeframeDay;

            }

            if (type.equals(FilterByTime.MORNING)) {
                Window wD = map_day_morning.get(reason);
                System.out.println("\u001B[35m" + "ADDING ON MORNING - REASON:   " + reason + "    FROM MSG ID:  " + msgID + "\u001B[0m"); //PRINT PURPLE
                if (wD == null) {
                    wD = new Window(1);
                    map_day_morning.put(reason, wD);
                }
                wD.increment(1);

            } else if (type.equals(FilterByTime.AFTERNOON)) {
                System.out.println("\u001B[35m" + "ADDING ON AFTERNOON - REASON:   " + reason + "    FROM MSG ID:  " + msgID + "\u001B[0m"); //PRINT PURPLE
                Window wD = map_day_afternoon.get(reason);
                if (wD == null) {
                    wD = new Window(1);
                    map_day_afternoon.put(reason, wD);
                }
                wD.increment(1);

            }


            if (this.latestCompletedTimeframeWeek < latestTimeframeWeek) {
                System.out.println("\u001B[36m" + "[" + "DATA WEEK" + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + map_week_morning + "]" + "\u001B[0m");
                System.out.println("\u001B[36m" + "[" + map_week_afternoon + "]" + "\u001B[0m");

                int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_WEEK));
                List<String> expiredRoutes = new ArrayList<>();

                if (type.equals(FilterByTime.MORNING)) {

                    for (String r : map_week_morning.keySet()) {

                        Window w = map_week_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        long numberOfDelays = w.getCounter();
                        w.moveForward(elapsedWeek);

                        //Reduce memory by removing windows with no data
                        expiredRoutes.add(r);

                        Values v = new Values(WEEK, occurredOn, r, numberOfDelays, time);

                        System.out.println("EVENT OCCURRED AT:" + occurredOn + " REASON: " + r + " NUMBER OF DELAYS PER REASON MORNING WEEK: " + numberOfDelays);

                        collector.emit(v);
                    }

                } else if (type.equals(FilterByTime.AFTERNOON)) {
                    for (String r : map_week_afternoon.keySet()) {

                        Window w = map_week_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        long numberOfDelays = w.getCounter();

                        w.moveForward(elapsedWeek);

                        //Reduce memory by removing windows with no data
                        expiredRoutes.add(r);

                        Values v = new Values(WEEK, occurredOn, r, numberOfDelays, time);

                        System.out.println("EVENT OCCURRED AT:" + occurredOn + " REASON: " + r + " NUMBER OF DELAYS PER REASON AFTERNOON WEEK: " + numberOfDelays);

                        collector.emit(v);
                    }
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredRoutes) {
                    map_week_morning.remove(r);
                    map_week_afternoon.remove(r);
                }

                this.latestCompletedTimeframeWeek = latestTimeframeWeek;

            }
            if (type.equals(FilterByTime.MORNING)) {
                Window wD = map_week_morning.get(reason);
                if (wD == null) {
                    wD = new Window(1);
                    map_week_morning.put(reason, wD);
                }
                wD.increment(1);
            } else if (type.equals(FilterByTime.AFTERNOON)) {
                Window wD = map_week_afternoon.get(reason);
                if (wD == null) {
                    wD = new Window(1);
                    map_week_afternoon.put(reason, wD);
                }
                wD.increment(1);
            }

        }

        //ACK
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TYPE, F_MSGID, OCCURRED_ON, F_TIMESTAMP));
    }

}

