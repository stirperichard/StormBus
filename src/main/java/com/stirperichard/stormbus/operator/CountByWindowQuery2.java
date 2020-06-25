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

    private OutputCollector collector;

    private long latestCompletedTimeframeDay, latestCompletedTimeframeWeek,
            latestCompletedTimeframeDayMorning, latestCompletedTimeframeDayAfternoon,
            latestCompletedTimeframeWeekMorning, latestCompletedTimeframeWeekAfternoon;

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
        if (input.getSourceStreamId().equals(S_METRONOME)) {

            //handleMetronomeMessage(input);  //sliding window based on event time

        } else {

            handleBusData(input);
        }
    }

    private void handleMetronomeMessage(Tuple tuple) {

        String msgType = tuple.getSourceStreamId();

        if (msgType.equals(S_METRONOME)) {
            Long time = tuple.getLongByField(OCCURREDON_MILLIS);
            String occurredOn = tuple.getStringByField(OCCURRED_ON);
            String typeMetronome = tuple.getStringByField(TYPE_OF_METRONOME);
            int metronomeID = tuple.getIntegerByField(METRONOME_ID);

            if (metronomeID > ID_from_metronomeQ2) {
                ID_from_metronomeQ2 = metronomeID;

                if (typeMetronome.equals(METRONOME_D)) {
                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO DAY" + " WITH ID: " + metronomeID + "\u001B[0m");
                    long latestTimeframe = TimeUtils.roundToCompletedDay(time);

                    if (this.latestCompletedTimeframeDayMorning < latestTimeframe && this.latestCompletedTimeframeDayAfternoon < latestTimeframe) {

                        int elapsedDay = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeDay) / (MILLIS_HOUR * 24));
                        List<String> expiredReasons = new ArrayList<>();

                        for (String r : map_day_morning.keySet()) {

                            Window w = map_day_morning.get(r);
                            if (w == null) {
                                continue;
                            }

                            int numberOfDelays = w.getEstimatedTotal();

                            w.moveForward(elapsedDay);

                            /* Reduce memory by removing windows with no data */
                            expiredReasons.add(r);

                            Values v = new Values(DAY, MORNING, occurredOn, r, numberOfDelays, latestTimeframe);

                            collector.emit(v);
                        }

                        for (String r : map_day_afternoon.keySet()) {

                            Window w = map_day_afternoon.get(r);
                            if (w == null) {
                                continue;
                            }

                            int numberOfDelays = w.getEstimatedTotal();

                            w.moveForward(elapsedDay);

                            /* Reduce memory by removing windows with no data */
                            expiredReasons.add(r);

                            Values v = new Values(DAY, AFTERNOON, occurredOn, r, numberOfDelays, latestTimeframe);

                            collector.emit(v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredReasons) {
                            map_day_morning.remove(r);
                            map_day_afternoon.remove(r);
                        }

                        this.latestCompletedTimeframeDay = latestTimeframe;
                    }
                }

                if (typeMetronome.equals(METRONOME_W)) {

                    System.out.println("\u001B[33m" + "RICEVUTO METRONOMO WEEK" + " WITH ID: " + metronomeID + "\u001B[0m");
                    long latestTimeframe = TimeUtils.lastWeek(time);

                    if (this.latestCompletedTimeframeWeek < latestTimeframe) {

                        int elapsedWeek = (int) Math.ceil((latestTimeframe - this.latestCompletedTimeframeWeek) / (MILLIS_HOUR * 24 * 7));
                        List<String> expiredReasons = new ArrayList<>();

                        for (String r : map_week_morning.keySet()) {

                            Window w = map_week_morning.get(r);
                            if (w == null) {
                                continue;
                            }

                            int numberOfDelays = w.getEstimatedTotal();

                            w.moveForward(elapsedWeek);

                            /* Reduce memory by removing windows with no data */
                            expiredReasons.add(r);

                            Values v = new Values(WEEK, MORNING, occurredOn, r, numberOfDelays, latestTimeframe);

                            collector.emit(v);
                        }

                        for (String r : map_week_afternoon.keySet()) {

                            Window w = map_week_afternoon.get(r);
                            if (w == null) {
                                continue;
                            }

                            int numberOfDelays = w.getEstimatedTotal();

                            w.moveForward(elapsedWeek);

                            /* Reduce memory by removing windows with no data */
                            expiredReasons.add(r);

                            Values v = new Values(WEEK, AFTERNOON, occurredOn, r, numberOfDelays, latestTimeframe);

                            collector.emit(v);
                        }

                        /* Reduce memory by removing windows with no data */
                        for (String r : expiredReasons) {
                            map_week_morning.remove(r);
                            map_week_afternoon.remove(r);
                        }

                        this.latestCompletedTimeframeWeek = latestTimeframe;
                    }
                }
            }
        }

        collector.ack(tuple);
    }

    private void handleBusData(Tuple tuple) {

        String occurredOn = tuple.getStringByField(OCCURRED_ON);
        long time = tuple.getLongByField(OCCURREDON_MILLIS);
        int msgID = tuple.getIntegerByField(F_MSGID);
        String reason = tuple.getStringByField(REASON);
        String type = tuple.getStringByField(TYPE);



        if (this.latestCompletedTimeframeWeekMorning == 0)
            this.latestCompletedTimeframeWeekMorning = TimeUtils.lastWeek(time);
        if (this.latestCompletedTimeframeWeekAfternoon == 0)
            this.latestCompletedTimeframeWeekAfternoon = TimeUtils.lastWeek(time);

        if (this.latestCompletedTimeframeDayMorning == 0)
            this.latestCompletedTimeframeDayMorning = TimeUtils.roundToCompletedDay(time);
        if (this.latestCompletedTimeframeDayAfternoon == 0)
            this.latestCompletedTimeframeDayAfternoon = TimeUtils.roundToCompletedDay(time);


        if (msgID > ID_from_parseQ2) {

            ID_from_parseQ2 = msgID;
            long latestTimeframeDay = TimeUtils.roundToCompletedDay(time);
            long latestTimeframeWeek = TimeUtils.lastWeek(time);

            if (type.equals(MORNING)) {

                if (this.latestCompletedTimeframeDayMorning < latestTimeframeDay) {

                    int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDayMorning) / (MILLIS_DAY));
                    List<String> expiredReasons = new ArrayList<>();

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeDayMorning) + "[" + "MAP DAY MORNING" + map_day_morning + "]" + "\u001B[0m");

                    for (String r : map_day_morning.keySet()) {

                        Window w = map_day_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(DAY, MORNING, occurredOn, r, numberOfDelays, latestCompletedTimeframeDayMorning);

                        collector.emit(v);
                    }

                    // Reduce memory by removing windows with no data
                    for (String r : expiredReasons) {
                        map_day_morning.remove(r);
                    }

                    this.latestCompletedTimeframeDayMorning = latestTimeframeDay;

                }

                Window wDM = map_day_morning.get(reason);
                if (wDM == null) {
                    wDM = new Window(1);
                    map_day_morning.put(reason, wDM);
                }
                wDM.increment(1);

                if (this.latestCompletedTimeframeWeekMorning < latestTimeframeWeek) {

                    int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeekMorning) / (MILLIS_WEEK));
                    List<String> expiredReasons = new ArrayList<>();

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeWeekMorning) + "[" + "MAP WEEK MORNING " + map_week_morning + "]" + "\u001B[0m");

                    for (String r : map_week_morning.keySet()) {

                        Window w = map_week_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();
                        w.moveForward(elapsedWeek);

                        //Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(WEEK, MORNING, occurredOn, r, numberOfDelays, latestCompletedTimeframeWeekMorning);

                        collector.emit(v);
                    }
                    // Reduce memory by removing windows with no data
                    for (String r : expiredReasons) {
                        map_week_morning.remove(r);
                    }

                    this.latestCompletedTimeframeWeekMorning = latestTimeframeWeek;

                }

                Window wWM = map_week_morning.get(reason);
                if (wWM == null) {
                    wWM = new Window(1);
                    map_week_morning.put(reason, wWM);
                }
                wWM.increment(1);

            } else if (type.equals(AFTERNOON)) {

                if (this.latestCompletedTimeframeDayAfternoon < latestTimeframeDay) {

                    int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDayAfternoon) / (MILLIS_DAY));
                    List<String> expiredReasons = new ArrayList<>();

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeDayAfternoon) + "[" + "MAP DAY AFTERNOON" + map_day_afternoon + "]" + "\u001B[0m");

                    for (String r : map_day_afternoon.keySet()) {

                        Window w = map_day_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(DAY, AFTERNOON, occurredOn, r, numberOfDelays, latestCompletedTimeframeDayAfternoon);

                        collector.emit(v);
                    }
                    // Reduce memory by removing windows with no data
                    for (String r : expiredReasons) {
                        map_day_afternoon.remove(r);
                    }

                    this.latestCompletedTimeframeDayAfternoon = latestTimeframeDay;
                }

                Window wDA = map_day_afternoon.get(reason);
                if (wDA == null) {
                    wDA = new Window(1);
                    map_day_afternoon.put(reason, wDA);
                }
                wDA.increment(1);


                if (this.latestCompletedTimeframeWeekAfternoon < latestTimeframeWeek) {

                    int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeekAfternoon) / (MILLIS_WEEK));
                    List<String> expiredReasons = new ArrayList<>();

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeWeekAfternoon) + "[" + "MAP WEEK AFTERNOON" + map_week_afternoon + "]" + "\u001B[0m");

                    for (String r : map_week_afternoon.keySet()) {

                        Window w = map_week_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedWeek);

                        // Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(WEEK, AFTERNOON, occurredOn, r, numberOfDelays, latestCompletedTimeframeWeekAfternoon);

                        collector.emit(v);
                    }
                    // Reduce memory by removing windows with no data
                    for (String r : expiredReasons) {
                        map_week_afternoon.remove(r);
                    }

                    this.latestCompletedTimeframeWeekAfternoon = latestTimeframeWeek;
                }


                Window wWA = map_week_afternoon.get(reason);
                if (wWA == null) {
                    wWA = new Window(1);
                    map_week_afternoon.put(reason, wWA);
                }
                wWA.increment(1);

            }

            //ACK
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields(TYPE, MORNING_OR_AFTERNOON, OCCURRED_ON, REASON, TOTAL, OCCURRED_ON_MILLIS_BASETIME));
    }

}

