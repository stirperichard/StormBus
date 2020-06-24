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

    public static final String OCCURRED_ON                  = "occurredOn";
    public static final String OCCURRED_ON_MILLIS_BASETIME  = "OccurredOnMillisBasetime";
    public static final String REASON                       = "reason";
    public static final String TYPE                         = "type";
    public static final String TOTAL                        = "total";

    public static final String DAY                          = "day";
    public static final String WEEK                         = "week";
    public static final String MORNING_OR_AFTERNOON         = "morningOrAfternoon";

    private static final long serialVersionUID              = 1L;
    public static final String MORNING = "morning";
    public static final String AFTERNOON = "afternoon";

    private OutputCollector collector;

    private long latestCompletedTimeframeDay, latestCompletedTimeframeWeek;

    private SimpleDateFormat sdf;

    Map<String, Window> map_day_morning, map_day_afternoon, map_week_morning, map_week_afternoon;

    public static int ID_from_metronomeQ2       = 0;
    public static int ID_from_parseQ2           = 0;


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

            //handleMetronomeMessage(input);  //sliding window based on event time

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

                if (typeMetronome.equals(Metronome.METRONOME_W)) {

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

        String occurredOn = tuple.getStringByField(FilterByTimeQ2.OCCURRED_ON);
        long time = tuple.getLongByField(FilterByTimeQ2.OCCURRED_ON_MILLIS);
        int msgID = tuple.getIntegerByField(FilterByTimeQ2.F_MSGID);
        String reason = tuple.getStringByField(FilterByTimeQ2.REASON);
        String type = tuple.getStringByField(FilterByTimeQ2.TYPE);


        if (this.latestCompletedTimeframeWeek == 0)
            this.latestCompletedTimeframeWeek = TimeUtils.lastWeek(time);
        if(this.latestCompletedTimeframeDay == 0)
            this.latestCompletedTimeframeDay = TimeUtils.roundToCompletedDay(time);


        if (msgID > ID_from_parseQ2) {

            ID_from_parseQ2 = msgID;
            long latestTimeframeDay = TimeUtils.roundToCompletedDay(time);
            long latestTimeframeWeek = TimeUtils.lastWeek(time);

            if (this.latestCompletedTimeframeDay < latestTimeframeDay) {

                int elapsedDay = (int) Math.ceil((latestTimeframeDay - this.latestCompletedTimeframeDay) / (MILLIS_DAY));
                List<String> expiredReasons = new ArrayList<>();

                if (type.equals(FilterByTimeQ2.MORNING)) {

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeDay) + "[" + "MAP DAY MORNING" + map_day_morning + "]" + "\u001B[0m");

                    for (String r : map_day_morning.keySet()) {

                        Window w = map_day_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(DAY, MORNING, occurredOn, r, numberOfDelays, latestCompletedTimeframeDay);

                        collector.emit(v);
                    }

                } else if (type.equals(FilterByTimeQ2.AFTERNOON)) {

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeDay) + "[" + "MAP DAY AFTERNOON" + map_week_afternoon + "]" + "\u001B[0m");

                    for (String r : map_week_afternoon.keySet()) {

                        Window w = map_week_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedDay);

                        // Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(DAY, AFTERNOON, occurredOn, r, numberOfDelays, latestCompletedTimeframeDay);

                        collector.emit(v);
                    }
                }


                // Reduce memory by removing windows with no data
                for (String r : expiredReasons) {
                    map_week_morning.remove(r);
                    map_week_afternoon.remove(r);
                }

                this.latestCompletedTimeframeDay = latestTimeframeDay;

            }

            if (type.equals(FilterByTimeQ2.MORNING)) {
                Window wDM = map_day_morning.get(reason);
                if (wDM == null) {
                    wDM = new Window(1);
                    map_day_morning.put(reason, wDM);
                }
                wDM.increment(1);

            } else if (type.equals(FilterByTimeQ2.AFTERNOON)) {
                Window wDA = map_day_afternoon.get(reason);
                if (wDA == null) {
                    wDA = new Window(1);
                    map_day_afternoon.put(reason, wDA);
                }
                wDA.increment(1);

            }


            if (this.latestCompletedTimeframeWeek < latestTimeframeWeek) {

                int elapsedWeek = (int) Math.ceil((latestTimeframeWeek - this.latestCompletedTimeframeWeek) / (MILLIS_WEEK));
                List<String> expiredReasons = new ArrayList<>();

                if (type.equals(FilterByTimeQ2.MORNING)) {

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeWeek) + "[" + "MAP WEEK MORNING " + map_week_morning + "]" + "\u001B[0m");


                    for (String r : map_week_morning.keySet()) {

                        Window w = map_week_morning.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();
                        w.moveForward(elapsedWeek);

                        //Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(WEEK, MORNING, occurredOn, r, numberOfDelays, latestCompletedTimeframeWeek);

                        collector.emit(v);
                    }

                } else if (type.equals(FilterByTimeQ2.AFTERNOON)) {

                    System.out.println("\u001B[36m" + "BASETIME: " + TimeUtils.retriveDataFromMillis(latestCompletedTimeframeWeek) + "[" + "MAP WEEK AFTERNOON " + map_week_afternoon + "]" + "\u001B[0m");

                    for (String r : map_week_afternoon.keySet()) {

                        Window w = map_week_afternoon.get(r);
                        if (w == null) {
                            continue;
                        }

                        int numberOfDelays = w.getEstimatedTotal();

                        w.moveForward(elapsedWeek);

                        //Reduce memory by removing windows with no data
                        expiredReasons.add(r);

                        Values v = new Values(WEEK, AFTERNOON, occurredOn, r, numberOfDelays, latestCompletedTimeframeWeek);

                        collector.emit(v);
                    }
                }

                // Reduce memory by removing windows with no data
                for (String r : expiredReasons) {
                    map_week_morning.remove(r);
                    map_week_afternoon.remove(r);
                }

                this.latestCompletedTimeframeWeek = latestTimeframeWeek;

            }
            if (type.equals(FilterByTimeQ2.MORNING)) {
                Window wWM = map_week_morning.get(reason);
                if (wWM == null) {
                    wWM = new Window(1);
                    map_week_morning.put(reason, wWM);
                }
                wWM.increment(1);
            } else if (type.equals(FilterByTimeQ2.AFTERNOON)) {
                Window wWA = map_week_afternoon.get(reason);
                if (wWA == null) {
                    wWA = new Window(1);
                    map_week_afternoon.put(reason, wWA);
                }
                wWA.increment(1);
            }

        }

        //ACK
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TYPE, MORNING_OR_AFTERNOON, OCCURRED_ON, REASON, TOTAL, OCCURRED_ON_MILLIS_BASETIME));
    }

}

