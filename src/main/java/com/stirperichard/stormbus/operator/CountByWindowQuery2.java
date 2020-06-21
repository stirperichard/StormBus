package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.TimeUtils;
import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.javatuples.Pair;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.MILLIS_HOUR;

public class CountByWindowQuery2 extends BaseRichBolt {

    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_DATETIME    	= "dropoff_datetime";

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private int day, month, year, hour, minutes, seconds;

    private long latestCompletedTimeframe;

    private SimpleDateFormat sdf;


    Map<Pair<String, Integer>, Window> map;


    /*
     * QUERY 2 :
     *
     *  Fornire la classifica delle tre cause di disservizio pi`u frequenti
     * (ad esempio, Heavy Traffic, MechanicalProblem, Flat Tire)
     * nelle due fasce orarie di servizio 5:00-11:59 e 12:00-19:00.
     * Le tre cause sonoordinate dalla pi`u frequente alla meno frequente.
     *
     */


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.latestCompletedTimeframe = 0;
        this.map = new HashMap<Pair<String, Integer>, Window>();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

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

        String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
        Long time		 		= tuple.getLongByField(Metronome.OCCURREDON_MILLIS);
        String timestamp 		= tuple.getStringByField(Metronome.F_TIMESTAMP);

        long latestTimeframe = TimeUtils.roundToCompletedHour(time);

        if (this.latestCompletedTimeframe < latestTimeframe){

            int elapsedHour = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MILLIS_HOUR));
            List<Pair<String, Integer>> expiredRoutes = new ArrayList<>();

            for (Pair<String, Integer> r : map.keySet()){

                Window w = map.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedHour);
                long rCount = w.getEstimatedTotal();

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredRoutes.add(r);

                Values v = new Values();
                v.add(r);
                v.add(rCount);
                v.add(time);
                v.add(timestamp);
                collector.emit(v);
            }

            /* Reduce memory by removing windows with no data */
            for (Pair<String, Integer> r : expiredRoutes){
                map.remove(r);
            }

            this.latestCompletedTimeframe = latestTimeframe;

        }

        collector.ack(tuple);

    }

    private void handleBusData(Tuple tuple){

        String msgId 			= tuple.getStringByField(ParseCSV.F_MSGID);
        String boro 			= tuple.getStringByField(ParseCSV.BORO);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        int howLongDelayed	    = tuple.getIntegerByField(ParseCSV.HOW_LONG_DELAYED);
        long occurredOnMillis   = tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        String timestamp 		= tuple.getStringByField(ParseCSV.F_TIMESTAMP);

        long latestTimeframe = TimeUtils.roundToCompletedHour(occurredOnMillis);

        Pair<String, Integer> pair = Pair.with(boro, howLongDelayed);

        if (this.latestCompletedTimeframe < latestTimeframe){

            int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MILLIS_HOUR));
            List<Pair<String, Integer>> expiredRoutes = new ArrayList<>();

            for (Pair<String, Integer> r : map.keySet()){

                Window w = map.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                int rCount = w.getEstimatedTotal();


                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(boro);
                v.add(occurredOn);
                v.add(howLongDelayed);
                v.add(r);
                v.add(rCount);
                v.add(timestamp);

                collector.emit(v);

            }

            /* Reduce memory by removing windows with no data */
            for (Pair<String, Integer> r : expiredRoutes){
                map.remove(r);
            }

            this.latestCompletedTimeframe = latestTimeframe;

        }


        /* Time has not moved forward. Update and emit count */
        Window w = map.get(msgId);
        if (w == null){
            w = new Window(24);
            map.put(pair, w);
        }

        w.increment();

        /* Retrieve route frequency in the last 24 hours */
        long delayPerBoro = w.computeTotal();

        Values values = new Values();
        values.add(msgId);
        values.add(boro);
        values.add(delayPerBoro);
        values.add(timestamp);

        collector.emit(values);
        collector.ack(tuple);

    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
