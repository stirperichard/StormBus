package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;


public class CountByWindow extends BaseRichBolt {

    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_DATETIME    	= "dropoff_datetime";

    private static final int  WINDOW_SIZE_1 		= 24; //HOUR IN DAY
    private static final int  WINDOW_SIZE_2		    = 24*7; // HOUR IN WEEK
    private static final int  WINDOW_SIZE_3 		= 24*30; // HOUR IN MONTH
    private static final double MILLISECONDS_IN_MINUTE 		= 60 * 1000;
    private static final double MILLISECONDS_IN_HOUR 		= 60 * 60 * 1000;
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private long latestCompletedTimeframe;

    Map<String, Window> windowPerRoute;

    /*
     * QUERY 1 :
     *
     *  Calcolare il ritardo medio degli autobus per quartiere nelle ultime 24 ore (di event time),
     *  7 giorni (dievent time) e 1 mese (di event time).
     *
     */


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.latestCompletedTimeframe = 0;
        this.windowPerRoute = new HashMap<String, Window>();

    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Metronome.S_METRONOME)){

            handleMetronomeMessage(input);  //sliding window based on event time

        } else {

            handleTaxiReport(input);

        }
    }

    private void handleMetronomeMessage(Tuple tuple){

        String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
        String time		 		= tuple.getStringByField(Metronome.F_TIME);
        String occurredOn   	= tuple.getStringByField(Metronome.OCCURRED_ON);
        String timestamp 		= tuple.getStringByField(Metronome.F_TIMESTAMP);

        long latestTimeframe = roundToCompletedMinute(time);

        if (this.latestCompletedTimeframe < latestTimeframe){

            int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MILLISECONDS_IN_MINUTE));
            List<String> expiredRoutes = new ArrayList<String>();

            for (String r : windowPerRoute.keySet()){

                Window w = windowPerRoute.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Reduce memory by removing windows with no data */
                if (w.getEstimatedTotal() == 0)
                    expiredRoutes.add(r);

                Values v = new Values();
                v.add(msgId);
                v.add(occurredOn);
                v.add(r);
                v.add(rCount);
                v.add(timestamp);
                collector.emit(v);
            }

            /* Reduce memory by removing windows with no data */
            for (String r : expiredRoutes){
                windowPerRoute.remove(r);
            }

            this.latestCompletedTimeframe = latestTimeframe;

        }

        collector.ack(tuple);

    }


    private long roundToCompletedMinute(String timestamp) {

        Date d = new Date(Long.valueOf(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
