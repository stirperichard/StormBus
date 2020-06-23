package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class CountByWindowQuery1 extends BaseRichBolt {

    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP      	= "timestamp";
    public static final String DROPOFF_DATETIME    	= "dropoff_datetime";

    private static final int  WINDOW_SIZE_1 		= 24; //HOUR IN DAY
    private static final double MILLISECONDS_IN_MINUTE 		= 60 * 1000;
    private static final double MILLISECONDS_IN_HOUR 		= 60 * 60 * 1000;
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    private int day, month, year, hour, minutes, seconds;

    private long latestCompletedTimeframe;

    private SimpleDateFormat sdf;

    Map<String, Window> windowPerRoute;

    /*
     * QUERY 1 :
     *
     *  Calcolare il ritardo medio degli autobus per quartiere nelle ultime 24 ore (di event time),
     *  7 giorni (di event time) e 1 mese (di event time).
     *
     */


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        this.latestCompletedTimeframe = 0;
        this.windowPerRoute = new HashMap<String, Window>();
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
        Long time		 		= tuple.getLongByField(Metronome.F_TIME);
        String occurredOn   	= tuple.getStringByField(Metronome.OCCURRED_ON);
        String timestamp 		= tuple.getStringByField(Metronome.F_TIMESTAMP);

        // long latestTimeframe = TimeUtils.roundToCompletedHour(time);

        if (this.latestCompletedTimeframe < time){

            int elapsedHour = (int) Math.ceil((time - latestCompletedTimeframe) / (MILLISECONDS_IN_HOUR));
            List<String> expiredRoutes = new ArrayList<String>();

            for (String r : windowPerRoute.keySet()){

                Window w = windowPerRoute.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedHour);
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

            this.latestCompletedTimeframe = time;

        }

        collector.ack(tuple);

    }

    private void handleBusData(Tuple tuple){

        String msgId 			= tuple.getStringByField(ParseCSV.F_MSGID);
        String boro 			= tuple.getStringByField(ParseCSV.BORO);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        String howLongDelayed	= tuple.getStringByField(ParseCSV.HOW_LONG_DELAYED);
        String occurredOnMillis 			= tuple.getStringByField(ParseCSV.OCCURRED_ON_MILLIS);
        String timestamp 		= tuple.getStringByField(ParseCSV.F_TIMESTAMP);

        long latestTimeframe = roundToCompletedMinute(occurredOnMillis);

        if (this.latestCompletedTimeframe < latestTimeframe){

            int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MILLISECONDS_IN_HOUR));
            List<String> expiredRoutes = new ArrayList<String>();

            for (String r : windowPerRoute.keySet()){

                Window w = windowPerRoute.get(r);
                if (w == null){
                    continue;
                }

                w.moveForward(elapsedMinutes);
                String rCount = String.valueOf(w.getEstimatedTotal());

                /* Emit the count of the current route after the update*/
                /*if (r.equals(route))
                    continue;

                 */

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
            for (String r : expiredRoutes){
                windowPerRoute.remove(r);
            }

            this.latestCompletedTimeframe = latestTimeframe;

        }

        /* Time has not moved forward. Update and emit count */
        Window w = windowPerRoute.get("route");
        /*
        if (w == null){
            w = new Window(WINDOW_SIZE);
            windowPerRoute.put(route, w);
        }

        w.increment();

         */

        /* Retrieve route frequency in the last 30 mins */
        String count = String.valueOf(w.getEstimatedTotal());

        Values values = new Values();
        values.add(msgId);
        //values.add(pickupDatatime);
        //values.add(dropoffDataTime);
        //values.add(route);
        values.add(count);
        values.add(timestamp);

        collector.emit(values);
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


    private void getInfoDate(String occurredOn) throws ParseException {

        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        day = calendar.get(Calendar.DAY_OF_MONTH);
        month = calendar.get(Calendar.MONTH);
        year = calendar.get(Calendar.YEAR);
        hour = calendar.get(Calendar.HOUR);
        minutes = calendar.get(Calendar.MINUTE);
        seconds = calendar.get(Calendar.SECOND);
    }

}
