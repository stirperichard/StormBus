package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class CountByWindowQuery3 extends BaseRichBolt {

	public static final String S_PULSE 				= "sPulse";
	public static final String F_MSGID				= "MSGID";
	public static final String F_PICKUP_DATATIME	= "pickupDatatime";
	public static final String F_DROPOFF_DATATIME	= "dropoffDatatime";
	public static final String F_ROUTE 				= "route";
	public static final String F_COUNT 				= "count";
	public static final String F_TIMESTAMP 			= "timestamp";
	
	
    /* 
     * DEBS 2015 GC: 
     *
     * The goal of the query is to find the top 10 most frequent routes 
     * during the last 30 minutes. A route is represented by a starting 
     * grid cell and an ending grid cell. All routes completed within 
     * the last 30 minutes are considered for the query. The output query
     * results must be updated whenever any of the 10 most frequent 
     * routes changes. 
     *  
     */

	private static final int  WINDOW_SIZE 		= 24 * 60 * 60;
	private static final double MIN_IN_MS 		= 60 * 1000; 
	private static final long serialVersionUID = 1L;	
	private OutputCollector collector;

	private long latestCompletedTimeframe;
	
	Map<String, Window> windowPerBusCompany;
	
	public CountByWindowQuery3(){

	}
	
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; 
        this.latestCompletedTimeframe = 0;
        this.windowPerBusCompany = new HashMap<String, Window>();
        
    }
    
    @Override
	public void execute(Tuple tuple) {
    	
    	if (tuple.getSourceStreamId().equals(Metronome.S_METRONOME)){
    		handleMetronomeMessage(tuple);  //sliding window based on event time
    	} else {
    		handleTaxiReport(tuple);
    	}
    
    }
    
    private void handleMetronomeMessage(Tuple tuple){
    
		String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
		String reason 			= tuple.getStringByField(DataGenerator.REASON);
		String time		 		= tuple.getStringByField(MetronomeQuery3.F_TIME);
		String busCompanyName 	= tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
		String howLongDelayed 	= tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

		long latestTimeframe = roundToCompletedMinute(time);
		
		if (this.latestCompletedTimeframe < latestTimeframe){
			
			int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MIN_IN_MS));
			List<String> expiredRoutes = new ArrayList<>();
			
			for (String r : windowPerBusCompany.keySet()){
				
				Window w = windowPerBusCompany.get(r);
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
				v.add(reason);
				v.add(time);
				v.add(r);	 
				v.add(rCount);  	
				v.add(busCompanyName);
				v.add(howLongDelayed);

				collector.emit(v);
			}
			
			/* Reduce memory by removing windows with no data */
			for (String r : expiredRoutes){
				windowPerBusCompany.remove(r);
			}
			
			this.latestCompletedTimeframe = latestTimeframe;
			
		} 
		
		collector.ack(tuple);			

    }
    
    private void handleTaxiReport(Tuple tuple){
    	
		String msgId 			= tuple.getStringByField(Metronome.F_MSGID);
		String reason 			= tuple.getStringByField(DataGenerator.REASON);
		String time		 		= tuple.getStringByField(MetronomeQuery3.F_TIME);
		String boro				= tuple.getStringByField(DataGenerator.BORO);
		String busCompanyName 	= tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
		String howLongDelayed 		= tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

		long latestTimeframe = roundToCompletedMinute(time);
		
		if (this.latestCompletedTimeframe < latestTimeframe){
			
			int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MIN_IN_MS));
			List<String> expiredRoutes = new ArrayList<>();
			
			for (String r : windowPerBusCompany.keySet()){
				
				Window w = windowPerBusCompany.get(r);
				if (w == null){
					continue;
				}
				
				w.moveForward(elapsedMinutes);
				String rCount = String.valueOf(w.getEstimatedTotal());
				
				/* Emit the count of the current route after the update*/
				if (r.equals(reason))
					continue;

				/* Reduce memory by removing windows with no data */
				if (w.getEstimatedTotal() == 0)
					expiredRoutes.add(r);

				Values v = new Values();
				v.add(msgId);
				v.add(r);
				v.add(rCount);
				v.add(time);
				v.add(busCompanyName);
				v.add(howLongDelayed);

				collector.emit(v);
				
			}

			/* Reduce memory by removing windows with no data */
			for (String r : expiredRoutes){
				windowPerBusCompany.remove(r);
			}
			
			this.latestCompletedTimeframe = latestTimeframe;
			
		} 
			
		/* Time has not moved forward. Update and emit count */
		Window w = windowPerBusCompany.get(busCompanyName);
		if (w == null){
			w = new Window(WINDOW_SIZE);
			windowPerBusCompany.put(busCompanyName, w);
		}
		
		w.increment();
		
		/* Retrieve route frequency in the last 30 mins */
		String count = String.valueOf(w.getEstimatedTotal());

		Values values = new Values();
		values.add(msgId);
		values.add(reason);
		values.add(count);  	
		values.add(time);
		values.add(busCompanyName);
		values.add(howLongDelayed);
		
		collector.emit(values);
		collector.ack(tuple);			

    }
    
	private long roundToCompletedMinute(String timestamp) {

		Date d = new Date(Long.parseLong(timestamp));
		Calendar date = new GregorianCalendar();
		date.setTime(d);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);

		return date.getTime().getTime();
	
	}

    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	
        outputFieldsDeclarer.declare(new Fields(F_MSGID, DataGenerator.REASON, F_COUNT,
				DataGenerator.OCCURRED_ON, DataGenerator.BUS_COMPANY_NAME,
				DataGenerator.HOW_LONG_DELAYED));
    
    }

}