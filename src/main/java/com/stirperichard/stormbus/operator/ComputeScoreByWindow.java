package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.entity.ReasonsCount;
import com.stirperichard.stormbus.utils.Constants;
import com.stirperichard.stormbus.utils.Window;
import com.stirperichard.stormbus.utils.WindowQ3;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.sql.DataSource;
import java.util.*;

public class ComputeScoreByWindow extends BaseRichBolt {


	public static final String S_PULSE = "sPulse";
	public static final String F_MSGID = "MSGID";
	public static final String F_PICKUP_DATATIME = "pickupDatatime";
	public static final String F_DROPOFF_DATATIME = "dropoffDatatime";
	public static final String F_ROUTE = "route";
	public static final String F_COUNT = "count";
	public static final String F_TIMESTAMP = "timestamp";


	private static final int WINDOW_SIZE = 24 * 60;
	private static final double MIN_IN_MS = 60 * 60 * 1000;
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	private long latestCompletedTimeframe;

	Map<String, WindowQ3> windowPerCompany;

	public ComputeScoreByWindow() {

	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		this.collector = outputCollector;
		this.latestCompletedTimeframe = 0;
		this.windowPerCompany = new HashMap<>();

	}

	@Override
	public void execute(Tuple tuple) {

		if (tuple.getSourceStreamId().equals(Metronome.S_METRONOME)) {
			handleMetronomeMessage(tuple);  //sliding window based on event time
		} else {
			handleTaxiReport(tuple);
		}

	}

	private void handleMetronomeMessage(Tuple tuple) {

		String msgId = tuple.getStringByField(DataGenerator.BUS_BREAKDOWN_ID);
		String reason = tuple.getStringByField(DataGenerator.REASON);
		String time = tuple.getStringByField(Metronome.F_TIME);
		String companyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
		String howLongDelayed = tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

		long latestTimeframe = roundToCompletedMinute(time);

		if (this.latestCompletedTimeframe < latestTimeframe) {

			int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MIN_IN_MS));
			List<String> expiredReasons = new ArrayList<>();

			for (String r : windowPerCompany.keySet()) {

				WindowQ3 w = windowPerCompany.get(r);
				if (w == null) {
					continue;
				}

				w.moveForward(elapsedMinutes);
				ReasonsCount rCount = w.getEstimatedTotal();

				/* Reduce memory by removing windows with no data */
				ReasonsCount zero = new ReasonsCount(0, 0, 0);
				//if (w.getEstimatedTotal().equals(zero))
					expiredReasons.add(r);

				Values v = new Values();
				v.add(msgId);
				v.add(r);

				double score = Constants.WT * rCount.getHEAVY_TRAFFIC() +
						Constants.WM * rCount.getMECHANICAL_PROBLEM() + Constants.WO * rCount.getOTHER();

				//v.add(rCount.getHEAVY_TRAFFIC());
				//v.add(rCount.getMECHANICAL_PROBLEM());
				//v.add(rCount.getOTHER());
				v.add(score);

				v.add(time);

				collector.emit(v);
				System.out.println("\033[0;35m" + v + "\u001B[0m");

			}

			/* Reduce memory by removing windows with no data */
			for (String r : expiredReasons) {
				windowPerCompany.remove(r);
			}

			this.latestCompletedTimeframe = latestTimeframe;
			System.out.println("\033[0;32m" + "Tick Tuple" + "\u001B[0m");

		}

		collector.ack(tuple);

	}

	private void handleTaxiReport(Tuple tuple) {

		String msgId = tuple.getStringByField(DataGenerator.BUS_BREAKDOWN_ID);
		String reason = tuple.getStringByField(DataGenerator.REASON);
		String time = tuple.getStringByField(MetronomeQuery3.F_TIME);
		String busCompanyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
		String howLongDelayed = tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

		long latestTimeframe = roundToCompletedMinute(time);

		if (this.latestCompletedTimeframe < latestTimeframe) {

			int elapsedMinutes = (int) Math.ceil((latestTimeframe - latestCompletedTimeframe) / (MIN_IN_MS));
			List<String> expiredReasons = new ArrayList<>();

			for (String r : windowPerCompany.keySet()) {

				WindowQ3 w = windowPerCompany.get(r);
				if (w == null) {
					continue;
				}

				w.moveForward(elapsedMinutes);
				ReasonsCount rCount = w.getEstimatedTotal();

				/* Emit the count of the current route after the update*/
				if (r.equals(busCompanyName))
					continue;

				/* Reduce memory by removing windows with no data */
				ReasonsCount zero = new ReasonsCount(0, 0, 0);
				if (w.getEstimatedTotal().equals(zero))
					expiredReasons.add(r);

				/*
				Values v = new Values();
				v.add(msgId);
				v.add(r);
				v.add(rCount.getHEAVY_TRAFFIC());
				v.add(rCount.getMECHANICAL_PROBLEM());
				v.add(rCount.getOTHER());
				v.add(time);

				collector.emit(v);

				 */

			}

			/* Reduce memory by removing windows with no data */
			for (String r : expiredReasons) {
				windowPerCompany.remove(r);
			}

			this.latestCompletedTimeframe = latestTimeframe;

		}

		/* Time has not moved forward. Update and emit count */
		WindowQ3 w = windowPerCompany.get(busCompanyName);
		if (w == null) {
			w = new WindowQ3(WINDOW_SIZE);
			windowPerCompany.put(busCompanyName, w);
		}

		// Codice bruttissimo, da cambiare
		if(Integer.parseInt(howLongDelayed) <= 30){
			w.increment(addReasonCount(reason));
		}else{
			w.increment(addReasonCount(reason));
			w.increment(addReasonCount(reason));
		}

		/* Retrieve route frequency in the last 30 mins */
		ReasonsCount count = w.getEstimatedTotal();

		/*
		Values values = new Values();
		values.add(msgId);
		values.add(busCompanyName);
		values.add(count.getHEAVY_TRAFFIC());
		values.add(count.getMECHANICAL_PROBLEM());
		values.add(count.getOTHER());
		values.add(time);

		System.out.println("\033[0;33m" + values + "\u001B[0m");
		collector.emit(values);
		collector.ack(tuple);
		*/
	}

	private long roundToCompletedMinute(String timestamp) {

		Date d = new Date(Long.parseLong(timestamp));
		Calendar date = new GregorianCalendar();
		date.setTime(d);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);

		return date.getTime().getTime();
	}


	private ReasonsCount addReasonCount(String reason) {
		ReasonsCount zero = new ReasonsCount(0, 0, 0);

		if (reason.equals("Heavy Traffic")) {
			zero.setHEAVY_TRAFFIC(1);
		} else if (reason.equals("Mechanical Problem")) {
			zero.setMECHANICAL_PROBLEM(1);
		} else
			zero.setOTHER(1);

		return zero;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//outputFieldsDeclarer.declare(new Fields(F_MSGID, F_PICKUP_DATATIME, F_DROPOFF_DATATIME, F_ROUTE, F_COUNT, F_TIMESTAMP));
		outputFieldsDeclarer.declare(new Fields(F_MSGID, F_PICKUP_DATATIME, F_DROPOFF_DATATIME, F_ROUTE, F_COUNT, F_TIMESTAMP));

	}

}