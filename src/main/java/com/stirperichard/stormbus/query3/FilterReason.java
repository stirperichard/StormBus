package com.stirperichard.stormbus.query3;


import com.stirperichard.stormbus.operator.MetronomeQuery3;
import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class FilterReason extends BaseRichBolt {

    private OutputCollector collector;

    public FilterReason() {
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }


    @Override
    public void execute(Tuple tuple) {

        String busBreakdownId = tuple.getStringByField(Constants.BUS_BREAKDOWN_ID);
        String reason = tuple.getStringByField(Constants.REASON);
        String occurredOn = tuple.getStringByField(Constants.OCCURRED_ON);
        String busCompanyName = tuple.getStringByField(Constants.BUS_COMPANY_NAME);
        String howLongDelayed = tuple.getStringByField(Constants.HOW_LONG_DELAYED);

        long currentTimestamp = tuple.getLongByField(Constants.CURRENNT_TIMESTAMP);

        long time = roundToCompletedMinute(occurredOn);


        Values values = new Values();
        values.add(busBreakdownId);
        values.add(computeReason(reason));
        values.add(String.valueOf(time));
        values.add(busCompanyName);
        values.add(howLongDelayed);
        values.add(currentTimestamp);


        collector.emit(values);
        collector.ack(tuple);

    }


    private String computeReason(String reason) {

        if (reason.equals("Heavy Traffic")) {
            return "Heavy Traffic";
        } else if (reason.equals("Mechanical Problem")) {
            return "Mechanical Problem";
        } else
            return "Other Reason";
    }


    private long roundToCompletedMinute(String timestamp) {

        try {
            Date d = Constants.sdf.parse(timestamp);
            Calendar date = new GregorianCalendar();
            date.setTime(d);
            date.set(Calendar.SECOND, 0);
            date.set(Calendar.MILLISECOND, 0);

            return date.getTime().getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return -1;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.BUS_BREAKDOWN_ID, Constants.REASON,
                MetronomeQuery3.F_TIME, Constants.BUS_COMPANY_NAME,
                Constants.HOW_LONG_DELAYED, Constants.CURRENNT_TIMESTAMP));
    }
}