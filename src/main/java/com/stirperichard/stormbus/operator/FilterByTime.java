package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.TimeUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class FilterByTime extends BaseRichBolt {

    public static final String F_MSGID				= "MSGID";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String REASON           	= "reason";
    public static final String F_TIMESTAMP 	        = "timestamp";
    public static final String OCCURRED_ON_MILLIS   = "occurred_on_millis";
    public static final String DAY_IN_MONTH         = "day_in_month";
    public static final String MORNING             = "morning";
    public static final String AFTERNOON            = "afternoon";


    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public FilterByTime() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple tuple) {
        String msgId 			= tuple.getStringByField(ParseCSV.F_MSGID);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        String reason           = tuple.getStringByField(ParseCSV.REASON);
        long occurredOnMillis	= tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);
        long timestamp 		    = tuple.getLongByField(ParseCSV.F_TIMESTAMP);
        int dayPerMonth         = tuple.getIntegerByField(ParseCSV.DAY_IN_MONTH);

        int hour = 0;

        try {
            hour = TimeUtils.getInfoDateHour(occurredOn);
        } catch (ParseException e) {
            e.printStackTrace();
            collector.ack(tuple);
            return;
        }

        if (hour < 5 || hour > 19){
            collector.ack(tuple);
            return;

        } else if (hour >= 5 && hour < 12) {
            Values values = new Values();
            values.add(msgId);
            values.add(occurredOn);
            values.add(reason);
            values.add(occurredOnMillis);
            values.add(dayPerMonth);
            values.add(timestamp);

            collector.emit(MORNING, values);
            collector.ack(tuple);

        } else {
            Values values = new Values();
            values.add(msgId);
            values.add(occurredOn);
            values.add(reason);
            values.add(occurredOnMillis);
            values.add(dayPerMonth);
            values.add(timestamp);

            collector.emit(AFTERNOON, values);
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, REASON, OCCURRED_ON_MILLIS, DAY_IN_MONTH, F_TIMESTAMP));

    }
}
