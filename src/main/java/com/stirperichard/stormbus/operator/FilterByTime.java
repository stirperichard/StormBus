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
    public static final String MORNING              = "morning";
    public static final String AFTERNOON            = "afternoon";
    public static final String TYPE                 = "type";


    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public static int filterIDMsg = 0;


    public FilterByTime() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple tuple) {
        int msgId 			    = tuple.getIntegerByField(ParseCSV.F_MSGID);
        String occurredOn   	= tuple.getStringByField(ParseCSV.OCCURRED_ON);
        String reason           = tuple.getStringByField(ParseCSV.REASON);
        long occurredOnMillis	= tuple.getLongByField(ParseCSV.OCCURRED_ON_MILLIS);

        if(filterIDMsg < msgId) {

            filterIDMsg = msgId;
            int hour;

            try {
                hour = TimeUtils.getInfoDateHour(occurredOn);
            } catch (ParseException e) {
                e.printStackTrace();
                collector.ack(tuple);
                return;
            }

            if (hour < 5 || hour > 19) {
                collector.ack(tuple);
                return;

            } else if ((hour < 12) && (hour >= 5)) {
                Values values = new Values(msgId, occurredOn, reason, occurredOnMillis, MORNING);

                collector.emit(values);
                collector.ack(tuple);

            } else {
                Values values = new Values(msgId, occurredOn, reason, occurredOnMillis, AFTERNOON);

                collector.emit(values);
                collector.ack(tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, REASON, OCCURRED_ON_MILLIS, TYPE));

    }
}
