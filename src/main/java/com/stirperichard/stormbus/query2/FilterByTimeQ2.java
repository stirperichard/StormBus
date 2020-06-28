package com.stirperichard.stormbus.query2;

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

import static com.stirperichard.stormbus.utils.Constants.*;

public class FilterByTimeQ2 extends BaseRichBolt {

    private OutputCollector collector;
    private SimpleDateFormat sdf;

    public static int filterIDMsg = 0;


    public FilterByTimeQ2() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void execute(Tuple tuple) {
        int msgId 			    = tuple.getIntegerByField(F_MSGID);
        String occurredOn   	= tuple.getStringByField(OCCURRED_ON);
        String reason           = tuple.getStringByField(REASON);
        long occurredOnMillis	= tuple.getLongByField(OCCURREDON_MILLIS);

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
        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, REASON, OCCURREDON_MILLIS, TYPE));

    }
}
