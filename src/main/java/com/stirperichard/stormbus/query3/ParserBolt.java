package com.stirperichard.stormbus.query3;

import com.stirperichard.stormbus.entity.BusRide;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.util.Map;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String rawdata = tuple.getStringByField(Configuration.RAW_DATA);
        long currentTimestamp = tuple.getLongByField(Configuration.CURRENNT_TIMESTAMP);

        try {
            BusRide br = BusRide.parse(rawdata);

            Values values = new Values(br.busbreakdownID, br.reason, br.occurredOn, br.boro, br.busCompanyName, br.howLongDelayed, currentTimestamp);
            _collector.emit(Configuration.PARSER_STREAM_ID, values);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Configuration.PARSER_STREAM_ID,
                new Fields(Configuration.BUS_BREAKDOWN_ID, Configuration.REASON,
                        Configuration.OCCURRED_ON,Configuration.BORO, Configuration.BUS_COMPANY_NAME,
                        Configuration.HOW_LONG_DELAYED, Configuration.CURRENNT_TIMESTAMP));
    }

}
