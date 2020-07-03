package com.stirperichard.stormbus.query3;

import com.stirperichard.stormbus.entity.BusRide;
import com.stirperichard.stormbus.utils.Constants;
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
        String rawdata = tuple.getStringByField(Constants.RAW_DATA);
        long currentTimestamp = tuple.getLongByField(Constants.CURRENNT_TIMESTAMP);

        try {
            BusRide br = BusRide.parse(rawdata);

            Values values = new Values(br.busbreakdownID, br.reason, br.occurredOn, br.boro, br.busCompanyName, br.howLongDelayed, currentTimestamp);
            _collector.emit(Constants.PARSER_STREAM_ID, values);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.PARSER_STREAM_ID,
                new Fields(Constants.BUS_BREAKDOWN_ID, Constants.REASON,
                        Constants.OCCURRED_ON,Constants.BORO, Constants.BUS_COMPANY_NAME,
                        Constants.HOW_LONG_DELAYED, Constants.CURRENNT_TIMESTAMP));
    }

}
