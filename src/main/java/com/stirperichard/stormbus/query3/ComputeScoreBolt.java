package com.stirperichard.stormbus.query3;


import com.stirperichard.stormbus.operator.DataGenerator;
import com.stirperichard.stormbus.operator.MetronomeQuery3;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class ComputeScoreBolt extends BaseRichBolt {
    private OutputCollector _collector;

    public ComputeScoreBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String reason = tuple.getStringByField(DataGenerator.REASON);
        long tupleTimestamp = tuple.getLongByField(Configuration.TIMESTAMP);
        String busCompanyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);

        Values values = new Values();
        values.add(tupleTimestamp);
        //values.add(currentTimestamp);

        _collector.emit(values);
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(MetronomeQuery3.F_TIME, DataGenerator.BUS_COMPANY_NAME,
                Configuration.SCORE));
    }
}
