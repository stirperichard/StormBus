package com.stirperichard.stormbus.query3;

import com.stirperichard.stormbus.operator.DataGenerator;
import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class PartialRankBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private TopKRanking topKranking;
    private int topK;

    public PartialRankBolt(int k) {
        this.topK = k;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(this.topK);
    }

    @Override
    public void execute(Tuple tuple) {

        String busCompanyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
        double score = tuple.getDoubleByField(Constants.SCORE);
        long timestamp = tuple.getLongByField(Constants.TIMESTAMP);
        long currentTimestamp = tuple.getLongByField(Constants.CURRENNT_TIMESTAMP);

        boolean update = false;
        RankItemQ3 item = new RankItemQ3(busCompanyName, score);
        update = topKranking.update(item);

        if (update) {
            RankingQ3 ranking = topKranking.getTopK();

            Values values = new Values();
            values.add(timestamp);
            values.add(currentTimestamp);
            values.add(busCompanyName);
            values.add(ranking);

            _collector.emit(values);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.TIMESTAMP, Constants.CURRENNT_TIMESTAMP, DataGenerator.BUS_COMPANY_NAME, Constants.PARTIAL_RANKING));
    }
}
