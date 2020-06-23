package com.stirperichard.stormbus.query3;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;


public class GlobalRankBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private TopKRanking topKranking;
    private int k;

    public GlobalRankBolt(int k) {
        this.k = k;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(k);
    }

    @Override
    public void execute(Tuple tuple) {
        long tupleTimestamp = tuple.getLongByField(Configuration.TIMESTAMP);
        long currentTimestamp = tuple.getLongByField(Configuration.CURRENNT_TIMESTAMP);
        //String metronomeMsg = tuple.getStringByField(METRONOME_H_STREAM_ID);
        //String articleID = tuple.getStringByField(PARSER_QUERY_1[1]);
        //long estimatedTotal = tuple.getLongByField(ESTIMATED_TOTAL);

        RankingQ3 partialRanking = (RankingQ3) tuple.getValueByField(Configuration.PARTIAL_RANKING);


        /* Update global rank */
        boolean updated = false;
        for (RankItemQ3 item : partialRanking.getRanking()) {
            updated |= topKranking.update(item);
        }

        String output = "";
        /* Emit if the local top10 is changed */
        if (updated) {

            long delay = 0;
            try {
                delay = System.currentTimeMillis() - Long.valueOf(tupleTimestamp);
            } catch (NumberFormatException nfe) {
            }

            List<RankItemQ3> globalTopK = topKranking.getTopK().getRanking();
            output = tupleTimestamp + ", " + currentTimestamp + ", ";

            for (int i = 0; i < globalTopK.size(); i++) {
                RankItemQ3 item = globalTopK.get(i);
                output += item.getBusCompanyName();
                output += ", ";
            }

            if (globalTopK.size() < k) {
                int i = k - globalTopK.size();
                for (int j = 0; j < i; j++) {
                    output += "NULL";
                    output += ", ";
                }
            }

            /* piggyback delay */
            output += String.valueOf(delay);
            System.out.println("\033[0;36m" + tupleTimestamp + ", " + currentTimestamp + ", " + partialRanking.getRanking().toString() + "\u001B[0m");

        }


        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // I don't need to declare fields, cuz this is the final bolt :B
        outputFieldsDeclarer.declare(new Fields("òllll"));
    }


}
