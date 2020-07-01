package com.stirperichard.stormbus.query3;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class GlobalRankBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private KafkaProducer<String, String> producer;
    private TopKRanking topKranking;
    private int k;
    private boolean USE_KAFKA;
    private String kafkaTopic;
    private long timestamp;
    private String old_tuple;


    public GlobalRankBolt(boolean USE_KAFKA, int k, String kafkaTopic) {
        this.USE_KAFKA = USE_KAFKA;
        this.k = k;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(k);

        this.timestamp = 0;
        this.old_tuple = "";

        if (this.USE_KAFKA) {
            Properties props = new Properties();
            props.put("bootstrap.servers", Configuration.BOOTSTRAP_SERVERS);
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);

            producer = new KafkaProducer<String, String>(props);
        }
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

        }


        String new_tuple = "\033[0;36m" + tupleTimestamp + ", " + currentTimestamp + ", " + partialRanking.getRanking().toString();

        if(old_tuple.isEmpty()){
            old_tuple = new_tuple;
        }

        if(timestamp == 0){
            timestamp = tupleTimestamp;
        }

        if(timestamp < tupleTimestamp){
            System.out.println(old_tuple);
            timestamp = tupleTimestamp;
        }

        old_tuple = new_tuple;
        //System.out.println(new_tuple);


        if (updated)
            createOutputResponse(currentTimestamp, tupleTimestamp);


        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // I don't need to declare fields, cuz this is the final bolt :B
        outputFieldsDeclarer.declare(new Fields("field"));
    }


    private void createOutputResponse(long currentTimestamp, long tupleTimestamp) {
        String result = "";
        List<RankItemQ3> globalRanking = this.topKranking.getTopK().getRanking();

        result.concat(String.valueOf(tupleTimestamp)).concat(", ");

        for (RankItemQ3 rankItemQ3 : globalRanking)
            result.concat(rankItemQ3.getBusCompanyName())
                    .concat(", ").concat(String.valueOf(rankItemQ3.getScore()));

        producer.send(new ProducerRecord<>(this.kafkaTopic, result));
    }



}
