package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

public class GlobalRank extends BaseRichBolt {

    public static final String OUT = "output";

    private RabbitMQManager rabbitmq;

    private boolean USE_RABBIT;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;

    private OutputCollector collector;
    private TopKRanking topKranking;
    private int k;


    public GlobalRank(int k) {
        this.k = k;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.topKranking = new TopKRanking(k);
    }

    @Override
    public void execute(Tuple input) {
        String type 			    = input.getStringByField(PartialRankQ2.TYPE);
        String occurredOn 		    = input.getStringByField(PartialRankQ2.OCCURRED_ON);
        long basetime           	= input.getLongByField(PartialRankQ2.OCCURRED_ON_MILLIS_BASETIME);
        Ranking ranking 	        = (Ranking) input.getValueByField(PartialRankQ2.TOPK);
        String mOA                  = input.getStringByField(PartialRankQ2.MORNING_OR_AFTERNOON);

        boolean updated = false;
        for (RankItem item : ranking.getRanking()) {
            updated |= topKranking.update(item);
        }

        String output = "";
        /* Emit if the local top10 is changed */
        if (updated) {

            List<RankItem> globalTopK = topKranking.getTopK().getRanking();

            for (int i = 0; i < globalTopK.size(); i++) {
                RankItem item = globalTopK.get(i);
                output += item.getReason();
                output += ", ";
            }

            if (globalTopK.size() < k) {
                int i = k - globalTopK.size();
                for (int j = 0; j < i; j++) {
                    output += "NULL";
                    output += ", ";
                }
            }

        }

        collector.ack(input);

        System.out.println("GLOBAL RANK: " + " DAY/WEEK: " + type + " MORNING/AFTERNOON: " + mOA + " BASETIME: " + TimeUtils.retriveDataFromMillis(basetime) + " OUTPUT: " + output);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(OUT));
    }
}
