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

import static com.stirperichard.stormbus.utils.Constants.*;

public class GlobalRank extends BaseRichBolt {

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
        String type 			    = input.getStringByField(TYPE);
        String occurredOn 		    = input.getStringByField(OCCURRED_ON);
        long basetime           	= input.getLongByField(OCCURRED_ON_MILLIS_BASETIME);
        Ranking ranking 	        = (Ranking) input.getValueByField(TOPK);
        String mOA                  = input.getStringByField(MORNING_OR_AFTERNOON);

        boolean updated = false;

        List<RankItemQ2> a;
        a = topKranking.getTopK().getRanking();
        for (RankItemQ2 item : a) {
            topKranking.remove(item);
        }

        System.out.println(ranking.getRanking().toString());
        System.out.println(a);

        for (RankItemQ2 item : ranking.getRanking()) {
            updated |= topKranking.update(item);
            System.out.println(TimeUtils.retriveDataFromMillis(basetime) + " " + topKranking.toString());
        }

        String output = "";
        /* Emit if the local top3 is changed */
        if (updated) {

            //System.out.println(TimeUtils.retriveDataFromMillis(basetime) + " - " + type + " - " + mOA + topKranking.toString());

            List<RankItemQ2> globalTopK = topKranking.getTopK().getRanking();

            //System.out.println("\u001B[33m" + TimeUtils.retriveDataFromMillis(basetime) + " - " + type + " - " + mOA + "\u001B[0m");

            for (int i = 0; i < globalTopK.size(); i++) {
                RankItemQ2 item = globalTopK.get(i);
                output += item.getReason();
                //System.out.println("\u001B[33m" + item.toString() + "\u001B[0m");
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
