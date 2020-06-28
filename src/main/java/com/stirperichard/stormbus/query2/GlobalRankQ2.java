package com.stirperichard.stormbus.query2;

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

public class GlobalRankQ2 extends BaseRichBolt {

    private RabbitMQManager rabbitmq;

    private boolean USE_RABBIT;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;

    private OutputCollector collector;
    private TopKRankingQ2 topKrankingQ2;
    private int k;

    public String latest_tuple;
    public long latest_basetime;

    public GlobalRankQ2(int k) {
        this.k = k;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.topKrankingQ2 = new TopKRankingQ2(k);
        this.latest_tuple = null;
        this.latest_basetime = 0;
    }

    @Override
    public void execute(Tuple input) {
        String type 			    = input.getStringByField(TYPE);
        String occurredOn 		    = input.getStringByField(OCCURRED_ON);
        long basetime           	= input.getLongByField(OCCURRED_ON_MILLIS_BASETIME);
        RankingQ2 rankingQ2 = (RankingQ2) input.getValueByField(TOPK);
        String mOA                  = input.getStringByField(MORNING_OR_AFTERNOON);

        boolean updated = false;

        List<RankItemQ2> a;
        a = topKrankingQ2.getTopK().getRanking();
        for (RankItemQ2 item : a) {
            topKrankingQ2.remove(item);
        }

        System.out.println(rankingQ2.getRanking().toString());
        System.out.println(a);

        for (RankItemQ2 item : rankingQ2.getRanking()) {
            updated |= topKrankingQ2.update(item);
            System.out.println(TimeUtils.retriveDataFromMillis(basetime) + " " + topKrankingQ2.toString());
        }

        String output = "";
        /* Emit if the local top3 is changed */
        if (updated) {

            //System.out.println(TimeUtils.retriveDataFromMillis(basetime) + " - " + type + " - " + mOA + topKranking.toString());

            List<RankItemQ2> globalTopK = topKrankingQ2.getTopK().getRanking();

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

        if(this.latest_tuple == null)
            this.latest_tuple = TimeUtils.retriveDataFromMillis(basetime) +  " - "  + output;
        if(latest_basetime == 0)
            this.latest_basetime = basetime;

        String new_tuple = "LATEST TUPLEEEEEEEEEEEEEEEEEEE" + TimeUtils.retriveDataFromMillis(basetime) +  " - "  + output;

        if (latest_basetime < basetime){
            System.out.println(latest_tuple);
            this.latest_basetime = basetime;
        }

        this.latest_tuple = new_tuple;

        System.out.println("GLOBAL RANK: " + " BASETIME: " + TimeUtils.retriveDataFromMillis(basetime) + " DAY/WEEK: " + type + " MORNING/AFTERNOON: " + mOA + " OUTPUT: " + output);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(OUT));
    }
}
