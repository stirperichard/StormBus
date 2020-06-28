package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.RankItemQ2;
import com.stirperichard.stormbus.utils.Ranking;
import com.stirperichard.stormbus.utils.TopKRanking;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.*;

public class PartialRankQ2 extends BaseRichBolt {

    private OutputCollector collector;
    private TopKRanking ranking;
    private int topK;

    public PartialRankQ2(int topk){
        this.topK = topk;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.ranking = new TopKRanking(topK);
    }

    @Override
    public void execute(Tuple input) {
        String type 			            = input.getStringByField(TYPE);
        String morningOrAfternoon           = input.getStringByField(MORNING_OR_AFTERNOON);
        String occurredOn   	            = input.getStringByField(OCCURRED_ON);
        String reason       	            = input.getStringByField(REASON);
        int total			                = input.getIntegerByField(TOTAL);
        long occurredOnMillisBasetime	    = input.getLongByField(OCCURRED_ON_MILLIS_BASETIME);
        long occurredOnMillis               = input.getLongByField(OCCURREDON_MILLIS);

        RankItemQ2 item = new RankItemQ2(reason, total, occurredOnMillis);
        boolean updated = ranking.update(item);

        /* Emit if the local top3 is changed */
        if (updated){
            Ranking topK = ranking.getTopK();
            Values values = new Values(type, morningOrAfternoon, occurredOn, occurredOnMillisBasetime, topK);

            collector.emit(values);

        }
        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TYPE, MORNING_OR_AFTERNOON, OCCURRED_ON, OCCURRED_ON_MILLIS_BASETIME, TOPK));
    }
}
