package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.RankItem;
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

public class PartialRankQ2 extends BaseRichBolt {

    public static final String OCCURRED_ON                  = "occurredOn";
    public static final String OCCURRED_ON_MILLIS_BASETIME  = "OccurredOnMillisBasetime";
    public static final String REASON                       = "reason";
    public static final String TYPE                         = "type";
    public static final String TOTAL                        = "total";
    public static final String TOPK                         = "topK";
    public static final String MORNING_OR_AFTERNOON         = "morningOrAfternoon";

    private static final int	NULL = -1;
    private static final long serialVersionUID = 1L;
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
        String type 			    = input.getStringByField(CountByWindowQuery2.TYPE);
        String morningOrAfternoon   = input.getStringByField(CountByWindowQuery2.MORNING_OR_AFTERNOON);
        String occurredOn   	    = input.getStringByField(CountByWindowQuery2.OCCURRED_ON);
        String reason       	    = input.getStringByField(CountByWindowQuery2.REASON);
        int total			        = input.getIntegerByField(CountByWindowQuery2.TOTAL);
        long occurredOnMillis	    = input.getLongByField(CountByWindowQuery2.OCCURRED_ON_MILLIS_BASETIME);

        RankItem item = new RankItem(reason, total, occurredOnMillis);
        boolean updated = ranking.update(item);

        /* Emit if the local top10 is changed */
        if (updated){
            Ranking topK = ranking.getTopK();

            Values values = new Values(type, morningOrAfternoon, occurredOn, occurredOnMillis, topK);

            collector.emit(values);

        }
        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TYPE, MORNING_OR_AFTERNOON, OCCURRED_ON, OCCURRED_ON_MILLIS_BASETIME, TOPK));
    }
}
