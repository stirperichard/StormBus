package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.RabbitMQManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class GlobalRank extends BaseRichBolt {

    private RabbitMQManager rabbitmq;

    private boolean USE_RABBIT;
    private String rabbitMqHost;
    private String rabbitMqUsername;
    private String rabbitMqPassword;
    private OutputCollector collector;


    public GlobalRank(String rabbitMqHost, String rabbitMqUsername, String rabbitMqPassword) {
        super();
        this.rabbitMqHost = rabbitMqHost;
        this.rabbitMqUsername = rabbitMqUsername;
        this.rabbitMqPassword = rabbitMqPassword;
        this.USE_RABBIT = true;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String msgId 			    = input.getStringByField(CountByWindowQuery1.F_MSGID);
        String occurredOn 		    = input.getStringByField(CountByWindowQuery1.OCCURRED_ON);
        int avgDelay           	    = input.getIntegerByField(CountByWindowQuery1.AVG_DELAY);
        long timestamp 	            = input.getLongByField(CountByWindowQuery1.TIMESTAMP);
        String boro                 = input.getStringByField(CountByWindowQuery1.BORO);

        String output = "TIMESTAMP:" + timestamp + "   BORO:" + boro + "    AVG:" + avgDelay;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
