package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.enums.Reason;
import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ParseCSVQuery2 extends BaseRichBolt {


    public static final String F_MSGID				= "MSGID";
    public static final String REASON           	= "reason";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String BUS_COMPANY_NAME    	= "busCompanyName";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";

    private OutputCollector collector;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String rawData 	= input.getStringByField(RedisSpout.F_DATA);
        String msgId 	= input.getStringByField(RedisSpout.F_MSGID);
        //String timestamp = input.getStringByField(RedisSpout.F_TIMESTAMP);

        /* Do NOT emit if the EOF has been reached */
        if (rawData == null || rawData.equals(Constants.REDIS_EOF)){
            collector.ack(input);
            return;
        }

        /* Do NOT emit if the EOF has been reached */
        String[] data = rawData.split(";", -1);
        if (data == null || data.length != 21){
            collector.ack(input);
            return;
        }

        Values values = new Values();

        values.add(msgId); //Aggiungo il MessageID

        if (!data[5].isEmpty()){
            //Aggiungo la Reason
            values.add(mappingReason(data[6]));
        }

        //Controllo la validità e aggiungo il valore della varibile Occurred_On (campo 7)
        if (!data[7].isEmpty()){
            values.add(data[7]);
        }

        collector.emit(values);
        collector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, REASON, OCCURRED_ON));
    }

    public static Reason mappingReason(String reason) {
        if (reason.equals("Accident")) {
            return Reason.ACCIDENT;
        } else if (reason.equals("Delayed by School")) {
            return Reason.DELAYED_BY_SCHOOL;
        } else if (reason.equals("Flat Tire")) {
            return Reason.FLAT_TIRE;
        } else if (reason.equals("Heavy Traffic")) {
            return Reason.HEAVY_TRAFFIC;
        } else if (reason.equals("Mechanical Problem")) {
            return Reason.MECHANICAL_PROBLEM;
        } else if (reason.equals("Problem Run")) {
            return Reason.PROBLEM_RUN;
        } else if (reason.equals("Weather Condition")) {
            return Reason.WEATHER_CONDITION;
        } else if (reason.equals("Won't Start")) {
            return Reason.WONT_START;
        } else {
            return Reason.OTHER;
        }
    }
}
