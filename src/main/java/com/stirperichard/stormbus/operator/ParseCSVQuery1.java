package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static com.stirperichard.stormbus.utils.ParseTime.minutesDelayed;

public class ParseCSVQuery1 extends BaseRichBolt {

    public static final String F_MSGID				= "MSGID";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
    public static final String F_TIMESTAMP 	= 	"timestamp";

    private OutputCollector collector;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String rawData 	= input.getStringByField(RedisSpout.F_DATA);
        String msgId 	= input.getStringByField(RedisSpout.F_MSGID);
        String timestamp = input.getStringByField(RedisSpout.F_TIMESTAMP);

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

        //Aggiungo il MessageID
        values.add(msgId);


        //Controllo la validità e aggiungo il valore della varibile Occurred_On (campo 7)
        if (!data[7].isEmpty()){
            values.add(data[7]);
        }

        //Controllo la validità e aggiungo il valore della varibile Boro (campo 9)
        if (!data[9].isEmpty()){
            values.add(data[9]);
        }

        //Controllo la validità e setto la varibile How_Long_Delayed (campo 11)
        if (!data[11].isEmpty()){
            values.add(minutesDelayed(data[11]));
        }

        values.add(timestamp);


        collector.emit(values);
        collector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(F_MSGID, OCCURRED_ON, BORO, HOW_LONG_DELAYED, F_TIMESTAMP));
    }
}
