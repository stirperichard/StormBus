package com.stirperichard.stormbus.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Metronome extends BaseRichBolt {

    public static final String S_METRONOME 			= "sMetronome";
    public static final String F_MSGID				= "msgId";
    public static final String F_TIME				= "time";
    public static final String OCCURRED_ON       	= "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String HOW_LONG_DELAYED 	= "howLongDelayed";


    private long currentTime = 0;
    private long latestMsgId = 0;
    private OutputCollector collector;
    private SimpleDateFormat sdfD, sdfT;

    public Metronome(){

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.sdfD = new SimpleDateFormat("yyyy-MM-dd");
        this.sdfT = new SimpleDateFormat("HH:mm:ss.000");
    }

    @Override
    public void execute(Tuple input) {

        String msgId 	   = input.getStringByField(RedisSpout.F_MSGID);
        String occurredOn  = input.getStringByField(ParseCSVQuery1.OCCURRED_ON);
        String boro        = input.getStringByField(ParseCSVQuery1.BORO);
        String howLongDelayed = input.getStringByField(ParseCSVQuery1.HOW_LONG_DELAYED);

        String[] times = occurredOn.split("T", -1);

        try {
            Date dDate = sdfD.parse(times[0]);
            Date tDate = sdfT.parse(times[1]);
        } catch (ParseException e) {
            collector.ack(input);
            return;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(F_MSGID, F_TIME, OCCURRED_ON, BORO, HOW_LONG_DELAYED));

    }
}
