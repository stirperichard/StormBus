package com.stirperichard.stormbus.operator;


import com.stirperichard.stormbus.utils.TimeUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

import static com.stirperichard.stormbus.utils.Constants.*;

/**
 *  DATA WRITER TO CSV
 */
public class DataWriter extends BaseRichBolt {

    private String outputPath;
    private BufferedWriter writer;
    private OutputCollector collector;
    public String latest_type = "";
    public long latest_basetime;
    public String line = "";

    public DataWriter(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.latest_basetime = 0;
        try {
            this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outputPath)
                    )
            );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in opening " + outputPath);
        }

        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String type 			            = tuple.getStringByField(TYPE);
        String boro                         = tuple.getStringByField(BORO);
        long occurredOnMillis       	    = tuple.getLongByField(OCCURREDON_MILLIS);
        double avgDelay                     = tuple.getDoubleByField(AVG_DELAY);

        if(this.latest_basetime == 0)
            this.latest_basetime = occurredOnMillis;
        if(latest_type.isEmpty())
            this.latest_type = type;
        if(line.isEmpty())
            this.line = TimeUtils.retriveDataFromMillis(occurredOnMillis) + " , ";

        System.out.println(TimeUtils.retriveDataFromMillis(occurredOnMillis));

        String new_tuple = boro + " , " + avgDelay + " , ";

        if (latest_basetime < occurredOnMillis) {
            if (type.equals(DAY)) {
                try {
                    writer.write(line + "\n");
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error in writing to " + outputPath);
                }
                this.latest_basetime = occurredOnMillis;
                this.line = "";
                this.latest_type = "";
            }
        }

        this.line += new_tuple;
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output field to declare
    }
}