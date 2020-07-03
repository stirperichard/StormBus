package com.stirperichard.stormbus.operator;


import com.stirperichard.stormbus.query3.RankItemQ3;
import com.stirperichard.stormbus.utils.Constants;
import com.stirperichard.stormbus.utils.TimeUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 *  DATA WRITER TO CSV
 */
public class DataWriterQ3 extends BaseRichBolt {

    private String outputPath;
    private BufferedWriter writer;
    private OutputCollector collector;
    private String outputFile;

    public DataWriterQ3(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
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

        long tupleTimestamp = tuple.getLongByField(Constants.TIMESTAMP);
        List<RankItemQ3> data =  (List<RankItemQ3>) tuple.getValueByField(Constants.RAW_DATA);

        String line = TimeUtils.retriveDataFromMillis(tupleTimestamp) + "; ";

        for(RankItemQ3 item : data){
            line = line + item.getBusCompanyName() + "; " + item.getScore() + "; ";
        }

        try {
            writer.write(line + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in writing to " + outputPath);
        }

        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output field to declare
    }
}