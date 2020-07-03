package com.stirperichard.stormbus.operator;


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
public class DataWriter extends BaseRichBolt {

    private String outputPath;
    private BufferedWriter writer;
    private OutputCollector collector;
    private String outputFile;

    public DataWriter(String outputPath) {
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
        String line = tuple.toString();
        try {
            writer.write(line + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in writing to " + outputPath);
        }

        collector.ack(tuple);
    }

    protected String getNewLine(Tuple t) {
        List<Tuple> ranking = (List<Tuple>) t.getValue(0);
        String l = "{\n";

        for (Tuple r : ranking) {
            if (r != null) {
                String cell = r.getString(1);
                Double profit = r.getDouble(2);
                l += "\t\'" + cell + "\': " + profit + ",\n";
            } else {
                l += "\tNULL,\n";
            }
        }

        l = l.substring(0, l.length() - 2); // remove last comma
        l += "\n}";
        return l;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output field to declare
    }
}