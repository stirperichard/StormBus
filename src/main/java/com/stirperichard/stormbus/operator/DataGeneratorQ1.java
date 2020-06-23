package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class DataGeneratorQ1 extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorQ1.class);

    public static final String REASON           	= "reason";
    public static final String OCCURRED_ON 	        = "occurredOn";
    public static final String BORO 	            = "boro";
    public static final String DATA                 = "data";
    public static final String ID                   = "id";

    public static final String PROFIT_STREAM_ID = "num";
    public static final String EMPTY_TAXIS_STREAM_ID = "den";
    boolean _feof;
    private SpoutOutputCollector collector;
    private final String dataPath;
    private BufferedReader reader;

    private long lastTs;
    private int tupleID = 0;
    private SimpleDateFormat sdf;

    private int i = 0;
    private String header = null;

    public DataGeneratorQ1(String dataPath) {
        this.dataPath = dataPath;
        this.lastTs = 0L;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(ID, DATA, OCCURRED_ON));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._feof = false;
        this.collector = spoutOutputCollector;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        System.out.println("\u001B[31m" + dataPath + "\u001B[0m");

        try {
            this.reader = new BufferedReader(new FileReader(dataPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (i == 0){
            try {
                header = this.reader.readLine();
                i++;
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            String line = null;
            try {
                line = this.reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (line != null && !line.equals(header)) {

                tupleID++;

                String[] data = line.split(";", -1);
                if (data == null || data.length != 21){
                    return;
                }

                Date dDate;
                try {
                    dDate = sdf.parse(data[7]);

                } catch (ParseException e) {
                    e.printStackTrace();
                    return;
                }

                long occurredOnMillis = dDate.getTime();
                Values tuple = new Values();
                tuple.add(tupleID);
                tuple.add(line);
                tuple.add(occurredOnMillis);


                if (lastTs > 0) {
                    // we have to sleep SECONDS_PER_TIME_UNIT
                    // for each TIME_UNIT_IN_SECONDS passed from last tuple
                    // to this one
                    long fromTupleToSystemTime = Constants.TIME_UNIT_IN_SECONDS * Constants.SECONDS_PER_TIME_UNIT;
                    long sleepTime = (occurredOnMillis - lastTs) /  fromTupleToSystemTime;
                    System.out.println("Sleep for: \u001B[31m" + sleepTime + "\u001B[0m");
                    Utils.sleep(sleepTime/10);
                }
                lastTs = occurredOnMillis;

                collector.emit(tuple);

            } else if (!_feof) {
                LOG.info(dataPath + ": FEOF");
                _feof = true;
            }
        }
    }
}