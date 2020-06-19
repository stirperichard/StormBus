package com.stirperichard.stormbus.operator;

import com.stirperichard.stormbus.entity.BusRide;
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

import java.io.*;
import java.text.ParseException;
import java.util.Map;

public class DataGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    public static final String PROFIT_STREAM_ID = "num";
    public static final String EMPTY_TAXIS_STREAM_ID = "den";
    boolean _feof;
    private SpoutOutputCollector collector;
    private final String dataPath;
    private BufferedReader reader;

    private long lastTs;
    private int tupleID = 0;

    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
        this.lastTs = 0L;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields = new Fields("Reason", "Occurred_On", "Boro", "Bus_Company_Name", "How_Long_Delayed");
        outputFieldsDeclarer.declareStream(PROFIT_STREAM_ID, fields);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._feof = false;
        this.collector = spoutOutputCollector;
        System.out.println("\u001B[31m" + dataPath + "\u001B[0m");

        try {
            this.reader = new BufferedReader(
                    new FileReader(dataPath)
            );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            // Mi salvo l'header per scartarlo in seguito
            String header = this.reader.readLine();
            String line = this.reader.readLine();

            if (line != null && !line.equals(header)) {
                BusRide tr = BusRide.parse(line);
                Values tuple = new Values(tr.reason.toString(), tr.occurredOn, tr.boro, tr.busCompanyName, tr.howLongDelayed);

                long newTs = tr.getDateTime().getMillis();
                if (lastTs > 0) {
                    // we have to sleep SECONDS_PER_TIME_UNIT
                    // for each TIME_UNIT_IN_SECONDS passed from last tuple
                    // to this one
                    long fromTupleToSystemTime = Constants.TIME_UNIT_IN_SECONDS * Constants.SECONDS_PER_TIME_UNIT;
                    long sleepTime = (newTs - lastTs) /  fromTupleToSystemTime;
                    System.out.println("Sleep for: \u001B[31m" + sleepTime + "\u001B[0m");
                    Utils.sleep(sleepTime);
                }
                lastTs = newTs;

                this.collector.emit(PROFIT_STREAM_ID, tuple);
                System.out.println("\u001B[31m" + tuple + "\u001B[0m");

                tupleID++;
            } else if (!_feof) {
                LOG.info(dataPath + ": FEOF");
                _feof = true;
            }
        } catch (IOException e) {
            LOG.error("Error in reading nextTuple", e);
        } catch (ParseException e) {
            LOG.error("Error in parsing datetime", e);
        }
    }
}