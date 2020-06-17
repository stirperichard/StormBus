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
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Map;

public class DataGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    public static final String PROFIT_STREAM_ID = "num";
    public static final String EMPTY_TAXIS_STREAM_ID = "den";
    boolean _feof;
    private SpoutOutputCollector collector;
    private String dataPath;
    private BufferedReader reader;

    private long lastTs;
    private int tupleID = 0;

    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
        this.lastTs = 0L;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields =  new Fields("tupleID", "taxiRide", "pickupCell", "taxiID");
        outputFieldsDeclarer.declareStream(PROFIT_STREAM_ID, fields);
        outputFieldsDeclarer.declareStream(EMPTY_TAXIS_STREAM_ID, fields);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._feof = false;
        this.collector = spoutOutputCollector;
        this.reader = new BufferedReader(
                new InputStreamReader(
                        getClass().getResourceAsStream(dataPath)
                )
        );
    }

    @Override
    public void nextTuple() {
        try {
            String line = this.reader.readLine();

            if (line != null) {
                TaxiRide tr = TaxiRide.parse(line);
                Values tuple = new Values(tupleID, tr, tr.pickupCell, tr.taxiID);

                long newTs = tr.dropoffTS.getTime();
                if (lastTs > 0) {
                    // we have to sleep SECONDS_PER_TIME_UNIT
                    // for each TIME_UNIT_IN_SECONDS passed from last tuple
                    // to this one
                    long fromTupleToSystemTime = Constants.TIME_UNIT_IN_SECONDS * Constants.SECONDS_PER_TIME_UNIT;
                    long sleepTime = (newTs - lastTs) /  fromTupleToSystemTime;
                    Utils.sleep(sleepTime);
                }
                lastTs = newTs;

                this.collector.emit(PROFIT_STREAM_ID, tuple);
                this.collector.emit(EMPTY_TAXIS_STREAM_ID, tuple);
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