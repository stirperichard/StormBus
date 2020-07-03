package com.stirperichard.stormbus.query3;

import com.stirperichard.stormbus.operator.DataGenerator;
import com.stirperichard.stormbus.operator.MetronomeQuery3;
import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

public class MetronomeBolt extends BaseRichBolt {

    public static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
    public static final long MILLIS_IN_WEEK = 7 * MILLIS_IN_DAY;

    private OutputCollector _collector;
    private long elapsedTime_d;
    private long elapsedTime_w;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.elapsedTime_d = 0;
        this.elapsedTime_w = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        long time = Long.parseLong(tuple.getStringByField(MetronomeQuery3.F_TIME));
        long currentTimestamp = tuple.getLongByField(Constants.CURRENNT_TIMESTAMP);

        if (this.elapsedTime_d == 0)
            this.elapsedTime_d = time;
        if (this.elapsedTime_w == 0)
            this.elapsedTime_w = time;

        else {
            // Metronome sends tick every day
            if (time - this.elapsedTime_d >= MILLIS_IN_DAY) {
                this.elapsedTime_d = 0;
                Values values = new Values();
                values.add(time);
                values.add(currentTimestamp);
                System.out.println("\033[0;32m" + "Tick Tuple day" + "\u001B[0m");
                _collector.emit(Constants.METRONOME_D_STREAM_ID, values);
            }

            // Metronome sends tick every week
            if (time - this.elapsedTime_w >= MILLIS_IN_WEEK) {
                this.elapsedTime_w = 0;
                Values values = new Values();
                values.add(time);
                values.add(currentTimestamp);
                System.out.println("\033[0;32m" + "Tick Tuple week" + "\u001B[0m");
                _collector.emit(Constants.METRONOME_W_STREAM_ID, values);
            }
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.METRONOME_D_STREAM_ID,
                new Fields(Constants.TIMESTAMP, Constants.CURRENNT_TIMESTAMP));

        outputFieldsDeclarer.declareStream(Constants.METRONOME_W_STREAM_ID,
                new Fields(Constants.TIMESTAMP, Constants.CURRENNT_TIMESTAMP));
    }
}
