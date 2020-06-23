package com.stirperichard.stormbus.query3;

import com.stirperichard.stormbus.operator.DataGenerator;
import com.stirperichard.stormbus.operator.MetronomeQuery3;
import com.stirperichard.stormbus.utils.Constants;
import com.stirperichard.stormbus.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountByDayBolt extends BaseRichBolt {
    private Map<String, Map<String, Window>> map;
    private OutputCollector _collector;
    private long lastTick;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.map = new HashMap<>();
        this.lastTick = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        String msgType = tuple.getSourceStreamId();

        // When a tick by metronome is received, it handles the window shifting operations
        if (msgType.equals(Configuration.METRONOME_D_STREAM_ID)) {

            long tupleTimestamp = tuple.getLongByField(Configuration.TIMESTAMP);
            long currentTimestamp = tuple.getLongByField(Configuration.CURRENNT_TIMESTAMP);

            int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_IN_DAY);

            // Control: only informations relating to the current window are processed
            for (String busCompanyName : this.map.keySet()) {

                int count_heavy_traffic = 0;
                int count_mechanical_problem = 0;
                int count_other = 0;

                for (String reason : this.map.get(busCompanyName).keySet()) {
                    Window window = this.map.get(busCompanyName).get(reason);

                    if (reason.equals("Heavy Traffic")) {
                        count_heavy_traffic = window.getEstimatedTotal();
                    } else if (reason.equals("Mechanical Problem")) {
                        count_mechanical_problem = window.getEstimatedTotal();
                    } else
                        count_other = window.getEstimatedTotal();

                    window.moveForward(elapsedHour);

                }

                double score = Constants.WT * count_heavy_traffic +
                        Constants.WM * count_mechanical_problem + Constants.WO * count_other;

                Values values = new Values();
                values.add(tupleTimestamp);
                values.add(currentTimestamp);
                values.add(Configuration.METRONOME_D_STREAM_ID);
                values.add(busCompanyName);
                values.add(score);

                System.out.println("\033[0;35m" + values + "\u001B[0m");

                _collector.emit(values);
            }
            this.lastTick = tupleTimestamp;

        }else if (msgType.equals(Configuration.METRONOME_W_STREAM_ID)) {

            long tupleTimestamp = tuple.getLongByField(Configuration.TIMESTAMP);
            long currentTimestamp = tuple.getLongByField(Configuration.CURRENNT_TIMESTAMP);

            int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_IN_WEEK);

            // Control: only informations relating to the current window are processed
            for (String busCompanyName : this.map.keySet()) {

                int count_heavy_traffic = 0;
                int count_mechanical_problem = 0;
                int count_other = 0;

                for (String reason : this.map.get(busCompanyName).keySet()) {
                    Window window = this.map.get(busCompanyName).get(reason);

                    if (reason.equals("Heavy Traffic")) {
                        count_heavy_traffic = window.getEstimatedTotal();
                    } else if (reason.equals("Mechanical Problem")) {
                        count_mechanical_problem = window.getEstimatedTotal();
                    } else
                        count_other = window.getEstimatedTotal();

                    window.moveForward(elapsedHour);

                }

                double score = Constants.WT * count_heavy_traffic +
                        Constants.WM * count_mechanical_problem + Constants.WO * count_other;

                Values values = new Values();
                values.add(tupleTimestamp);
                values.add(currentTimestamp);
                values.add(Configuration.METRONOME_W_STREAM_ID);
                values.add(busCompanyName);
                values.add(score);

                System.out.println("\033[0;35m" + values + "\u001B[0m");

                _collector.emit(values);
            }
            this.lastTick = tupleTimestamp;
        }
        // When a msg from parser is received, it handles memorization operations in the window
        else {
            String busCompanyName = tuple.getStringByField(DataGenerator.BUS_COMPANY_NAME);
            String reason = tuple.getStringByField(DataGenerator.REASON);
            String howLongDelayed = tuple.getStringByField(DataGenerator.HOW_LONG_DELAYED);

            long timestamp = Long.parseLong(tuple.getStringByField(MetronomeQuery3.F_TIME));

            // Control: only informations relating to the current window are processed
            if (timestamp > this.lastTick) {
                // If there isn't the key in the map, create a new <key, value> object

                this.map.putIfAbsent(busCompanyName, new HashMap<>());
                this.map.get(busCompanyName).putIfAbsent(reason, null);

                Window window = this.map.get(busCompanyName).get(reason);
                if (window == null) {
                    window = new Window(7);
                    map.get(busCompanyName).put(reason, window); // This is a tumbling window
                }

                if(Integer.parseInt(howLongDelayed) <= 30)
                    window.increment();
                else{
                    window.increment();
                    window.increment();
                }

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Configuration.TIMESTAMP, Configuration.CURRENNT_TIMESTAMP,
                Configuration.METRONOME_D_STREAM_ID,
                DataGenerator.BUS_COMPANY_NAME, Configuration.SCORE));
    }
}
