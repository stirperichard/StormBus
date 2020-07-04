package com.stirperichard.stormbus.query3;


import com.stirperichard.stormbus.operator.DataGenerator;
import com.stirperichard.stormbus.operator.DataWriter;
import com.stirperichard.stormbus.operator.DataWriterQ3;
import com.stirperichard.stormbus.operator.KafkaSpout;
import com.stirperichard.stormbus.utils.Constants;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import javax.xml.crypto.Data;

import static com.stirperichard.stormbus.utils.Constants.*;

public class TopologyQ3 {

    public static String OUTPUT_FILE = "result_query3.output";
    public static int TOP_K_COMPANIES = 5;


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("spout", new KafkaSpout(), 1);
        builder.setSpout("spout", new DataGenerator("src/main/resources/dataset.csv"), 1);

        builder.setBolt("parser", new ParserBolt(), 1)
                .shuffleGrouping("spout");



        builder.setBolt("filter", new FilterReason(), 1)
                .fieldsGrouping(
                        "parser",
                        new Fields(DataGenerator.BUS_COMPANY_NAME)
                );


        builder.setBolt("metronome", new MetronomeBolt(), 1)
                .allGrouping("filter");

        builder.setBolt("count_by_day", new CountByDayBolt(), 1)
                .allGrouping("filter")
                .allGrouping("metronome", Constants.METRONOME_D_STREAM_ID);
                //.allGrouping("metronome", Constants.METRONOME_W_STREAM_ID);


        builder.setBolt("partial", new PartialRankBolt(TOP_K_COMPANIES), 1)
                //.allGrouping("count_by_day")
                .fieldsGrouping("count_by_day", new Fields(Constants.BUS_COMPANY_NAME));


        builder.setBolt("global_h", new GlobalRankBolt(false, TOP_K_COMPANIES, Constants.TOPIC_3_OUTPUT), 1)
                .allGrouping("partial");

        builder.setBolt("data_writer", new DataWriterQ3(QUERY_3_OUTPUT_DAILY), 1)
                .allGrouping("global_h");


        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("query3", conf, builder.createTopology());
            //Thread.sleep(100000);
            //cluster.shutdown();
        }
    }
}
