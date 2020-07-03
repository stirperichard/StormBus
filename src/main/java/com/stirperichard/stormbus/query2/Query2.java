package com.stirperichard.stormbus.query2;

import com.stirperichard.stormbus.operator.KafkaSpout;
import com.stirperichard.stormbus.operator.MetronomeQ1Q2;
import com.stirperichard.stormbus.operator.ParseCSVQ1Q2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static com.stirperichard.stormbus.utils.Constants.MORNING_OR_AFTERNOON;
import static com.stirperichard.stormbus.utils.Constants.S_METRONOME;

public class Query2 {

    public static String INPUT_FILE = "src/main/resources/dataset.csv";

    public static void main(String[] args) throws Exception {
	// write your code here
        int numTasks 			= 1;
        int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
        int numTasksGlobalRank  = 1;

        System.out.println("===================================================== ");
        System.out.println("START QUERY 2:");
        System.out.println("===================================================== ");


        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        //Redis
        builder.setSpout("spout", new KafkaSpout(), 1);

        //Parser
        builder.setBolt("parser", new ParseCSVQ1Q2())
                .setNumTasks(numTasks)
                .allGrouping("spout");

        builder.setBolt("filterbytime", new FilterByTimeQ2())
                .setNumTasks(numTasks)
                .allGrouping("parser");

        //Metronome
        builder.setBolt("metronome", new MetronomeQ1Q2())
                .setNumTasks(numTasksMetronome)
                .allGrouping("parser");

        //Count by window
        builder.setBolt("countByWindow2", new CountByWindowQuery2())
                .setNumTasks(numTasks)
                .allGrouping("filterbytime")
                .allGrouping("metronome", S_METRONOME);

		/* Two operators that realize the top-10 ranking in two steps (typical design pattern):
        PartialRank can be distributed and parallelized,
        whereas TotalRank is centralized and computes the global ranking */

        builder.setBolt("partialRank", new PartialRankQ2(3))
                .setNumTasks(1)
                //.setNumTasks(numTasks)
                .fieldsGrouping("countByWindow2", new Fields(MORNING_OR_AFTERNOON));

        builder.setBolt("globalRank", new GlobalRankQ2(3))
                .setNumTasks(numTasksGlobalRank)
                .allGrouping("partialRank");
/*

        builder.setBolt("to_file", new DataWriter(OUTPUT_FILE)).globalGrouping("rankings");
*/
        StormTopology stormTopology = builder.createTopology();

        /* Create configurations */
        Config conf = new Config();
        conf.setDebug(false);
        /* number of workers to create for current topology */
        conf.setNumWorkers(3);


        /* Update numWorkers using command-line received parameters */
        if (args.length == 2){
            try{
                if (args[1] != null){
                    int numWorkers = Integer.parseInt(args[1]);
                    conf.setNumWorkers(numWorkers);
                    System.out.println("Number of workers to generate for current topology set to: " + numWorkers);
                }
            } catch (NumberFormatException nf){}
        }

        // cluster
        //StormSubmitter.submitTopology(args[0], conf, stormTopology);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());


    }
}
