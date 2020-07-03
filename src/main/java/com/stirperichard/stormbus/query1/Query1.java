package com.stirperichard.stormbus.query1;

import com.stirperichard.stormbus.operator.KafkaSpout;
import com.stirperichard.stormbus.operator.MetronomeQ1Q2;
import com.stirperichard.stormbus.operator.ParseCSVQ1Q2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import static com.stirperichard.stormbus.utils.Constants.S_METRONOME;

public class Query1 {

    public static String INPUT_FILE = "src/main/resources/dataset.csv";
    public static String OUTPUT_PATH = "src/main/results";

    public static void main(String[] args) throws Exception {
	// write your code here
        int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
        int numTasks = 1;



        System.out.println("===================================================== ");
        System.out.println("START QUERY 1:");
        System.out.println("===================================================== ");



        // Build topology
        TopologyBuilder builder = new TopologyBuilder();

        //Redis
        builder.setSpout("spout", new KafkaSpout(), 1);

        //Parser
        builder.setBolt("parser", new ParseCSVQ1Q2())
                .setNumTasks(numTasks)
                .allGrouping("spout");

        //Metronome
        builder.setBolt("metronome", new MetronomeQ1Q2())
                .setNumTasks(numTasksMetronome)
                .allGrouping("parser");

        //Count by window
        builder.setBolt("countByWindow", new CountByWindowQuery1())
                .setNumTasks(numTasks)
                .allGrouping("parser")
                .allGrouping("metronome", S_METRONOME);

        /*
        builder.setBolt("to_file", new DataWriter(OUTPUT_PATH))
                .globalGrouping("countByWindow");
         */

        StormTopology stormTopology = builder.createTopology();

        // Create configurations
        Config conf = new Config();
        conf.setDebug(false);
        // number of workers to create for current topology
        conf.setNumWorkers(3);


        // Update numWorkers using command-line received parameters
        if (args.length == 2){
            try {
                if (args[1] != null) {
                    int numWorkers = Integer.parseInt(args[1]);
                    // number of workers to create for current topology
                    conf.setNumWorkers(numWorkers);
                    StormSubmitter.submitTopology(args[0], conf, stormTopology);
                    System.out.println("Number of workers to generate for current topology set to: " + numWorkers);
                }
            } catch (NumberFormatException ignored) {
            }
        } else {
            // cluster
            //StormSubmitter.submitTopology(args[0], conf, stormTopology);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, stormTopology);
        }
    }
}
