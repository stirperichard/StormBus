package com.stirperichard.stormbus;

import com.stirperichard.stormbus.operator.*;
import com.stirperichard.stormbus.utils.TConf;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class Query2 {

    public static String INPUT_FILE = "src/main/resources/dataset.csv";

    public static void main(String[] args) throws Exception {
	// write your code here
        TConf config = new TConf();
        String redisUrl			= config.getString(TConf.REDIS_URL);
        int redisPort 			= config.getInteger(TConf.REDIS_PORT);
        int numTasks 			= config.getInteger(TConf.NUM_TASKS);
        int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
        int numTasksGlobalRank  = 1;
        String rabbitMqHost 	= config.getString(TConf.RABBITMQ_HOST);
        String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
        String rabbitMqPassword	= config.getString(TConf.RABBITMQ_PASSWORD);

        System.out.println("===================================================== ");
        System.out.println("Configuration:");
        System.out.println("Redis: " + redisUrl + ":" + redisPort);
        System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
        System.out.println("Tasks:" + numTasks);
        System.out.println("===================================================== ");


        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        //Redis
        builder.setSpout("datasource", new DataGeneratorQ1(INPUT_FILE));

        //Parser
        builder.setBolt("parser", new ParseCSV())
                .setNumTasks(numTasks)
                .allGrouping("datasource");

        builder.setBolt("filterbytime", new FilterByTime())
                .setNumTasks(numTasks)
                .allGrouping("parser");

        //Metronome
        builder.setBolt("metronome", new Metronome())
                .setNumTasks(numTasksMetronome)
                .allGrouping("parser");

        //Count by window
        builder.setBolt("countByWindow2", new CountByWindowQuery2())
                .setNumTasks(numTasks)
                .allGrouping("filterbytime")
                .allGrouping("metronome", Metronome.S_METRONOME);

		/* Two operators that realize the top-10 ranking in two steps (typical design pattern):
        PartialRank can be distributed and parallelized,
        whereas TotalRank is centralized and computes the global ranking */
/*
        builder.setBolt("partialRank", new PartialRank(10))
                .setNumTasks(numTasks)
                .fieldsGrouping("countByWindow", new Fields(ComputeCellID.F_ROUTE));

        builder.setBolt("globalRank", new GlobalRank(10, rabbitMqHost, rabbitMqUsername, rabbitMqPassword), 1)
                .setNumTasks(numTasksGlobalRank)
                .shuffleGrouping("partialRank");

        builder.setBolt("rankings", new RankingBolt(TOP_N)).globalGrouping("profitability");

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
        /*
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

         */

    }
}
