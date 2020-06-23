package com.stirperichard.stormbus;

import com.stirperichard.stormbus.operator.*;
import com.stirperichard.stormbus.utils.TConf;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

public class Query3 {

    public static final int TOP_N_COMPANIES = 5;
    public static int DAILY_WINDOW = 24 * 60 * 60; // in seconds
    public static int EMPTY_TAXIS_WINDOW = 30 * 60; // in seconds
    public static String INPUT_FILE = "src/main/resources/dataset.csv";
    public static String OUTPUT_FILE = "result_query3.output";

    int numTasks 			= 3;
    int numTasksMetronome   = 1;  // each task of the metronome generate a flood of messages
    int numTasksGlobalRank  = 1;


    public static void main(String[] args) throws Exception {
        // write your code here
        TConf config = new TConf();
        String redisUrl = config.getString(TConf.REDIS_URL);
        int redisPort = config.getInteger(TConf.REDIS_PORT);
        int numTasks = config.getInteger(TConf.NUM_TASKS);
        int numTasksMetronome = 1;  // each task of the metronome generate a flood of messages
        int numTasksGlobalRank = 1;
        String rabbitMqHost = config.getString(TConf.RABBITMQ_HOST);
        String rabbitMqUsername = config.getString(TConf.RABBITMQ_USERNAME);
        String rabbitMqPassword = config.getString(TConf.RABBITMQ_PASSWORD);

        System.out.println("===================================================== ");
        System.out.println("Configuration:");
        System.out.println("Redis: " + redisUrl + ":" + redisPort);
        System.out.println("RabbitMQ: " + rabbitMqHost + " (user: " + rabbitMqUsername + ", " + rabbitMqPassword + ")");
        System.out.println("Tasks:" + numTasks);
        System.out.println("===================================================== ");



        /* Build topology */
        TopologyBuilder builder = new TopologyBuilder();

        //Redis
        builder.setSpout("dataSource", new DataGenerator(INPUT_FILE));
        //Parser
        builder.setBolt("filterReason", new FilterReason())
                .setNumTasks(numTasks)
                .fieldsGrouping(
                        "dataSource",
                        DataGenerator.PROFIT_STREAM_ID,
                        new Fields(DataGenerator.BUS_COMPANY_NAME)
                );

        builder.setBolt("metronome", new MetronomeQuery3())
                .setNumTasks(numTasksMetronome)
                .shuffleGrouping("filterReason");


        builder.setBolt("computeScore", new ComputeScoreByWindow())
                .setNumTasks(numTasks)
                .fieldsGrouping("filterReason", new Fields(DataGenerator.BUS_COMPANY_NAME))
                .allGrouping("metronome", Metronome.S_METRONOME);


        /*
        builder.setBolt("compute", new FilterReason()
                        .withWindow(
                                new BaseWindowedBolt.Duration(24, TimeUnit.SECONDS),
                                new BaseWindowedBolt.Duration(4, TimeUnit.SECONDS)
                        ),
                12)
                //.fieldsGrouping("datasource", new Fields("Bus_Company_Name"));
                .fieldsGrouping(
                        "datasource",
                        DataGenerator.PROFIT_STREAM_ID,
                        new Fields("Bus_Company_Name")
                );

         */

/*
        builder.setBolt("metronome", new Metronome())
                .setNumTasks(numTasksMetronome)
                .shuffleGrouping("filterByCoordinates");

        builder.setBolt("computeCellID", new ComputeCellID())
                .setNumTasks(numTasks)
                .shuffleGrouping("filterByCoordinates");

        builder.setBolt("countByWindow", new CountByWindow())
                .setNumTasks(numTasks)
                .fieldsGrouping("computeCellID", new Fields(ComputeCellID.F_ROUTE))
                .allGrouping("metronome", Metronome.S_METRONOME);
*/
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
*/

        StormTopology stormTopology = builder.createTopology();

        /* Create configurations */
        Config conf = new Config();
        conf.setDebug(false);


        /* Update numWorkers using command-line received parameters */
        if (args.length == 2) {

            try {
                if (args[1] != null) {
                    int numWorkers = Integer.parseInt(args[1]);
                    /* number of workers to create for current topology */
                    conf.setNumWorkers(numWorkers);
                    StormSubmitter.submitTopology(args[0], conf, stormTopology);
                    System.out.println("Number of workers to generate for current topology set to: " + numWorkers);
                }
            } catch (NumberFormatException ignored) {
            }

        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            //Utils.sleep(15000);
            //cluster.killTopology("test");
            //cluster.shutdown();
        }
    }
}
