package com.stirperichard.stormbus;

import com.stirperichard.stormbus.operator.*;
import com.stirperichard.stormbus.utils.TConf;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class Main {

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
        builder.setSpout("datasource", new RedisSpout(redisUrl, redisPort));

        //Parser
        builder.setBolt("parser", new ParseCSVQuery1())
                .setNumTasks(numTasks)
                .shuffleGrouping("datasource");

        //Filter
        /*
        builder.setBolt("convertDateTime", new ConvertDatetime())
                .setNumTasks(numTasks)
                .shuffleGrouping("parser");

         */
/*
        builder.setBolt("metronome", new Metronome())
                .setNumTasks(numTasksMetronome)
                .shuffleGrouping("filterByCoordinates");

        builder.setBolt("computeCellID", new ComputeCellID())
                .setNumTasks(numTasks)
                .shuffleGrouping("ConvertDatetime");

        builder.setBolt("countByWindow", new CountByWindow())
                .setNumTasks(numTasks)
                .fieldsGrouping("ConvertDatetime", new Fields(ConvertDatetime.F_ROUTE))
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
        StormSubmitter.submitTopology(args[0], conf, stormTopology);

    }
}
