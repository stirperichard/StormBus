package com.stirperichard.stormbus.query3;

public class Configuration {

    // Kafka configuration
    private static final String KAFKA_BROKER = "localhost:9092";
    public static final String ZOOKEEPER = "localhost:2181";
    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER;
    public static final String TOPIC_1_INPUT = "query-1-input";
    public static final String TOPIC_2_INPUT = "query-2-input";
    public static final String TOPIC_3_INPUT = "query-3-input";
    public static final String TOPIC_1_OUTPUT = "query-1-output";
    public static final String TOPIC_2_OUTPUT = "query-2-output";
    public static final String TOPIC_3_OUTPUT = "query-3-output";
    public static final String PRODUCER_GROUPID = "producer";
    public static final String CONSUMER_GROUPID = "consumer";
    public static final String GLOBAL_GROUP_ID = "nyt-comments";

    // Dataset configuration
    public static final String DATASET = "/Users/simone/Projects/proj_2/data/Comments_jan-apr2018.csv";

    // Discard probability for the SamplingBolt
    public static final int PERCENT = 10;

    // Data fields
    public static final String TIMESTAMP = "timestamp";
    public static final String RAW_DATA = "rawdata";
    public static final String CURRENNT_TIMESTAMP = "current";

    public static final String[] PARSER_QUERY_1 = {"occurred_on", "bus_company"};

    public static final String METRONOME_D_STREAM_ID = "d_msg";
    public static final String METRONOME_W_STREAM_ID = "w_msg";

    public static final String PARSER_STREAM_ID = "parser";
    public static final String ESTIMATED_TOTAL = "estimated_total";
    public static final String PARTIAL_RANKING = "partial_ranking";
    public static final String SCORE = "score";

}
