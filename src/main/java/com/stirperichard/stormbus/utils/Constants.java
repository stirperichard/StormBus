package com.stirperichard.stormbus.utils;

import java.text.SimpleDateFormat;

public class Constants {

	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	//Constant for Redis
	public static final String REDIS_DATA		= "data";
	public static final String REDIS_CONSUMED 	= "consumed";
	public static final String REDIS_EOF 		= "EOF";
	public static final String RABBITMQ_RESULTS = "stormbus-results";

	//Constant for Query3
	public final static double WT = 0.3;
	public final static double WM = 0.5;
	public final static double WO = 0.2;

	//Constants files Q1 / Q2

	public static final String MSGID				= "MSGID";
	public static final String BUS_BREAKDOWN_ID    	= "busBreakdownId";

	public static final String F_MSGID				= "MSGID";
	public static final String REASON           	= "reason";
	public static final String OCCURRED_ON       	= "occurredOn";
	public static final String BORO 	            = "boro";
	public static final String BUS_COMPANY_NAME    	= "busCompanyName";
	public static final String HOW_LONG_DELAYED 	= "howLongDelayed";
	public static final String DATA                 = "data";
	public static final String ID                   = "id";

	public static final String S_METRONOME          = "sMetronome";
	public static final String OCCURREDON_MILLIS    = "time";
	public static final String METRONOME_D          = "metronome_day";
	public static final String METRONOME_W          = "metronome_week";
	public static final String METRONOME_M          = "metronome_month";
	public static final String DAY_IN_MONTH         = "day_in_month";
	public static final String TYPE_OF_METRONOME    = "type";
	public static final String METRONOME_ID         = "metronomeID";

	public static final String F_TIMESTAMP          = "timestamp_real";
	public static final String AVG_DELAY            = "avg_delay";
	public static final String DAY                  = "day";
	public static final String WEEK                 = "week";
	public static final String MONTH                = "month";
	public static final String TYPE                 = "type";

	public static final String MORNING              = "morning";
	public static final String AFTERNOON            = "afternoon";

	public static final String OCCURRED_ON_MILLIS_BASETIME 		= "OccurredOnMillisBasetime";
	public static final String TOTAL 				= "total";
	public static final String MORNING_OR_AFTERNOON = "morningOrAfternoon";

	public static final String TOPK                 = "topK";

	public static final String OUT 					= "output";



	// how often a tick tuple will be sent to our bolt
	public static final int SECONDS_PER_TIME_UNIT = 3;
	// mapping system time to time in tuples
	public static final int TIME_UNIT_IN_SECONDS = 3 * 60 * 60;

	public static final double Wt = 0.3;
	public static final double Wm = 0.5;
	public static final double Wo = 0.2;


	//Dataset
	public static final String DATASET = "src/main/resources/dataset.csv";

	public static final String QUERY_2_OUTPUT_DAILY = "src/main/resources/results_query_2_daily.csv";
	public static final String QUERY_2_OUTPUT_WEEKLY = "src/main/resources/results_query_2_weekly.csv";

	public static final String QUERY_3_OUTPUT_DAILY = "src/main/resources/results_query_3_daily.csv";
	public static final String QUERY_3_OUTPUT_WEEKLY = "src/main/resources/results_query_3_weekly.csv";


	//Millis
	public static final long MILLIS_HOUR = 1000*60*60;
	public static final long MILLIS_DAY = MILLIS_HOUR*24;
	public static final long MILLIS_WEEK = MILLIS_DAY*7;
	public static long MILLIS_MONTH;



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
	public static final String GLOBAL_GROUP_ID = "bus-ride";

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
