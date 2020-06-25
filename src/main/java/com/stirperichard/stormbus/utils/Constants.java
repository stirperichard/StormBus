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
	public static final String dataset = "/home/richard/IdeaProjects/StormBus/src/main/resources/bus-breakdown-and-delays.csv";


	//Millis
	public static final long MILLIS_HOUR = 1000*60*60;
	public static final long MILLIS_DAY = MILLIS_HOUR*24;
	public static final long MILLIS_WEEK = MILLIS_DAY*7;
	public static long MILLIS_MONTH;


}
