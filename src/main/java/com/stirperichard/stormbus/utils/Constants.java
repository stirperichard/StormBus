package com.stirperichard.stormbus.utils;

public class Constants {

	//Constant for Redis
	public static final String REDIS_DATA		= "data";
	public static final String REDIS_CONSUMED 	= "consumed";
	public static final String REDIS_EOF 		= "EOF";
	public static final String RABBITMQ_RESULTS = "stormbus-results";

	//Constant for Query3
	public final static double WT = 0.3;
	public final static double WM = 0.5;
	public final static double WO = 0.2;

	//Constants for ParseCSV file
	public static final String F_MSGID				= "MSGID";
	public static final String REASON           	= "reason";
	public static final String OCCURRED_ON       	= "occurredOn";
	public static final String BORO 	            = "boro";
	public static final String BUS_COMPANY_NAME    	= "busCompanyName";
	public static final String HOW_LONG_DELAYED 	= "howLongDelayed";



	// how often a tick tuple will be sent to our bolt
	public static final int SECONDS_PER_TIME_UNIT = 1;
	// mapping system time to time in tuples
	public static final int TIME_UNIT_IN_SECONDS = 60 * 60;

	public static final double Wt = 0.3;
	public static final double Wm = 0.5;
	public static final double Wo = 0.2;


}
