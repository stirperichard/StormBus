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
	
}
