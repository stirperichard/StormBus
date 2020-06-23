package com.stirperichard.stormbus.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtils {

    //TUMBLING WINDOW FUNCTION
    public static long roundToCompletedHour(long timestamp) {

        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date.getTime().getTime();
    }

    public static long roundToCompletedDay(long timestamp) {

        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date.getTime().getTime();

    }

    public static long lastMonth(long timestamp){
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.DAY_OF_MONTH, 1);
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date.getTimeInMillis();
    }

    public static long lastWeek(long timestamp){
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date.getTimeInMillis();
    }

    // SLIDING WINDOW FUNCTIONS

    public static long lastMonthSlidingWindow(long timestamp){
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MONTH, date.get(Calendar.MONTH)-1);
        return date.getTimeInMillis();
    }

    public static long lastWeekSlidingWindow(long timestamp) {
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.WEEK_OF_MONTH, date.get(Calendar.WEEK_OF_MONTH)-1);
        return date.getTimeInMillis();
    }

    public static long lastDaySlidingWindow(long timestamp) {
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.DAY_OF_MONTH, date.get(Calendar.DAY_OF_MONTH)-1);
        return date.getTimeInMillis();
    }

    public static long lastHourSlidingWindow(long timestamp) {
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.HOUR, date.get(Calendar.HOUR)-1);
        return date.getTimeInMillis();
    }

    //GET INFO FROM DATE FUNCTION

    public static int getInfoDateDay(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    public static int getInfoDateMonth(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.MONTH);
    }

    public static int getInfoDateYear(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.YEAR);
    }

    public static int getInfoDateHour(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int getInfoDateMinute(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.MINUTE);
    }

    public static int getInfoDateSecond(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.SECOND);
    }

    public static int getInfoDateDayOfTheWeek(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.DAY_OF_WEEK);
    }

    //RETRIEVE DATA FUNCTION
    public static String retriveDataFromMillis(long timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy'T'HH:mm:ss");
        Timestamp stamp = new Timestamp(timestamp);
        Date date = new Date(stamp.getTime());
        String date_final = sdf.format(date);
        return date_final;
    }

}
