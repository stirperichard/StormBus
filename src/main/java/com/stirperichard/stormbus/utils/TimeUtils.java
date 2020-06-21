package com.stirperichard.stormbus.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtils {

    //Setto a 0 i millisecondi, secondi e minuti del timestamp così da avere l'ora di appartenenza
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

    public static Date retriveDataFromMillis(long timestamp){
        Timestamp stamp = new Timestamp(timestamp);
        Date date = new Date(stamp.getTime());
        System.out.println(date);
        return date;
    }

    public static Calendar lastMonthSlidingWindow(long timestamp){
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MONTH, date.get(Calendar.MONTH)-1);
        date.set(Calendar.HOUR, date.get(Calendar.HOUR)+1);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date;
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

    private int getInfoDateDay(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    private int getInfoDateMonth(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.MONTH);
    }

    private int getInfoDateYear(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.YEAR);
    }

    private int getInfoDateHour(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.HOUR);
    }

    private int getInfoDateMinute(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.MINUTE);
    }

    private int getInfoDateSecond(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.SECOND);
    }

    private int getInfoDateDayOfTheWeek(String occurredOn) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        //Converto la data in un formato
        Date dDate = sdf.parse(occurredOn);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dDate);
        return calendar.get(Calendar.DAY_OF_WEEK);
    }

}
