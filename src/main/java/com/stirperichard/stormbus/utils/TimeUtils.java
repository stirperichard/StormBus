package com.stirperichard.stormbus.utils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtils {

    //Setto a 0 i millisecondi, secondi e minuti del timestamp così da avere l'ora di appartenenza
    public static long roundToCompletedHour(String timestamp) {

        Date d = new Date(Long.parseLong(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();
    }


    public static long addHours(String timestamp, int h) {

        Date d = new Date(Long.parseLong(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        date.add(Calendar.HOUR, h);


        return date.getTime().getTime();
    }


    public static long roundToCompletedMonth(String timestamp) {

        Date d = new Date(Long.valueOf(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);

        date.set(Calendar.DAY_OF_MONTH, 1);
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();

    }


    public static Date retriveFromMillis(String timestamp){
        Timestamp stamp = new Timestamp(Long.valueOf(timestamp));
        Date date = new Date(stamp.getTime());
        System.out.println(date);
        return date;
    }

    public static Calendar lastMonth(String timestamp){
        Date d = new Date(Long.valueOf(timestamp));
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.MONTH, date.get(Calendar.MONTH)-1);
        date.set(Calendar.HOUR, date.get(Calendar.HOUR)+1);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date;
    }
}
