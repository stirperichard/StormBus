package com.stirperichard.stormbus.utils;

import com.stirperichard.stormbus.entity.Bus;
import com.stirperichard.stormbus.enums.Reason;

public class ParseCSV {

    public static Bus parseCSV(String csvLine) {

        String[] csvValues = csvLine.split(";", -1);

        String[] occurredOn = csvValues[7].split("T", -1);
        String[] createdOn = csvValues[8].split("T", -1);
        String[] informedOn = csvValues[17].split("T", -1);
        String[] lastUpdatedOn = csvValues[19].split("T", -1);

        Bus busData = new Bus();

        //Controllo la validità del campo 1
        /*
        if (!csvValues[0].isEmpty() && csvValues[0].matches("\\d{4}-\\d{4}")){
            busData.setSchoolYear(csvValues[0]);
        }

         */
        //Controllo la validità del campo 2
        if (!csvValues[1].isEmpty()){
            busData.setBusbreakdownID(Integer.parseInt(csvValues[1]));
        }

        //Controllo la validità e setto la varibile Reason (campo 5)
        if (!csvValues[5].isEmpty()){
            busData.setReason(mappingReason(csvValues[6]));
        }

        //Controllo la validità e setto la varibile Occurred_On (campo 7)
        if (!csvValues[5].isEmpty()){
            busData.setOccurredOn(csvValues[7]);
        }

        //Controllo la validità e setto la varibile Boro (campo 9)
        if (!csvValues[9].isEmpty()){
            busData.setBoro(csvValues[9]);
        }

        //Controllo la validità e setto la varibile Bus_Company_Name (campo 10)
        if (!csvValues[9].isEmpty()){
            busData.setBoro(csvValues[10]);
        }

        //Controllo la validità e setto la varibile How_Long_Delayed (campo 11)
        if (!csvValues[10].isEmpty()){
            busData.setBoro(csvValues[10]);
        } else {
            busData.setBoro(String.valueOf(0));
        }

        return busData;
    }





    public static Reason mappingReason(String reason) {
        if (reason.equals("Accident")) {
            return Reason.ACCIDENT;
        } else if (reason.equals("Delayed by School")) {
            return Reason.DELAYED_BY_SCHOOL;
        } else if (reason.equals("Flat Tire")) {
            return Reason.FLAT_TIRE;
        } else if (reason.equals("Heavy Traffic")) {
            return Reason.HEAVY_TRAFFIC;
        } else if (reason.equals("Mechanical Problem")) {
            return Reason.MECHANICAL_PROBLEM;
        } else if (reason.equals("Problem Run")) {
            return Reason.PROBLEM_RUN;
        } else if (reason.equals("Weather Condition")) {
            return Reason.WEATHER_CONDITION;
        } else if (reason.equals("Won't Start")) {
            return Reason.WONT_START;
        } else {
            return Reason.OTHER;
        }
    }
}
