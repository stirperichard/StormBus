package com.stirperichard.stormbus.entity;

import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.text.ParseException;

import static com.stirperichard.stormbus.utils.ParseTime.minutesDelayed;

public class BusRide implements Serializable {

    private String schoolYear;
    public String busbreakdownID;
    private String runType;
    private String busNo;
    private String routeNumber;
    public String reason;
    private String schoolsServiced;
    public String occurredOn;
    private String createdOn;
    public String boro;
    public String busCompanyName;
    public String howLongDelayed;
    private int numerOfStudentsOnTheBus;
    private String hasContractorNotifiedSchool;
    private String hasContractorNotifiedParents;
    private String haveYouAlertedOPT;
    private String informedOn;
    private String incidentNumber;
    private String lastUpdatedOn;
    private String schoolAgeOrPreK;

    public String getSchoolYear() {
        return schoolYear;
    }

    public void setSchoolYear(String schoolYear) {
        this.schoolYear = schoolYear;
    }

    public String getBusbreakdownID() {
        return busbreakdownID;
    }

    public void setBusbreakdownID(String busbreakdownID) {
        this.busbreakdownID = busbreakdownID;
    }

    public String getRunType() {
        return runType;
    }

    public void setRunType(String runType) {
        this.runType = runType;
    }

    public String getBusNo() {
        return busNo;
    }

    public void setBusNo(String busNo) {
        this.busNo = busNo;
    }

    public String getRouteNumber() {
        return routeNumber;
    }

    public void setRouteNumber(String routeNumber) {
        this.routeNumber = routeNumber;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getSchoolsServiced() {
        return schoolsServiced;
    }

    public void setSchoolsServiced(String schoolsServiced) {
        this.schoolsServiced = schoolsServiced;
    }

    public String getOccurredOn() {
        return occurredOn;
    }

    public void setOccurredOn(String occurredOn) {
        this.occurredOn = occurredOn;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
    }

    public String getBoro() {
        return boro;
    }

    public void setBoro(String boro) {
        this.boro = boro;
    }

    public String getBusCompanyName() {
        return busCompanyName;
    }

    public void setBusCompanyName(String busCompanyName) {
        this.busCompanyName = busCompanyName;
    }

    public String getHowLongDelayed() {
        return howLongDelayed;
    }

    public void setHowLongDelayed(String howLongDelayed) {
        this.howLongDelayed = howLongDelayed;
    }

    public int getNumerOfStudentsOnTheBus() {
        return numerOfStudentsOnTheBus;
    }

    public void setNumerOfStudentsOnTheBus(int numerOfStudentsOnTheBus) {
        this.numerOfStudentsOnTheBus = numerOfStudentsOnTheBus;
    }

    public String getHasContractorNotifiedSchool() {
        return hasContractorNotifiedSchool;
    }

    public void setHasContractorNotifiedSchool(String hasContractorNotifiedSchool) {
        this.hasContractorNotifiedSchool = hasContractorNotifiedSchool;
    }

    public String getHasContractorNotifiedParents() {
        return hasContractorNotifiedParents;
    }

    public void setHasContractorNotifiedParents(String hasContractorNotifiedParents) {
        this.hasContractorNotifiedParents = hasContractorNotifiedParents;
    }

    public String getHaveYouAlertedOPT() {
        return haveYouAlertedOPT;
    }

    public void setHaveYouAlertedOPT(String haveYouAlertedOPT) {
        this.haveYouAlertedOPT = haveYouAlertedOPT;
    }

    public String getInformedOn() {
        return informedOn;
    }

    public void setInformedOn(String informedOn) {
        this.informedOn = informedOn;
    }

    public String getIncidentNumber() {
        return incidentNumber;
    }

    public void setIncidentNumber(String incidentNumber) {
        this.incidentNumber = incidentNumber;
    }

    public String getLastUpdatedOn() {
        return lastUpdatedOn;
    }

    public void setLastUpdatedOn(String lastUpdatedOn) {
        this.lastUpdatedOn = lastUpdatedOn;
    }



    public String getSchoolAgeOrPreK() {
        return schoolAgeOrPreK;
    }

    public void setSchoolAgeOrPreK(String schoolAgeOrPreK) {
        this.schoolAgeOrPreK = schoolAgeOrPreK;
    }


    public static BusRide parse(String line) throws ParseException {
        BusRide br = new BusRide();

        String[] tokens = line.split(";");


        if (!tokens[1].isEmpty()){
            //Aggiungo la Reason
            br.busbreakdownID = tokens[1];
        }


        if (!tokens[5].isEmpty()){
            //Aggiungo la Reason
            br.reason = mappingReason(tokens[5]);
        }

        //Controllo la validità e aggiungo il valore della varibile Occurred_On (campo 7)
        if (!tokens[7].isEmpty()){
            br.occurredOn =  tokens[7];
        }

        //Controllo la validità e aggiungo il valore della varibile Boro (campo 9)
        if (!tokens[9].isEmpty()){
            br.boro = tokens[9];
        }

        if (!tokens[10].isEmpty()){
            br.busCompanyName = tokens[10];
        }


        //Controllo la validità e aggiungo il valore della varibile How_Long_Delayed (campo 11)
        if (!tokens[11].isEmpty()){
            br.howLongDelayed = String.valueOf(minutesDelayed(tokens[11]));
        } else {
            br.howLongDelayed = String.valueOf(0);
        }


        return br;
    }


    private static String mappingReason(String reason) {
        switch (reason) {
            case "Accident":
                return "Accident";
            case "Delayed by School":
                return "Delayed by School";
            case "Flat Tire":
                return "Flat Tire";
            case "Heavy Traffic":
                return "Heavy Traffic";
            case "Mechanical Problem":
                return "Mechanical Problem";
            case "Problem Run":
                return "Problem Run";
            case "Weather Condition":
                return "Weather Condition";
            case "Won't Start":
                return "Won't Start";
            default:
                return "Other";
        }
    }


    public DateTime getDateTime(){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        return formatter.parseDateTime(occurredOn);
    }


    @Override
    public String toString() {
        return "Bus{" +
                "reason=" + reason +
                ", occurredOn='" + occurredOn + '\'' +
                ", boro='" + boro + '\'' +
                ", busCompanyName='" + busCompanyName + '\'' +
                ", howLongDelayed='" + howLongDelayed + '\'' +
                '}';
    }
}
