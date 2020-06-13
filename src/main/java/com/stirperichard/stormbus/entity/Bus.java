package com.stirperichard.stormbus.entity;

import com.stirperichard.stormbus.enums.BreakdownOrRunningLate;
import com.stirperichard.stormbus.enums.Reason;

import java.io.Serializable;

public class Bus implements Serializable {

    private String schoolYear;
    private int busbreakdownID;
    private String runType;
    private String busNo;
    private String routeNumber;
    private Reason reason;
    private String schoolsServiced;
    private String occurredOn;
    private String createdOn;
    private String boro;
    private String busCompanyName;
    private String howLongDelayed;
    private int numerOfStudentsOnTheBus;
    private String hasContractorNotifiedSchool;
    private String hasContractorNotifiedParents;
    private String haveYouAlertedOPT;
    private String informedOn;
    private String incidentNumber;
    private String lastUpdatedOn;
    private BreakdownOrRunningLate breakdownOrRunningLate;
    private String schoolAgeOrPreK;

    public String getSchoolYear() {
        return schoolYear;
    }

    public void setSchoolYear(String schoolYear) {
        this.schoolYear = schoolYear;
    }

    public int getBusbreakdownID() {
        return busbreakdownID;
    }

    public void setBusbreakdownID(int busbreakdownID) {
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

    public Reason getReason() {
        return reason;
    }

    public void setReason(Reason reason) {
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

    public BreakdownOrRunningLate getBreakdownOrRunningLate() {
        return breakdownOrRunningLate;
    }

    public void setBreakdownOrRunningLate(BreakdownOrRunningLate breakdownOrRunningLate) {
        this.breakdownOrRunningLate = breakdownOrRunningLate;
    }

    public String getSchoolAgeOrPreK() {
        return schoolAgeOrPreK;
    }

    public void setSchoolAgeOrPreK(String schoolAgeOrPreK) {
        this.schoolAgeOrPreK = schoolAgeOrPreK;
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
