package com.stirperichard.stormbus.entity;

import java.io.Serializable;

public class Data implements Serializable {
    private String date;
    private String time;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Data{" +
                "date='" + date + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
