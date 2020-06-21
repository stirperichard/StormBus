package com.stirperichard.stormbus.entity;


public class ReasonsCount {

    private int HEAVY_TRAFFIC;
    private int MECHANICAL_PROBLEM;
    private int OTHER;

    public ReasonsCount(int HEAVY_TRAFFIC, int MECHANICAL_PROBLEM, int OTHER) {
        this.HEAVY_TRAFFIC = HEAVY_TRAFFIC;
        this.MECHANICAL_PROBLEM = MECHANICAL_PROBLEM;
        this.OTHER = OTHER;
    }

    public int getHEAVY_TRAFFIC() {
        return HEAVY_TRAFFIC;
    }

    public void setHEAVY_TRAFFIC(int HEAVY_TRAFFIC) {
        this.HEAVY_TRAFFIC = HEAVY_TRAFFIC;
    }

    public int getMECHANICAL_PROBLEM() {
        return MECHANICAL_PROBLEM;
    }

    public void setMECHANICAL_PROBLEM(int MECHANICAL_PROBLEM) {
        this.MECHANICAL_PROBLEM = MECHANICAL_PROBLEM;
    }

    public int getOTHER() {
        return OTHER;
    }

    public void setOTHER(int OTHER) {
        this.OTHER = OTHER;
    }


    public ReasonsCount subtract(ReasonsCount o2){
        HEAVY_TRAFFIC = HEAVY_TRAFFIC - o2.HEAVY_TRAFFIC;
        MECHANICAL_PROBLEM = MECHANICAL_PROBLEM - o2.MECHANICAL_PROBLEM;
        OTHER = OTHER - o2.OTHER;
        return this;
    }

    public ReasonsCount sum(ReasonsCount o2){
        HEAVY_TRAFFIC = HEAVY_TRAFFIC + o2.HEAVY_TRAFFIC;
        MECHANICAL_PROBLEM = MECHANICAL_PROBLEM + o2.MECHANICAL_PROBLEM;
        OTHER = OTHER + o2.OTHER;
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReasonsCount that = (ReasonsCount) o;
        return HEAVY_TRAFFIC == that.HEAVY_TRAFFIC &&
                MECHANICAL_PROBLEM == that.MECHANICAL_PROBLEM &&
                OTHER == that.OTHER;
    }


}
