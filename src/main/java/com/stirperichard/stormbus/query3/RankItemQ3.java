package com.stirperichard.stormbus.query3;

import java.io.Serializable;

public class RankItemQ3 implements Serializable {

	private static final long serialVersionUID = 1L;

	private String busCompanyName;
	private double score;
	
	public RankItemQ3() {
	}
	
	public RankItemQ3(String busCompanyName, double score) {
		super();
		this.busCompanyName = busCompanyName;
		this.score = score;
	}

	public String getBusCompanyName() {
		return busCompanyName;
	}

	public void setBusCompanyName(String busCompanyName) {
		this.busCompanyName = busCompanyName;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public boolean equals(Object obj) {
	
		if (obj == null || !(obj instanceof RankItemQ3))
			return false;
		
		RankItemQ3 other = (RankItemQ3) obj;
		
		if (this.busCompanyName.equals(other.busCompanyName))
			return true;
		
		return false;
	}
	
	@Override
	public String toString() {
		return busCompanyName + ":" + String.valueOf(score);
	}
}
