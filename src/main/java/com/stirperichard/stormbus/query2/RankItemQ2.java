package com.stirperichard.stormbus.query2;

import java.io.Serializable;

public class RankItemQ2 implements Serializable{

	private static final long serialVersionUID = 1L;

	private String reason;
	private int frequency;
	private long basetime;
	
	public RankItemQ2() {
	}
	
	public RankItemQ2(String reason, int frequency, long basetime) {
		super();
		this.reason = reason;
		this.frequency = frequency;
		this.basetime = basetime;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	} 
	
	public long getBasetime() {
		return basetime;
	}

	public void setBasetime(long basetime) {
		this.basetime = basetime;
	}

	@Override
	public boolean equals(Object obj) {
	
		if (obj == null || !(obj instanceof RankItemQ2))
			return false;

		RankItemQ2 other = (RankItemQ2) obj;
		
		if (this.reason.equals(other.reason))
			return true;
		
		return false;
	}
	
	@Override
	public String toString() {
		return reason + ":" + frequency;
	}
}
