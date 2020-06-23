package com.stirperichard.stormbus.query3;

import java.io.Serializable;
import java.util.List;

public class RankingQ3 implements Serializable {

	private static final long serialVersionUID = 1L;

	List<RankItemQ3> ranking;
	
	public RankingQ3() {
	
	}

	public List<RankItemQ3> getRanking() {
		return ranking;
	}

	public void setRanking(List<RankItemQ3> ranking) {
		this.ranking = ranking;
	}
	
}
