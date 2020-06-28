package com.stirperichard.stormbus.query2;

import com.stirperichard.stormbus.query2.RankItemQ2;

import java.io.Serializable;
import java.util.List;

public class RankingQ2 implements Serializable {

	private static final long serialVersionUID = 1L;

	List<RankItemQ2> ranking;
	
	public RankingQ2() {
	
	}

	public List<RankItemQ2> getRanking() {
		return ranking;
	}

	public void setRanking(List<RankItemQ2> ranking) {
		this.ranking = ranking;
	}
	
}
