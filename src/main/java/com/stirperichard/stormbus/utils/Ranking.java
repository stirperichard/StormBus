package com.stirperichard.stormbus.utils;

import java.io.Serializable;
import java.util.List;

public class Ranking implements Serializable {

	private static final long serialVersionUID = 1L;

	List<RankItemQ2> ranking;
	
	public Ranking() {
	
	}

	public List<RankItemQ2> getRanking() {
		return ranking;
	}

	public void setRanking(List<RankItemQ2> ranking) {
		this.ranking = ranking;
	}
	
}
