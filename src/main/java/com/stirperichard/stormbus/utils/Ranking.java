package com.stirperichard.stormbus.utils;

import java.io.Serializable;
import java.util.List;

public class Ranking implements Serializable {

	private static final long serialVersionUID = 1L;

	List<com.stirperichard.stormbus.utils.RankItem> ranking;
	
	public Ranking() {
	
	}

	public List<com.stirperichard.stormbus.utils.RankItem> getRanking() {
		return ranking;
	}

	public void setRanking(List<com.stirperichard.stormbus.utils.RankItem> ranking) {
		this.ranking = ranking;
	}
	
}
