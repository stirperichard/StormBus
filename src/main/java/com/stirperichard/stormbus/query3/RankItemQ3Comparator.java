package com.stirperichard.stormbus.query3;

import java.util.Comparator;

public class RankItemQ3Comparator implements Comparator<RankItemQ3> {

	@Override
	public int compare(RankItemQ3 o1, RankItemQ3 o2) {
		return - Double.compare(o1.getScore(), o2.getScore());
	}

}
