package com.stirperichard.stormbus.query2;

import com.stirperichard.stormbus.query2.RankItemQ2;

import java.util.Comparator;

public class RankItemComparatorQ2 implements Comparator<RankItemQ2> {

	@Override
	public int compare(RankItemQ2 o1, RankItemQ2 o2) {
		
		if (o1.getFrequency() == o2.getFrequency() 
				&& !o1.getReason().equals(o2.getReason())){
			return - (int) (o1.getBasetime() - o2.getBasetime());
		}
		
		return -(o1.getFrequency() - o2.getFrequency());
		
	}

}
