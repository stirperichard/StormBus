package com.stirperichard.stormbus.utils;

import java.util.Comparator;

public class RankItemComparator implements Comparator<RankItem> {

	@Override
	public int compare(RankItem o1, RankItem o2) {
		
		if (o1.getFrequency() == o2.getFrequency() 
				&& !o1.getRoute().equals(o2.getRoute())){
			return - (int) (o1.getTimestamp() - o2.getTimestamp());
		}
		
		return -(o1.getFrequency() - o2.getFrequency());
		
	}

}
