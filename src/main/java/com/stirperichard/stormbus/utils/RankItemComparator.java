package com.stirperichard.stormbus.utils;

import java.util.Comparator;

public class RankItemComparator implements Comparator<it.uniroma2.debs2015gc.utils.RankItem> {

	@Override
	public int compare(it.uniroma2.debs2015gc.utils.RankItem o1, it.uniroma2.debs2015gc.utils.RankItem o2) {
		
		if (o1.getFrequency() == o2.getFrequency() 
				&& !o1.getRoute().equals(o2.getRoute())){
			return - (int) (o1.getTimestamp() - o2.getTimestamp());
		}
		
		return -(o1.getFrequency() - o2.getFrequency());
		
	}

}
