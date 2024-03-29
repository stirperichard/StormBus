package com.stirperichard.stormbus.query2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopKRankingQ2 {

	private Comparator<RankItemQ2> comparator = null;
	private List<RankItemQ2> ranking = null;
	private int topK = 10;

	private static final int NOT_PRESENT = -1;

	public TopKRankingQ2(int k) {
		this.comparator = new RankItemComparatorQ2();
		this.ranking = new ArrayList<RankItemQ2>();
		this.topK = k;
	}

	/**
	 * Update or insert a new RankItem.
	 * 
	 * Returns if the update changed the order of the first observed items.
	 * 
	 * @param item
	 * @return
	 */
	public boolean update(RankItemQ2 item) {

		int sizePreUpdate = ranking.size();
		int oldPosition = findIndex(item);

		if (oldPosition != NOT_PRESENT) {
			ranking.remove(item);
		}

		int newPosition = add(item);

		int sizePostUpdate = ranking.size();
		
		if (newPosition == oldPosition && 
				sizePreUpdate == sizePostUpdate) {
			
			/* do not notify position changed */
			return false;
			
		} else if (newPosition > topK - 1) {
			
			/* do not notify position changed in the lower side of the ranking */
			return false;
		}

		return true;

	}

	public int add(RankItemQ2 item) {

		int insertionPoint = Collections.binarySearch(ranking, item, comparator);
		ranking.add((insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1, item);
		insertionPoint = (insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1;
		return insertionPoint;

	}
	
	public void remove(RankItemQ2 item){
		
		ranking.remove(item);
		
	}

	private int findIndex(RankItemQ2 item) {

		for (int i = 0; i < ranking.size(); i++) {
			if (item.equals(ranking.get(i)))
				return i;
		}

		return NOT_PRESENT;

	}

	public boolean containsElement(RankItemQ2 item) {

		return (findIndex(item) != NOT_PRESENT);
	
	}
	
	public RankingQ2 getTopK(){
		
		List<RankItemQ2> top = new ArrayList<RankItemQ2>();
		
		if (ranking.isEmpty()){
			RankingQ2 topKRankingQ2 = new RankingQ2();
			topKRankingQ2.setRanking(top);
			return topKRankingQ2;
		}
		
		int elems = Math.min(topK, ranking.size());
		
		for (int i = 0; i < elems; i++){
			top.add(ranking.get(i));
		}
		
		RankingQ2 topKRankingQ2 = new RankingQ2();
		topKRankingQ2.setRanking(top);
		return topKRankingQ2;
		
	}
	
	@Override
	public String toString() {
		return ranking.toString();
	}

}