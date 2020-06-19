package com.stirperichard.stormbus.utils;

import com.stirperichard.stormbus.entity.ReasonsCount;

import java.util.ArrayList;
import java.util.List;

public class WindowQ3 {

	List<ReasonsCount> timeframes;
	int currentIndex;
	int size;
	ReasonsCount estimatedTotal;

	public WindowQ3(int size) {

		this.timeframes = new ArrayList<>();
		this.size = size; 
		this.currentIndex = 0;
		estimatedTotal = new ReasonsCount(0, 0, 0);


		for (int i = 0; i < size; i++) {
			ReasonsCount estimatedTotal = new ReasonsCount(0, 0, 0);
			timeframes.add(estimatedTotal);
		}
		
	}

	public ReasonsCount moveForward(){

		/* Free the timeframe that is going to go out of the window */
		int lastTimeframeIndex = (currentIndex + 1) % size;

		ReasonsCount value = timeframes.get(lastTimeframeIndex);
		//timeframes[lastTimeframeIndex] = 0;
		
		estimatedTotal.subtract(value);
		
		/* Move forward the current index */
		currentIndex = (currentIndex + 1) % size;
		
		return value;
		
	}	

	public ReasonsCount moveForward(int positions){

		ReasonsCount cumulativeValue = new ReasonsCount(0, 0, 0);
		
		for (int i = 0; i < positions; i++){
			cumulativeValue.sum(moveForward());
		}
		
		return cumulativeValue;
	}
	

	public void increment(ReasonsCount value){
		
		timeframes.set(currentIndex, timeframes.get(currentIndex).sum(value));
		
		estimatedTotal.sum(value);
		
	}

	@Override
	public String toString() {
	
		String s = "[";
		
		for (int i = 0; i < timeframes.size(); i++){
			
			s += timeframes.get(i);
			
			if (i < (timeframes.size() - 1))
				s += ", ";
			
		}
		
		s += "]";
		return s;
	
	}
	
	public ReasonsCount getEstimatedTotal() {
		return estimatedTotal;
	}
	
	public ReasonsCount computeTotal(){
		
		ReasonsCount total = new ReasonsCount(0, 0, 0);
		for (int i = 0; i < timeframes.size(); i++)
			total.sum(timeframes.get(i));
		return total;
		
	}
}
