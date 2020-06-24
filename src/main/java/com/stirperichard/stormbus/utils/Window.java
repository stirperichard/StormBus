package com.stirperichard.stormbus.utils;

public class Window {

	int[] timeframes; 
	int currentIndex; 
	int size; 
	int estimatedTotal;
	int counter; //Conta il numero di eventi delay
	
	public Window(int size) {
		
		this.timeframes = new int[size];
		this.size = size; 
		this.currentIndex = 0;
		this.estimatedTotal = 0;
		this.counter = 0;
		
		for (int i = 0; i < size; i++)
			timeframes[i] = 0;
		
	}

	public int moveForward(){

		/* Free the timeframe that is going to go out of the window */
		int lastTimeframeIndex = (currentIndex + 1) % size;
		
		int value = timeframes[lastTimeframeIndex];
		timeframes[lastTimeframeIndex] = 0;
		
		estimatedTotal -= value;
		
		/* Move forward the current index */
		currentIndex = (currentIndex + 1) % size;

		counter = 0;

		return value;
		
	}	

	public int moveForward(int positions){

		int cumulativeValue = 0;
		
		for (int i = 0; i < positions; i++){
			cumulativeValue += moveForward();
		}
		
		return cumulativeValue;
	}
	
	public void increment(){
		increment(1);
	}
	
	public void increment(int value){
		
		timeframes[currentIndex] = timeframes[currentIndex] + value;
		
		estimatedTotal += value;

		counter++;
		
	}

	@Override
	public String toString() {
	
		String s = "[";
		
		for (int i = 0; i < timeframes.length; i++){
			
			s += timeframes[i];
			
			if (i < (timeframes.length - 1))
				s += ", ";
		}
		
		s += "]";
		return s;
	
	}
	
	public int getEstimatedTotal() {
		return estimatedTotal;
	}

	public int getCounter() {
		return counter;
	}

	public int computeTotal(){
		
		int total = 0;
		int i;
		for ( i = 0; i < timeframes.length; i++)
			total += timeframes[i];
		return total;
		
	}
}
