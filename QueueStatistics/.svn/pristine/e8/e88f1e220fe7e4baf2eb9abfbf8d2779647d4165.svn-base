package com.interactcrm.qstats.bean;
import java.util.HashMap;
import java.util.Map;


public class QueueQueueGroupMapping {

	
	private static QueueQueueGroupMapping instance = new QueueQueueGroupMapping();
	
	 private QueueQueueGroupMapping() {		
	
	}
	
	public static QueueQueueGroupMapping getInstance(){
		return instance;
	}	
	
	private Map<Integer, Integer> queueQueueGroup = new HashMap<Integer, Integer>();
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("QueueQueueGroupMapping [").append(queueQueueGroup).append("]");
		return builder.toString();
	}

	public void addEntry(Integer queue , int queueGroup){
		queueQueueGroup.put(queue, queueGroup);
	}
	public boolean containsKey(int queue){
		return queueQueueGroup.containsKey(queue);
	}
	
	public Integer getQueueGroupId(Integer queue){
		return queueQueueGroup.containsKey(queue) ? (Integer) queueQueueGroup.get(queue) : 0;  
		//return queueQueueGroup.get(queue);
	}
	
	public Map<Integer, Integer> getQueueQueueGroupMap(){
		return queueQueueGroup;  
		//return queueQueueGroup.get(queue);
	}
}
	