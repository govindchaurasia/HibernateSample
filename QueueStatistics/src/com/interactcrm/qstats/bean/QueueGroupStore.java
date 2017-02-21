package com.interactcrm.qstats.bean;

import java.util.HashMap;
import java.util.Map;

public class QueueGroupStore {
	
		private static class SingletonHelper {
		private static final QueueGroupStore INSTANCE = new QueueGroupStore();
	}

	public static QueueGroupStore getInstance() {
		return SingletonHelper.INSTANCE;
	}

	private Map<Integer,Long> versionMap = new HashMap<>();
    
	
	public Map<Integer, Long> getVersionMap() {
		return versionMap;
	}


	public void setVersion(int queueGroupId,long version)
	{
			versionMap.put(queueGroupId, version);
	}
	
	public Long getVersionFromMap(int queueGroupId)
	{
		 return versionMap.get(queueGroupId);
	}
	
	
	
	
	
}
