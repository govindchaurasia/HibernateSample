package com.interactcrm.qstats.qm;

import java.util.HashMap;
import java.util.Map;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.util.logging.LogHelper;

/**
 * QueueManagerMap stores all the Queue Managers.
 * Class Name : com.interactcrm.im.qm.QueueManagerMap
 * Project Name: InteractionManagerMM
 * @author Vandana T. Joshi
 * @version 1.0
 * @since 1.0
 */
public class QueueManagerMap {
	private Map<Integer,QueueManager> _qmMap = new HashMap<Integer,QueueManager>();
	private static QueueManagerMap _mapObj = new QueueManagerMap();
	
	private QueueManagerMap(){}
	
	public static QueueManagerMap getInstance(){
		return _mapObj;
	}
	
	/**
	 * Adds specified Queue Manager to the Map.
	 * @param key : Pkey of Queue Manager
	 * @param queueMgr : Queue Manager object
	 */
	public synchronized void addQM(int key, QueueManager queueMgr) {
		if (!_qmMap.containsKey(key)) {
			_qmMap.put(new Integer(key), queueMgr);
		} else {
			System.out.println("addQM:: QM with key = " + key
					+ " already exists.");

		}
	}
	
	public boolean containsKey(int key){
		return _qmMap.containsKey(key);
	}
	/**
	 * @param key : Pkey of Queue Manager
	 * @return Queue Manager object
	 */
	public synchronized QueueManager getQueueManager(int key) {
    	return _qmMap.containsKey(key) ? (QueueManager)_qmMap.get(key) : null;    	 	
    }

	public  Map<Integer,QueueManager> getQMMap(){
		return _qmMap;
	}
}
