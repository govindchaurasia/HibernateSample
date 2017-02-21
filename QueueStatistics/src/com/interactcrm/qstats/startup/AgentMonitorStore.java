package com.interactcrm.qstats.startup;

import java.util.HashMap;
import java.util.Map;

import com.interactcrm.mm.beans.TenantGroup;

/**
 * This class is a storage for storing Agent Statistics data per {@link TenantGroup}.  
 * This data is retrieved from QM on periodic basis and stored against Tenant Group pkey.
 *  * @author Vandana T. Joshi
 * @version 1.0
 * @since 1.0
 */
public class AgentMonitorStore {
	private static AgentMonitorStore detailStore = new AgentMonitorStore();
	private Map<Integer, String> map = new HashMap<Integer, String>();

	private AgentMonitorStore() {
	}

	public static AgentMonitorStore getInstance() {
		return detailStore;
	}

	public synchronized void addAgentMonitoreData(int tenantGrpId, String agentList) {
		map.put(new Integer(tenantGrpId), agentList);
	}

	public String getAgentMonitoreData(int tenantGrpId) {
		Integer key = new Integer(tenantGrpId);
		return map.get(key);
	}


	@Override
	public String toString() {
		return "AgentMonitorStore [map=" + map.toString() + "]";
	}
}
