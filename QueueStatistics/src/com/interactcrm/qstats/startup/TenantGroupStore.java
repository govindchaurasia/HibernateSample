package com.interactcrm.qstats.startup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.interactcrm.qstats.dao.QueueStatsDAO;

public class TenantGroupStore {
	
	
	private static List<Integer> tenantGroupList	=	null;
	
	private static TenantGroupStore tgStore	=	new TenantGroupStore(); 
	private static Map<Integer,TenantGroupObj> tgIdTenangGrpObjMap	=	new HashMap<Integer,TenantGroupObj>();
	public static TenantGroupStore getInstance(){
		return tgStore;
	}
	
	private Map<Integer, Integer> tenantTGMap=new HashMap<Integer,Integer>();
	
	public void putToTgIdTenangGrpMap (int tgId,TenantGroupObj tgObj){
		tgIdTenangGrpObjMap.put(tgId, tgObj)	;
	}
	public TenantGroupObj getTenantGroupObjFromMap(int tenantId){
		
		return tgIdTenangGrpObjMap.get(tenantId);
			
	}
	
	public Map<Integer,TenantGroupObj> getTgObjectMap(){
		return tgIdTenangGrpObjMap;
	}

	public void prepareTgObjMap(){
		
		for(Integer tg: tenantGroupList){
			
			tgIdTenangGrpObjMap.put(tg,new TenantGroupObj());
		}
	}
	
	public List<Integer> getTenantGrpList(){
		
		return tenantGroupList;
	}
	
	public void createTenantGrpList(){
		tenantGroupList	=	new QueueStatsDAO().getTenantGroupList();
	}
	
	
	public void addEntry(int tenant , int tgId){
		tenantTGMap.put(tenant, tgId);
	}
	public boolean containsKey(int tenant){
		return tenantTGMap.containsKey(tenant);
	}

	public Map<Integer, Integer> getTenantTGMap(){
		return tenantTGMap;  
	}
	
	private Map<Integer,Long> versionMap = new HashMap<>();

	public Map<Integer, Long> getVersionMap() {
		return versionMap;
	}


	public void setVersion(int tenantId,long version)
	{
			versionMap.put(tenantId, version);
	}
	
	public Long getVersionFromMap(int tenantId)
	{
		 return versionMap.get(tenantId);
	}
}
