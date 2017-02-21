package com.interactcrm.qstats.startup;

import java.util.HashMap;
import java.util.Map;


/**
 * This class stores mapping of Queue Manager and Tenant Groups.
 * One Map here stored TenantGroup pkey as key and QM pkey as value.
 * Second map stores : (QM against tenantGrpName)
 * @author Vipin Singh
 * @version 1.0
 * @since 1.0
 */
public class QMTenantGroupStore {
	private Map<Integer,Integer> qmTGMap = new HashMap<Integer,Integer>();
	private static QMTenantGroupStore qmStoreObj = new QMTenantGroupStore();
	
	private Map<Integer,String> tgDetailsMap	=	new HashMap<Integer,String>();	
	public static QMTenantGroupStore getInstance(){
		return qmStoreObj;
	}
	
	public void addQMTGMapping(int tenantGrpKey, int qmPkey){
		qmTGMap.put(new Integer(tenantGrpKey), new Integer(qmPkey));
	}
	
	public int getQMPkey(int tgPkey){
		Integer value = qmTGMap.get(new Integer(tgPkey));
		return value.intValue();
	}

	public  Map<Integer,Integer> getQMTGMap(){
		return qmTGMap;
	}
	
	public Map<Integer,String> getTGDetails(){
		return tgDetailsMap;
	}
	
	public String getTGDetailsByTGPkey(int tgPkey){
		String tgName = tgDetailsMap.get(new Integer(tgPkey));
		return tgName;
	}
	
	@Override
	public String toString() {
		return "QMTenantGroupStore [qmTGMap=" + qmTGMap.toString() + "]";
	}

	public void addTenantGroupDetails(int tgPkey, String tgName) {
		tgDetailsMap.put(tgPkey, tgName);
		
	}
	
}
