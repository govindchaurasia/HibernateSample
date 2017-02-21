package com.interactcrm.qstats.util;

import org.json.JSONException;
import org.json.JSONObject;

import com.interactcrm.qstats.bean.QueryBean;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.bean.QueueQueueGroupMapping;
import com.interactcrm.qstats.startup.TenantGroupStore;

public enum QueryUtil {
	DEFAULT{

		@Override
		public String getVersion(String qString, JSONObject jsonData,
				String val) throws NumberFormatException,
				JSONException {
			return qString;
		}
		
	},
	GETQUEUEDETAILS {
		@Override
		public String getVersion(String qString,JSONObject jsonData,String val) throws NumberFormatException, JSONException {
			if (qString.contains("queueGroupId")) {
				int queueGroupId = QueueQueueGroupMapping.getInstance()
						.getQueueGroupId(Integer.parseInt(jsonData.getString(val)));
				qString = qString.replaceAll("##queueGroupId##", "" + queueGroupId);
				if (qString.contains("version")) {
					long version = QueueGroupStore.getInstance().getVersionFromMap(queueGroupId);
					qString = qString.replaceAll("##version##", "" + version);
				}
				
			}
			return qString;
		}
	},
	GETPENDINGCALLBACKSBYTENANTID{

		@Override
		public String getVersion(String qString,JSONObject jsonData,String val) throws NumberFormatException, JSONException {
			if (qString.contains("callback")) {
				if (qString.contains("version")) {
					
					long version = TenantGroupStore.getInstance().getVersionFromMap(Integer.parseInt(jsonData.getString(val)));
					qString = qString.replaceAll("##version##", "" + version);
					
				}
			}
			return qString;
		}
		
	};
	
	public abstract String getVersion(String qString,JSONObject jsonData,String val) throws NumberFormatException, JSONException;
	
	public static QueryUtil getQueryUtil(String queryName) {
		for(QueryUtil queries : QueryUtil.values())
		{
			if(queries.toString().equals(queryName.toUpperCase()))
			{
				return QueryUtil.valueOf(queryName.toUpperCase());
			}
			
		}
		
		return DEFAULT;
	//	return ((queryName != null)  && (!queryName.isEmpty()))? QueryUtil.valueOf(queryName.toUpperCase()) : DEFAULT;
		
	}
}
