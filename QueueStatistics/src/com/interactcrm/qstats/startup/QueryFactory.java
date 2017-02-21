package com.interactcrm.qstats.startup;

import java.util.HashMap;
import java.util.Map;

import com.interactcrm.qstats.bean.QueryBean;

public class QueryFactory {

	private static QueryFactory queryFactory 		=	new QueryFactory();
	private static Map<String,QueryBean> queryMap 	=	new HashMap<String,QueryBean>();
	
	public static  QueryFactory getInstance(){
		return queryFactory;
	}
	
	public QueryBean getquery(String queryName){
		return queryMap.get(queryName);
	}
	
	public void putQuery(String query,QueryBean qbean){
		queryMap.put(query, qbean);
	}
	
}
