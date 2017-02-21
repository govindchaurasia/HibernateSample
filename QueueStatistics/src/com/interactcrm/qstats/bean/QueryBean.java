package com.interactcrm.qstats.bean;

import java.util.LinkedList;

public class QueryBean {
	
	private String queryString	=	"";
	
	private LinkedList<String> parameterList = new LinkedList<String>();

	/**
	 * @param queryString
	 * @param parameterList
	 */
	public QueryBean(String queryString, LinkedList<String> parameterList) {
		super();
		this.queryString = queryString;
		this.parameterList = parameterList;
	}

	/**
	 * @return the queryString
	 */
	public String getQueryString() {
		return queryString;
	}

	/**
	 * @param queryString the queryString to set
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	/**
	 * @return the parameterList
	 */
	public LinkedList<String> getParameterList() {
		return parameterList;
	}

	/**
	 * @param parameterList the parameterList to set
	 */
	public void setParameterList(LinkedList<String> parameterList) {
		this.parameterList = parameterList;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "QueryBean [queryString=" + queryString + ", parameterList="
				+ parameterList + "]";
	}


}
