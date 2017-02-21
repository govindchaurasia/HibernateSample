package com.interactcrm.qstats.bean;

import java.util.LinkedHashMap;
import java.util.Map;

public class ChannelProperties {
	private Map<String, String> map = new LinkedHashMap<String, String>(10);

	@Override
	public String toString() {
		return "ChannelProperties [map=" + map + "]";
	}

	public ChannelProperties(Map<String, String> map) {
		this.map = map;
	}

	public String getProperty(String key) {
		if (map.isEmpty()) {
			throw new IllegalStateException("Properties are not initlized");
		}
		String val = map.get(key);
		if(val.equalsIgnoreCase("null")||val==null){
			val="10";
		}
		return val;
	}

}