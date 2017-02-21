package com.interactcrm.qstats.bean;

public class CallbackBean {
	
	int pendingCount=0;
	int scheduleCount=0;
	int type=0;
	
	
	public CallbackBean()
	{
		
	}
	
	public CallbackBean(int pendingCount, int scheduleCount, int type) {

		this.pendingCount = pendingCount;
		this.scheduleCount = scheduleCount;
		this.type = type;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}


	public int getPendingCount() {
		return pendingCount;
	}
	public void setPendingCount(int pendingCount) {
		this.pendingCount = pendingCount;
	}
	public int getScheduleCount() {
		return scheduleCount;
	}
	public void setScheduleCount(int scheduleCount) {
		this.scheduleCount = scheduleCount;
	}
	
	
}
