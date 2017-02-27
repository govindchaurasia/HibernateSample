package com.hibernate.sample;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class EmployeeDetails {
	@Id
	private int eid;
	private String name;
	private String contacts;
	
	public EmployeeDetails() {
		
	}
	public EmployeeDetails(int eid, String name, String contacts) {
		this.eid = eid;
		this.name = name;
		this.contacts = contacts;
	}
	
	public int getEid() {
		return eid;
	}
	public void setEid(int eid) {
		this.eid = eid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getContacts() {
		return contacts;
	}
	public void setContacts(String contacts) {
		this.contacts = contacts;
	}

	@Override
	public String toString() {
		return "EmployeeDetails [eid=" + eid + ", name=" + name + ", contacts=" + contacts + "]";
	}
	
}
