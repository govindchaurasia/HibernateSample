package com.hibernate.sample;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

@Entity (name="Employee_details")
// @Table (name="Employee")
public class EmployeeDetails {
	@Id @GeneratedValue (strategy=GenerationType.AUTO)
	private int eid;
	@Column (name="employeeName")
	private String name;
	private String contacts;
//	@Temporal (TemporalType.DATE) -- Enters only date in currentdate field
//	@Transient  -- Skip this fields
	private Date currentDate;
//	@Lob -- If large amount of data is stored i.e cross the limit of 255 character which is by default 
	private String description;
	
	@ElementCollection
	@JoinTable(name="address",
			joinColumns=@JoinColumn(name="eid"))
	private Set<Address> listOfAddress = new HashSet<Address>(); 
			
	
	public Set<Address> getListOfAddress() {
		return listOfAddress;
	}
	public void setListOfAddress(Set<Address> listOfAddress) {
		this.listOfAddress = listOfAddress;
	}
	public Date getCurrentDate() {
		return currentDate;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public void setCurrentDate(Date currentDate) {
		this.currentDate = currentDate;
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
