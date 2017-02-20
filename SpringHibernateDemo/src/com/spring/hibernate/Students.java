package com.spring.hibernate;

import java.util.List;

public class Students {
	
	private String name;
	private int age;
	private String address;
	private Admission admission;
	private List<String> list;
    private List<Admission> listAdmission;

	public List<Admission> getListAdmission() {
		return listAdmission;
	}


	public void setListAdmission(List<Admission> listAdmission) {
		this.listAdmission = listAdmission;
	}


	public List<String> getList() {
		return list;
	}


	public void setList(List<String> list) {
		this.list = list;
	}


	public Admission getAdmission() {
		return admission;
	}


	public void setAdmission(Admission admission) {
		this.admission = admission;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public int getAge() {
		return age;
	}


	public void setAge(int age) {
		this.age = age;
	}


	public String getAddress() {
		return address;
	}


	public void setAddress(String address) {
		this.address = address;
	}


	public void getInfo()
	{
		System.out.println("Name : "+getName()+", Age : "+getAge()+", Address : "+getAddress()+", Course : "+getAdmission().getCourse()+", Fees : "+getAdmission().getFees());
		for (Admission admission : listAdmission) {
			System.out.println("List of Admission : "+admission.getCourse());
		}
		
		for (String s : list) {
			System.out.println("List of courses : "+s);
		}
	}
	
}
