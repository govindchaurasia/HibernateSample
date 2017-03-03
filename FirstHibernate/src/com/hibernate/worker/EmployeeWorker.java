package com.hibernate.worker;


import java.util.Date;

import org.hibernate.Session; 
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import com.hibernate.sample.Address;
import com.hibernate.sample.EmployeeDetails;

public class EmployeeWorker {

	public static void main(String[] args) {
		
		EmployeeDetails emp= new EmployeeDetails();
		emp.setName("Govind");
		emp.setContacts("84145454");
		emp.setCurrentDate(new Date());
		emp.setDescription("Anything entered here");
		
		Address address1= new Address();
		address1.setCity("Mumbai");
		address1.setState("Maharashtra");
		address1.setPincode(111014);
		
		Address address2= new Address();
		address2.setCity("Mumbai");
		address2.setState("Maharashtra");
		address2.setPincode(111056);
		
		emp.getListOfAddress().add(address1);
		emp.getListOfAddress().add(address2);
		
		try
		{
		SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();
		
		Session session=sessionFactory.openSession();
		session.beginTransaction();
		session.save(emp);
		session.getTransaction().commit();
		session.close();
		/*
		emp=null;
		
		session=sessionFactory.openSession();
		session.beginTransaction();
		emp=(EmployeeDetails)session.get(EmployeeDetails.class, 1);
		Sytem.out.println("Data fetched "+emp.getName());*/
		
		
		} 
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
