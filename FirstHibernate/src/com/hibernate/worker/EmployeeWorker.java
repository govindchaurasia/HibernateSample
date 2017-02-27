package com.hibernate.worker;

import org.hibernate.HibernateException; 
import org.hibernate.Session; 
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import com.hibernate.sample.EmployeeDetails;

public class EmployeeWorker {

	public static void main(String[] args) {
		
		EmployeeDetails emp= new EmployeeDetails();
		emp.setEid(3);
		emp.setName("Govind");
		emp.setContacts("84145454");
		
		try
		{
			System.out.println("		SessionFactory "+new Configuration().configure().buildSessionFactory());
		SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();
		
		Session session=sessionFactory.openSession();
		session.beginTransaction();
		session.save(emp);
		session.getTransaction().commit();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
