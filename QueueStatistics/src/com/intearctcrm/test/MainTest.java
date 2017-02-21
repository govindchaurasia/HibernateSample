package com.intearctcrm.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


import com.interactcrm.dbcp.ConnectionPool;
import com.interactcrm.dbcp.ConnectionPoolManager;
import com.interactcrm.qstats.threads.QueueStatsUpdater;




final public class MainTest
{
	MainTest()
	{
		
	}
	
    public static void main(String...args)
    {
    	
     System.out.println(getFormattedQueueTime(-86895));	

    	/*try{
	    	ResourceBundle rb = ResourceBundle.getBundle("data");
	        rb.getKeys();
	        Enumeration<String> keys = rb.getKeys();

	        while (keys.hasMoreElements()) {
	            String key 		= 	(String)keys.nextElement();
	            String value	=	rb.getString(key);
	            
	            if(key.equalsIgnoreCase("defaultRole")){
	            	System.out.println("----default---"+rb.getString(key));
	            }else{
	            	System.out.println(key+"----------------"+value);
	            }	                     
	        }
	       
	        //System.out.println("role"+role);
    	}catch(Exception e ){
    		 System.out.println("role"+e);
    	}*/
    	
		/*try {
            AbsFixedSaltPasswordEncryptor passwordEncryptor;
            String loginId = "icrm";
            passwordEncryptor = EncryptorFactory.getInstance();
            String password = "icrm537";
            String encryptedP = passwordEncryptor.encryptPassword(loginId,
                                            password);
           System.out.println("encryptedPassword-----"+encryptedP +"\n"+encryptedP.length());
            //System.out.println(passwordEncryptor.checkPassword(loginId,password, encryptedP));
		} catch (Exception e) {
		            // TODO Auto-generated catch block
		            e.printStackTrace();
		}
*/
    	/*try{
	    	String txt="$P{1234}";
	
	        String re1="(\\$)";	// Any Single Character 1
	        String re2="(P)";	// Variable Name 1
	        String re3="(\\{)";	// Any Single Character 2
	        String re4="((?:[a-z][a-z]*[0-9]+[a-z0-9]*))";	// Alphanum 1
	        String re5="(\\})";	// Any Single Character 3
	        System.out.println("pattern----------    "+re1+re2+re3+re4+re5);
	        Pattern p = Pattern.compile(re1+re2+re3+re4+re5,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	        Matcher m = p.matcher(txt);
	        while(m.find())
	        {
	            String c1		=	m.group(1);
	            String var1		=	m.group(2);
	            String c2		=	m.group(3);
	            String alphanum1=	m.group(4);
	            String c3		=	m.group(5);
	            System.out.println(c1.toString()+var1.toString()+c2.toString()+alphanum1.toString()+c3.toString());
	            //System.out.print("("+c1.toString()+")"+"("+var1.toString()+")"+"("+c2.toString()+")"+"("+alphanum1.toString()+")"+"("+c3.toString()+")"+"\n");
	        }
    	}catch(Exception e){
    		System.out.println("Exception e"+e);
        	}
        }*/
     /*	try{
    		 String txt="select * from APP.QueueStats where qs_id in $P{QUEUEID}";

    		    String re1="(\\$)";	// Any Single Character 1
    		    String re2="(P)";	// Variable Name 1
    		    String re3="(\\{)";	// Any Single Character 2
    		    String re4="((?:[a-z]*[a-z0-9]*))";	// Alphanum 1
    		    String re5="(\\})";	// Any Single Character 3

    		    Pattern p = Pattern.compile(re1+re2+re3+re4+re5,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    		    Matcher m = p.matcher(txt);
    		    String regex = null;
    		    		
    		    while (m.find())
    		    {
    		        String c1=m.group(1);
    		        String var1=m.group(2);
    		        String c2=m.group(3);
    		        String alphanum1=m.group(4);
    		        String c3=m.group(5);
    		        regex = c1.toString()+var1.toString()+c2.toString()+alphanum1.toString()+c3.toString();
    		        System.out.println("Regex---"+regex);
    		        
    		    }
    		    txt=txt.replace(regex, "?");
    		    
    		    System.out.print(txt);
    	}catch(Exception e){
    		System.out.println("Exception e"+e);
        	}
        } */
    	//IntializationDAO.getDataSourceForCampaign();
     }
    public static String getFormattedQueueTime(long totalMinutes)
	{
		boolean isNegativeNum=false;
		if(totalMinutes<0)isNegativeNum = true;
		totalMinutes = Math.abs(totalMinutes);
		String date = "";
		if (((totalMinutes / 1440 / 60)) > 0) {
			date = ((totalMinutes / 1440 / 60)) + " d "
					+ ((totalMinutes / 60 / 60) % 24) + " h "
					+ ((totalMinutes / 60) % 60) + " m " + (totalMinutes % 60)
					+ " s ";
		} else if (((totalMinutes / 60 / 60) % 24) > 0) {
			date = ((totalMinutes / 60 / 60) % 24) + " h "
					+ (totalMinutes / 60 % 60) + " m " + (totalMinutes % 60)
					+ " s ";
		} else if (((totalMinutes / 60) % 60) > 0) {
			date = ((totalMinutes / 60) % 60) + " m " + (totalMinutes % 60)
					+ " s ";
		} else if ((totalMinutes % 60) > 0) {
			date = (totalMinutes % 60) + " s ";
		} else {
			date = "0 s";
		}
		 if(isNegativeNum) date = "-"+date;
		return date;
	}
	
    public static synchronized Connection getDerbyConnection() {
		Connection connection = null;
		
		try {
			ConnectionPoolManager.setCallerTraceLevel(4);
			
			ConnectionPool derbyConnectionPool = ConnectionPoolManager
					.getInstance().getConnectionPool("QSDerby");
			
			
			connection = derbyConnectionPool.getConnection();
			
			System.out.println("Get connection");
			
			

		}catch (Exception e) {
		e.printStackTrace();
			
		} finally {
			return connection;
		}
	}
	
    }

