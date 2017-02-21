package com.interactcrm.qstats.threads;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;

public class ContactPurging implements Runnable {

	private boolean _debugLog 		= false;
    private boolean _errorLog 		= false;
    private Log _logger 			= null;
    private long version;
    private Connection derbyConnection=null;
    private PreparedStatement derbyStmt=null;
    private int purging_SleepTime	=	20;
    
	public ContactPurging()
	{
		_logger 	= new LogHelper(ContactPurging.class).getLogger(LogModuleFactory.getModule("ContactPurging"));
		 if (_logger != null) {
	            _debugLog = _logger.isDebugEnabled();
	            _errorLog = _logger.isErrorEnabled();
	            _logger.isInfoEnabled();
	        }
		 	InputStream in	=	null;
	        Properties push	=	new Properties();
	        
	        try {
				 in	=	new FileInputStream(Utility.getAppHome() + File.separator + "SleepInterval.properties");
				 push.load(in);
				 purging_SleepTime	=	Integer.parseInt(push.getProperty("Purging_Sleeptime"));
			} catch (Exception e) {
				
				e.printStackTrace();
			}
	       
	}
	
	public void run() {
		while(true)
		{
			
		try {
			if(_debugLog)
			{
				_logger.debug("Thread sleeping for "+purging_SleepTime * 1000);
			}
			Thread.sleep(purging_SleepTime*1000);
			
			purgingQueueGroupContacts();
			purgingCallbackContacts();
		} catch (Exception e) {
			
			if(_errorLog)
			{
				_logger.error("Error Occured in thread",e);
			}
		}
		
		}
	}
	
	public void purgingQueueGroupContacts()
	{
		
		try {
		    Map<Integer, Long> versionMap=QueueGroupStore.getInstance().getVersionMap();
		    derbyConnection = MMConnectionPool.getDerbyConnection();
		    for (Entry<Integer,Long> entry : versionMap.entrySet()) {
		    	String deleteQuery = "delete from APP.QSTATS_CONTACT_QUEUEGROUP_" + entry.getKey()
						+ " where version<" + entry.getValue();
				if (_debugLog) {
					_logger.debug("[purgingQueueGroupContacts] :: QueueGroup Contacts Deleting Query " + deleteQuery);
				}
				
				if (derbyConnection != null) {
					derbyStmt = derbyConnection.prepareStatement(deleteQuery);
					int i = derbyStmt.executeUpdate();
					if (_debugLog) {
						_logger.debug("[purgingQueueGroupContacts] :: No of contacts deleted: " + i);
					}
				} else {
					if (_errorLog) {
						_logger.error("[purgingQueueGroupContacts] :: Error in getting connection");
					}
				}
			}
					
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[purgingQueueGroupContacts] :: Exception in QueueGroupPurging", e);
			}
		} finally {
			if (derbyStmt != null) {
				try {
					derbyStmt.close();
				} catch (Exception e2) {
					if (_errorLog) {
						_logger.error("[purgingQueueGroupContacts] :: Exception in closing statement", e2);
					}
				}
			}
			if (derbyConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(derbyConnection);
				} catch (Exception e2) {
					if (_errorLog) {
						_logger.error("[purgingQueueGroupContacts] :: Exception in closing connction", e2);
					}
				}
			}
		}
	}
	
	public void purgingCallbackContacts()
	{
		
		try {
		    Map<Integer, Long> versionMap=TenantGroupStore.getInstance().getVersionMap();
		    derbyConnection = MMConnectionPool.getDerbyConnection();
		    for (Entry<Integer,Long> entry : versionMap.entrySet()) {
		    	String deleteQuery = "delete from APP.QSTATS_CALLBACK_DATA where version<" + entry.getValue();
				if (_debugLog) {
					_logger.debug("[purgingCallbackContacts] :: Callback Deleting Query " + deleteQuery);
				}
				
				if (derbyConnection != null) {
					derbyStmt = derbyConnection.prepareStatement(deleteQuery);
					int i = derbyStmt.executeUpdate();
					if (_debugLog) {
						_logger.debug("[purgingCallbackContacts] :: No of contacts deleted: " + i);
					}
				} else {
					if (_errorLog) {
						_logger.error("[purgingCallbackContacts] :: Error in getting connection");
					}
				}
			}
					
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[purgingCallbackContacts] :: Exception in purgingCallbackContacts", e);
			}
		} finally {
			if (derbyStmt != null) {
				try {
					derbyStmt.close();
				} catch (Exception e2) {
					if (_errorLog) {
						_logger.error("[purgingCallbackContacts] ::  Exception in closing statement", e2);
					}
				}
			}
			if (derbyConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(derbyConnection);
				} catch (Exception e2) {
					if (_errorLog) {
						_logger.error("[purgingCallbackContacts] :: Exception in closing connction", e2);
					}
				}
			}
		}
	}
}
