/**
 * Class Name : com.interactcrm.db.MMConnectionPool
 * Project Name: InteractionManagerMM
 * Version : 1.0
 * @author Vandana T. Joshi
 */
package com.interactcrm.qstats.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.interactcrm.alarm.Alarm;
import com.interactcrm.alarm.AlarmFactory;
import com.interactcrm.alarm.AlarmGeneratorUtil;
import com.interactcrm.dbcp.ConnectionPool;
import com.interactcrm.dbcp.ConnectionPoolManager;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.dao.QueueStatsDAO;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.util.logging.LogHelper;

/**
 * Manages the Database Connection.
 * Contains methods to fetch and release the database connection for primary and derby db.
 * @author Vipin Singh
 * @version 1.0
 * @since 1.0
 */
public class MMConnectionPool {
	private static long OPEN_NUM = 0;
	private static long OPEN_DERBY = 0;
	private static long OPEN_SECONDARY=0;
	private static Log _logger = new LogHelper(MMConnectionPool.class)
			.getLogger(LogModuleFactory.getModule("QueueStatistics"),"DBConnection");

	private static boolean flagForDerby	= false;
	private static boolean flagForWs	=	false;
	private static boolean flagForPrimary	=	false;
	private static boolean flagForDerbyCrash = false;
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;

	/** 
	 * Frees the DB connection for derby table.
	 * @param connection : Connection to derby database
	 */
	
	static {
		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
		}
	}
	
	public static synchronized void freeDerbyConnection(Connection connection) {
	
		try {
			if ((connection != null)) {
				long ctr = --OPEN_DERBY;
				if (_debugLog)
					_logger.debug("[freeDerbyConnection] Number of opened Connections = ["
							+ ctr + "] = [" + connection.hashCode() + "]");
				
				ConnectionPoolManager.getInstance()
						.getConnectionPool("QSDerby")
						.freeConnection(connection);
			} else {
				if (_errorLog)
					_logger.error("[freeDerbyConnection] Connection is either null or already closed.");
			}
		} catch (Exception e) {
			String message = "[freeDerbyConnection] Exception closing a database connection";
			if (_errorLog)
				_logger.error(message, e);
		}
	}

	/**
	 * Fetches derby connection. Make sure that entry QSDerby exists in database_connections table.
	 * @return Connection to derby database
	 */
	@SuppressWarnings("finally")
	public static synchronized Connection getDerbyConnection() {
		Connection connection = null;
		long ctr	=	0;
		List<Integer> tenantGroups	=	Initializer.getInstance().getTenantGrpList();
		
		try {
			ConnectionPoolManager.setCallerTraceLevel(4);
			
			ConnectionPool derbyConnectionPool = ConnectionPoolManager
					.getInstance().getConnectionPool("QSDerby");
			
			
			connection = derbyConnectionPool.getConnection();
			
			
			
			if (_debugLog)
				_logger.debug("Connection "+connection);
			 ctr = ++OPEN_DERBY;
			if (_debugLog)
				_logger.debug("[getDerbyConnection] Number of open Connections = ["
						+ ctr + "] = [" + connection.hashCode() + "]");
			


		} catch (SQLException e) {
			Alarm alarm = AlarmFactory.createAlarm();
			alarm.setName("Alarm from Queuestats");
			alarm.setPriority(10);
			alarm.setStatus(Alarm.IMMEDIATE);
			
			if (_errorLog)
				_logger.error("[getDerbyConnection] ", e);
			
			if(e.getErrorCode()==40000){
			
				if(!flagForDerbyCrash){	
					alarm.setDescription("Derby db crashed , replace the derby and restart the server !!!  "+e.toString());
					for (Integer tgId : tenantGroups) {
						alarm.setTenantGroupId(tgId);									
						AlarmGeneratorUtil.getInstance().raiseAlarm(alarm);
					}
					flagForDerbyCrash	=	true;
					if (_logger.isInfoEnabled())
						_logger.info("[getDerbyConnection] Derby db crashed :: Alaram sent "+e.toString());
				}	
			}
			
			if(!flagForDerby){	
				alarm.setDescription("Derby connection down "+e.toString());
				for (Integer tgId : tenantGroups) {
					alarm.setTenantGroupId(tgId);									
					AlarmGeneratorUtil.getInstance().raiseAlarm(alarm);
				}
				flagForDerby	=	true;
				if (_errorLog)
					_logger.error("[getDerbyConnectiom] Derby connection down :: Alram sent "+ e.toString());
			}
			
		
	
			
		} catch (Exception e) {
		
			
			if (_errorLog)
				_logger.error("[getDerbyConnection] ", e);
		} finally {
			return connection;
		}
	}
	
	
	/**
	 * @return Available Database connection from the connection pool.
	 */
	@SuppressWarnings("finally")
	public static synchronized Connection getDBConnection() {
		Connection connection = null;
		
		try {
			ConnectionPoolManager.setCallerTraceLevel(4);
			ConnectionPool connectionPool = ConnectionPoolManager.getInstance()
					.getConnectionPool("PRIMARY");
			connection = connectionPool.getConnection();
			long ctr = ++OPEN_NUM;
			if (_debugLog) {
				_logger.debug("getDBConnection :: Number of open Connections = ["
						+ ctr + "] = [" + connection.hashCode() + "]");
			}
		} catch (SQLException e) {
			
			List<Integer> tenantGroups	=	Initializer.getInstance().getTenantGrpList();
		
			Alarm alarm = AlarmFactory.createAlarm();
			alarm.setName("Alarm from Queuestats");
			alarm.setPriority(10);
			alarm.setStatus(Alarm.IMMEDIATE);
			
			
			if (_errorLog){
				_logger.error("[getPrimaryConnectiom] ",e);
				_logger.error("[getPrimaryConnection ]"+e.getErrorCode()+""+e.getMessage());
			}
			
			if(!flagForPrimary){	
				alarm.setDescription("Primary connection down "+e.toString());
				for (Integer tgId : tenantGroups) {
					alarm.setTenantGroupId(tgId);									
					AlarmGeneratorUtil.getInstance().raiseAlarm(alarm);
				}
				flagForPrimary	=	true;
				if (_errorLog)
					_logger.error("[getPrimaryConnectiom] Primary connection down :: Alram sent "+ e.toString());
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("getDBConnection ", e);
			}
		} finally {
			return connection;
		}
	}

	/**
	 * closes the Database connection and returns connection to the pool.
	 */
	public static synchronized void freeConnection(Connection connection) {
		try {
			if ((connection != null) && !(connection.isClosed())) {
				long ctr = --OPEN_NUM;
				if (_debugLog) {
					_logger.debug("freeConnection :: Number of closed Connections = ["
							+ ctr + "] = [" + connection.hashCode() + "]");
				}
				ConnectionPoolManager.getInstance()
						.getConnectionPool("PRIMARY")
						.freeConnection(connection);
			} else {
				if (_errorLog) {
					_logger.error("freeConnection :: Connection is either null or already closed.");
				}
			}
		} catch (Exception e) {
			String message = "freeConnection :: Exception closing a database connection";
			if (_errorLog) {
				_logger.error(message, e);
			}
		}
	}
	
	/** 
	 * Frees the DB connection for derby table.
	 * @param connection : Connection to derby database
	 */
	public static synchronized void freeSecondaryConnection(Connection connection) {
		// String methodName = "freeConnection";
		// TODO free connection pool
		try {
			if ((connection != null)) {
				long ctr = --OPEN_SECONDARY;
				if (_debugLog)
					_logger.debug("[[freeSecondaryConnection]] Number of opened Connections = ["
							+ ctr + "] = [" + connection.hashCode() + "]");
				ConnectionPoolManager.getInstance()
						.getConnectionPool("WIC")
						.freeConnection(connection);
			} else {
				if (_errorLog)
					_logger.error("[[freeSecondaryConnection]] Connection is either null or already closed.");
			}
		} catch (Exception e) {
			String message = "[[freeSecondaryConnection]] Exception closing a database connection";
			if (_errorLog)
				_logger.error(message, e);
		}
	}

	/**
	 * Fetches derby connection. Make sure that entry QSDerby exists in database_connections table.//WIC
	 * @return Connection to derby database
	 */
	@SuppressWarnings("finally")
	public static synchronized Connection getSecondaryConnection() {
		Connection connection = null;
		
		List<Integer> tenantGroups	=	Initializer.getInstance().getTenantGrpList();
		
		
		try {
			ConnectionPoolManager.setCallerTraceLevel(4);
			ConnectionPool derbyConnectionPool = ConnectionPoolManager
					.getInstance().getConnectionPool("WIC");
			connection = derbyConnectionPool.getConnection();
			long ctr = ++OPEN_SECONDARY;
			if (_debugLog)
				_logger.debug("[[getSecondaryConnection]] Number of open Connections = ["
						+ ctr + "] = [" + connection.hashCode() + "]");
			

		} catch (SQLException e) {
			Alarm alarm = AlarmFactory.createAlarm();
			alarm.setName("Alarm from Queuestats");
			alarm.setPriority(10);
			alarm.setStatus(Alarm.IMMEDIATE);
			if (_errorLog)
				_logger.error("[getSecondaryConnectiom]  " ,e);
			
			if(!flagForWs){	
				alarm.setDescription("Secondary connection DOWN "+e.toString());
				for (Integer tgId : tenantGroups) {
					alarm.setTenantGroupId(tgId);									
					AlarmGeneratorUtil.getInstance().raiseAlarm(alarm);
				}
				flagForWs	=	true;
				if (_logger.isInfoEnabled())
					_logger.info("[getSecondaryConnectiom] Secondary connection down :: Alram sent "+e.toString());
			}
			

		} catch (Exception e) {
			if (_errorLog)
				_logger.error("[[getSecondaryConnection]] ", e);
		} finally {
			return connection;
		}
	}
	
	

}
