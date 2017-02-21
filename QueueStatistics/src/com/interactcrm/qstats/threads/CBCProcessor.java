package com.interactcrm.qstats.threads;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.interactcrm.dbcp.ConnectionPoolManager;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.CallbackBean;
import com.interactcrm.qstats.bean.QueryBean;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.util.logging.LogHelper;

public class CBCProcessor implements Runnable {

	private int channelId;
	private int tgId;
	private int queueGroup;
	private int fetcher_Sleeptime;
	private TenantGroupObj tgGroup;
	private Log _logger 			= null;
	private boolean _debugLog = false;
	private boolean _errorLog = false;
	private String callbackInsertQuery=null,queryForWaitingCallbacks=null;
	private String tenant="";
	private ArrayList<Integer> tenantList=new ArrayList<Integer>();
	
	public CBCProcessor(int channelId, int tgId,int queueGroup,int fetcher_Sleeptime,TenantGroupObj tgGroup)
	{
		this.channelId=channelId;
		this.tgId=tgId;
		this.queueGroup=queueGroup;
		this.fetcher_Sleeptime=fetcher_Sleeptime;
		this.tgGroup=tgGroup;
		_logger = (new LogHelper(CBCProcessor.class)).getLogger(LogModuleFactory.getModule("CBCProcessor"),"CBCProcessor_" + channelId + "_" + tgId);
		this.tenant=getTenantsByTgId(tgId);
		this.tenantList=getTenantListByTgId(tgId);
		if(_logger!=null)
		{
			_debugLog=_logger.isDebugEnabled();
			_errorLog=_logger.isErrorEnabled();
		}
		 QueryBean qBean = QueryFactory.getInstance().getquery("getWaitingCallbacks");
		 queryForWaitingCallbacks=qBean.getQueryString();
	}

	@Override
	public void run() {
		while (tgGroup.isRunning()) {
			if (_debugLog) {
				_logger.debug("[CBCProcessor run] tenantGrpObj " + tgGroup + " is " + tgGroup.isRunning());
			}
			  
				try {
					Thread.sleep(fetcher_Sleeptime * 1000);
					updateSummaryCountForExecutedCallback(tgId);
					insertCallbackData();
				
				} catch (InterruptedException e) {
					if (_errorLog) {
						_logger.error("CBCProcessor :: " + " ", e);
					}
				}
		}
	
	}
	
	public void insertCallbackData()
	{
		Connection conn=null;
		ResultSet dbrs=null;
		PreparedStatement dbstatement = null;
		PreparedStatement derbystatement=null;
		Connection derbyconnection=null;
		ResultSetMetaData rsmd				= null;
		ArrayList<String> insertQueryList	= new ArrayList<String>();
		long epoch=System.currentTimeMillis();
		int tenantId=0;
		try {
			conn=MMConnectionPool.getDBConnection();
			if(conn!=null)
			{
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				if (this.callbackInsertQuery == null){
					callbackInsertQuery=ConnectionPoolManager.getInstance().getProperty("WIC","FIND_WAITING_CALLBACKS");
					this.callbackInsertQuery = callbackInsertQuery.replaceAll("##tenantId##", tenant.substring(0, tenant.length()-1));
				}
				if (_debugLog) {
					_logger.debug("[insertCallbackData]:: FIND_WAITING_CALLBACKS Query----"+this.callbackInsertQuery);
				}
				dbstatement 	=   conn.prepareStatement(this.callbackInsertQuery,ResultSet.TYPE_SCROLL_INSENSITIVE, 
						  ResultSet.CONCUR_READ_ONLY); 
				dbrs					=   dbstatement.executeQuery();
				if(dbrs.next())
				{
					dbrs.beforeFirst();
					rsmd	  			= dbrs.getMetaData();
					while(dbrs.next())
					{
						String columnName	=	"";
						StringBuilder columnValues	=  new StringBuilder();
						
						for (int i = 1; i <= rsmd.getColumnCount(); i++) {								
							columnName		+= rsmd.getColumnLabel(i)+",";
							
							if(rsmd.getColumnType(i) == Types.VARCHAR){
								try{
									String str = dbrs.getString(rsmd
											.getColumnLabel(i));
									if (str != null) {
										if (str.contains("'")) {
											str = str.replaceAll("'", "''");
										}
									}
									columnValues.append("'").append(str).append("',");
								}catch(Exception e){
									columnValues.append("'',");
								}
							}else if(rsmd.getColumnType(i) == Types.INTEGER){
								try{
									
									Integer.parseInt(dbrs.getString(rsmd.getColumnLabel(i)));
									columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");
									if("tenant_pkey".equals(rsmd.getColumnLabel(i)))
									{
										tenantId=Integer.parseInt(dbrs.getString(rsmd.getColumnLabel(i)));
									}
								}catch(Exception e){
									columnValues.append("0,");
								}
							}else if (rsmd.getColumnType(i) == Types.BIGINT) {
								try {
									Long.parseLong(dbrs.getString(rsmd.getColumnLabel(i)));
									columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");

								} catch (Exception e) {
									columnValues.append("0,");

								}
							}
							
							else if(rsmd.getColumnType(i) == Types.TIMESTAMP){
								
								columnValues.append("'").append(dbrs.getString(rsmd.getColumnLabel(i))).append("',");
							}else{
								Integer.parseInt(dbrs.getString(rsmd.getColumnLabel(i)));
								columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");								 
							}														
						}
						columnName 		= columnName.substring(0,columnName.length()-1);
						String columnValue	= columnValues.substring(0,columnValues.length()-1);
						StringBuilder sb=new StringBuilder();
						sb.append("insert into APP.qstats_callback_data (")
						.append(columnName)
						.append(",channelId")
						.append(",version) values(")
						.append(columnValue)
						.append(",")
						.append(channelId)
						.append(",")
						.append(epoch)
					    .append(")");
						insertQueryList.add(sb.toString());
						
						if(_debugLog)
						{
							_logger.debug("insert Query list "+insertQueryList);
						}
						
						TenantGroupStore.getInstance().setVersion(tenantId, epoch);
						if (_debugLog) {
							_logger.debug("[insertCallbackData] :: Static map is created with version "+epoch+" and tenantId "+tenantId);
						}
					}
					
					
					int counter=0;
					
					try {
						derbyconnection=MMConnectionPool.getDerbyConnection();
						for(String executequery:insertQueryList){
							if (derbyconnection != null) {
								derbystatement = derbyconnection.prepareStatement(executequery);
								derbystatement.executeUpdate();
			 					counter++;
			 				}
							else
							{
								if (_debugLog) {
									_logger.error("[insertCallbackData] :: Exception in creating derbyconnection");
								}
							}
					   }
						if(_debugLog)
						{
							_logger.debug("[insertCallbackData] ::["+counter+"] No of records inserted successfully in derby for Callback");
						}
						updateSummaryCountForPendingCount(epoch);
					}
					catch(Exception e)
					{
						if (_errorLog) {
							_logger.error("[insertCallbackData] :: Exception in inserting", e);
						}
					}
					finally
					{
						if(derbyconnection!=null)
						{
							try
							{
								counter=0;
								MMConnectionPool.freeDerbyConnection(derbyconnection);
							}
							catch(Exception e3)
							{
								if (_errorLog) {
									_logger.error("[insertCallbackData] :: Error in closing connection ", e3);
								}
							}
						}
					}
					
				}
				else
				{
					for (Integer tenant : tenantList) {
						TenantGroupStore.getInstance().setVersion(tenant, epoch);
					}
					
					if (_debugLog) {
						_logger.debug("[insertCallbackData] :: FIND_WAITING_CALLBACKS ResultSet is null");
					}
				}
				updateSummaryCountForPendingCount(epoch);
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[insertCallbackData] :: Error in executing insertQueuegroupData", e);
			}
			
		}
		finally
		{

			if(dbstatement!=null)
			{
				try {
					dbstatement.close();
				} catch (Exception e2) {
					if (_errorLog) {
						_logger.error("[insertCallbackData] :: Error in closing statement ", e2);
					}
				}
			}
			if(conn!=null)
			{
				try
				{
					MMConnectionPool.freeConnection(conn);
				}
				catch(Exception e3)
				{
					if (_errorLog) {
						_logger.error("[insertCallbackData] :: Error in closing connection ", e3);
					}
				}
			}
				
		}
	}
	
	public void updateSummaryCountForPendingCount(long version)
	{
		List<Integer> tenantList	=	new ArrayList<Integer>();
		Map<Integer, CallbackBean> callbackMap=new HashMap<Integer,CallbackBean>();
		Connection derbyConn=null;
		PreparedStatement derbyPsmt=null;
		ResultSet derbyRs=null;
		try {
			derbyConn=MMConnectionPool.getDerbyConnection();
			if(derbyConn!=null)
			{
				derbyPsmt=derbyConn.prepareStatement(queryForWaitingCallbacks,ResultSet.TYPE_SCROLL_INSENSITIVE, 
						  ResultSet.CONCUR_READ_ONLY);
				derbyPsmt.setLong(1, version);
				if (_debugLog) {
					_logger.debug("updateSummaryCountForPendingCount:: Summary Details Query "+queryForWaitingCallbacks+" with version "+version);
				}
				derbyRs=derbyPsmt.executeQuery();
				int type	=	0;
				String waitingCounts	=	null;
				int tenantId	=	0;
				PreparedStatement stmt =null;
				String updateQuery=null;
				
				if(derbyRs.next())
				{
					derbyRs.beforeFirst();	
					while(derbyRs.next())
					{
						type=derbyRs.getInt("TYPE");
						waitingCounts=derbyRs.getString("WAITING_CALLBACK");
						tenantId=derbyRs.getInt("TENANT");
						
						if (_debugLog) {
							_logger.info("[updateSummaryCountForPendingCount]::  = Parameters  TYPE ["+type+"]  WAITING_COUNTS [ "+waitingCounts+"] TENANTID ["+tenantId+"]");
						}
						
						
						if(type==1)
						{
							updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_immediate_counts =? where tenant_pkey=?";
						}
						else if(type==2)
						{
							updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_schedule_counts =? where tenant_pkey=?";
						
						}
						
						stmt=derbyConn.prepareStatement(updateQuery);
						stmt.setString(1, waitingCounts);
						stmt.setInt(2, tenantId);
						stmt.addBatch();
						int i = stmt.executeUpdate();
						if (_debugLog) {
							_logger.info("[updateSummaryCountForPendingCount]:: = Number of rows updated with waiting count :: "+i);
						}
						tenantList.add(tenantId);	
					}
					
					}
				else
				{
					if(_debugLog)
					{
						_logger.debug("[updateSummaryCountForPendingCount]:: getWaitingCallbacks ResultSet is null");
					}
				}
				}
			else
			{
				if (_debugLog) {
					_logger.debug("updateSummaryCountForPendingCount:: Error in creating derby connection");
				}
			}
			
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[updateSummaryCountForPendingCount] :: Exception in inserting", e);
			}
			
		}
		finally
		{
			if(derbyConn!=null)
			{
				try {
					MMConnectionPool.freeDerbyConnection(derbyConn);
				} catch (Exception e2) {
					_logger.error("[updateSummaryCountForPendingCount] :: Error in closing connection ", e2);
				}
			}
					
		}
		updateSummaryForNonWaitingCallbacks(tenantList);
	}
	
	public void updateSummaryCountForPendingCountNew (long version)
	{

		List<Integer> tenantList	=	new ArrayList<Integer>();
		Map<Integer, CallbackBean> callbackMap=new HashMap<Integer,CallbackBean>();
		Connection derbyConn=null;
		PreparedStatement derbyPsmt=null;
		ResultSet derbyRs=null;
		try {
			derbyConn=MMConnectionPool.getDerbyConnection();
			if(derbyConn!=null)
			{
				derbyPsmt=derbyConn.prepareStatement(queryForWaitingCallbacks,ResultSet.TYPE_SCROLL_INSENSITIVE, 
						  ResultSet.CONCUR_READ_ONLY);
				derbyPsmt.setLong(1, version);
				if (_debugLog) {
					_logger.debug("updateSummaryCountForPendingCount:: Summary Details Query "+queryForWaitingCallbacks+" with version "+version);
				}
				derbyRs=derbyPsmt.executeQuery();
				int type	=	0;
				String waitingCounts	=	null;
				int tenantId	=	0;
				PreparedStatement stmt =null;
				String updateQuery=null;
				
				if(derbyRs.next()) 
				{
					derbyRs.beforeFirst();	
					while(derbyRs.next())
					{
						
						CallbackBean cb=new CallbackBean();
					    cb.setType(derbyRs.getInt("TYPE"));
						type=derbyRs.getInt("TYPE");
						waitingCounts=derbyRs.getString("WAITING_CALLBACK");
						tenantId=derbyRs.getInt("TENANT");
						
						
						/*
						
						
						if (_debugLog) {
							_logger.info("[updateSummaryCountForPendingCount]::  = Parameters  TYPE ["+type+"]  WAITING_COUNTS [ "+waitingCounts+"] TENANTID ["+tenantId+"]");
						}
						
						
						if(type==1)
						{
							updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_immediate_counts =? where tenant_pkey=?";
						
						}
						else if(type==2)
						{
							updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_schedule_counts =? where tenant_pkey=?";
						
						}
						stmt=derbyConn.prepareStatement(updateQuery);
						stmt.setString(1, waitingCounts);
						stmt.setInt(2, tenantId);
						stmt.addBatch();
						int i = stmt.executeUpdate();
						if (_debugLog) {
							_logger.info("[updateSummaryCountForPendingCount]:: = Number of rows updated with waiting count :: "+i);
						}
						tenantList.add(tenantId);	
					*/}
					
					}
				else
				{
					if(_debugLog)
					{
						_logger.debug("[updateSummaryCountForPendingCount]:: getWaitingCallbacks ResultSet is null");
					}
				}
				}
			else
			{
				if (_debugLog) {
					_logger.debug("updateSummaryCountForPendingCount:: Error in creating derby connection");
				}
			}
			
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[updateSummaryCountForPendingCount] :: Exception in inserting", e);
			}
			
		}
		finally
		{
			if(derbyConn!=null)
			{
				try {
					MMConnectionPool.freeDerbyConnection(derbyConn);
				} catch (Exception e2) {
					_logger.error("[updateSummaryCountForPendingCount] :: Error in closing connection ", e2);
				}
			}
					
		}
		updateSummaryForNonWaitingCallbacks(tenantList);
	
	}
	
	public void updateSummaryForNonWaitingCallbacks(List<Integer> tenantList)
	{
		
		StringBuilder sb	=	new StringBuilder();
		String updateQuery="";
		if (_debugLog) {
			_logger.info("updateSummaryForNonWaitingCallbacks::  = Updating RAW table for non waiting queues :: Other than queues ::"+tenantList);
			_logger.info("updateSummaryForNonWaitingCallbacks::  = QueueList size : "+tenantList.size());
		}
		if (tenantList.size() == 0) {
			updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_immediate_counts=0 ,pending_schedule_counts=0";
			if(_debugLog)
			{
				_logger.info("updateSummaryForNonWaitingCallbacks::  Queue size is 0,  Query to be execute :: "+updateQuery);	
			}
			
		} else {
			for (Integer tenant : tenantList) {
				sb.append(tenant + ",");
			}
			updateQuery = "update APP.QSTATS_CALLBACK_SUMMARY set pending_immediate_counts=0 ,pending_schedule_counts=0 where tenant_pkey NOT IN ("+sb.substring(0,sb.length()-1)+")";
			if(_debugLog)
			{
				_logger.info("updateSummaryForNonWaitingCallbacks::  Queue size is "+tenantList.size()+"  Query to be execute :: "+updateQuery);	
			}

		}
			Connection connection = MMConnectionPool.getDerbyConnection();
			PreparedStatement derbyStatement	=	null;
			try{
				
				derbyStatement = connection.prepareStatement(updateQuery);
			
				int i	=	derbyStatement.executeUpdate();
				
				if (_debugLog) {
					_logger.info("updateSummaryForNonWaitingCallbacks::  = No of rows updated :: "+i+" with waiting contact 0 , duration 0 for other queues");
				}
			}catch(Exception e){
				if (_errorLog) {
					_logger.error("Error in updating waiting contacts = 0", e);
				}
			}finally{
				if (derbyStatement != null) {
					try {
						derbyStatement.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("updateSummaryForNonWaitingCallbacks ::   "
									+  " ", ex);
						}
					}
					derbyStatement = null;
				}
				if (connection != null) {
					try {
						MMConnectionPool.freeDerbyConnection(connection);
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("updateSummaryForNonWaitingCallbacks :: "
									+  " ", ex);
						}
					}
					connection = null;
				}
				
			}
	
	
		
	}
	
	public void updateSummaryCountForExecutedCallback(int tg)
	{
		Connection conn=null,derbyConn=null;
		PreparedStatement derbyPstmt=null;
		CallableStatement cs=null;
		ResultSet rs=null;
		String query="",status="";
		boolean isResultSetData=false;
		
	    int counts=0,type=0,cr_tenant=0;
		
		try {
			conn=MMConnectionPool.getDBConnection();
			if(conn!=null)
			{
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				if(_debugLog)
				{
					_logger.debug("[updateCallbackSummarydetails] :: SP for Callbacks counts is 'GetCallbackCount(?,?)'");
				}
				cs=conn.prepareCall("{call GetCallbackCount(?,?)}");
				cs.setString(1,tenant.substring(0, tenant.length()-1));
				cs.setInt(2, channelId);
				
				rs=cs.executeQuery();
				derbyConn=MMConnectionPool.getDerbyConnection();
				
				
				while(rs.next())
					{
						StringBuilder sb=new StringBuilder();
						counts=rs.getInt("counts");
						type=rs.getInt("type");
						cr_tenant=rs.getInt("tenant");
						status=rs.getString("status");
				
					if (type == 1 && status.equals("Executed")) {
						sb.append("executed_immediate_counts=" + counts);
					} else if (type == 2 && status.equals("Executed")) {
						sb.append("executed_schedule_counts=" + counts);
					} else {
						sb.append("executed_immediate_counts=0,executed_schedule_counts=0");
					}
						
						String updateQuery="update APP.QSTATS_CALLBACK_SUMMARY set "+sb+" where tenant_pkey=?";
						if(_debugLog)
						{
							_logger.info("[updateCallbackSummaryCount] :: Update Query "+updateQuery);
						}
						
						derbyPstmt=derbyConn.prepareStatement(updateQuery);
						derbyPstmt.setInt(1, cr_tenant);
						int i=derbyPstmt.executeUpdate();
						isResultSetData=true;
						if (_debugLog) {
							_logger.info("[updateCallbackSummaryCount]:: = Number of rows updated :: "+i);
						}
					}
			
				if(!isResultSetData)
				{
					
					for(Integer tenantId:tenantList)
					{
						derbyPstmt=derbyConn.prepareStatement("update APP.QSTATS_CALLBACK_SUMMARY set executed_immediate_counts=0,executed_schedule_counts=0 where tenant_pkey=?");
						derbyPstmt.setInt(1, tenantId);
						derbyPstmt.executeUpdate();
						
					}
					
					if (_debugLog) {
						_logger.info("[updateCallbackSummaryCount] :: ResultSet for Callback Count is NULL for tenantId "
								+ tenant + " hence updating 0."+!isResultSetData);

					}
				}
			}
			
			else
			{
				
				if(_debugLog)
				{
					_logger.debug("[updateCallbackSummarydetails] :: Error is creating connection");
				}
			}
			
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[updateCallbackSummarydetails] :: Exception in updating", e);
			}
		}
		finally
		{

			try{
				if (conn != null) {
					MMConnectionPool.freeConnection(conn);
				}
				if (derbyConn != null) {
					MMConnectionPool.freeDerbyConnection(derbyConn);
				}
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("getTenantsByTgId :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
		
		}
		
	}
	
	
	public String getTenantsByTgId(int tgId)
	{
		Connection conn=null;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		ArrayList<Integer> tenantList=new ArrayList<Integer>();
		
		StringBuilder sb=new StringBuilder();
		try {
			
			conn=MMConnectionPool.getDBConnection();
			if(conn!=null)
			{
				String query="SELECT mt_pkey FROM mt_tenant where mt_is_active=1 and mt_is_deleted=0 and mt_tenant_group_pkey=? ";
				pstmt=conn.prepareStatement(query);
				pstmt.setInt(1, tgId);
				rs=pstmt.executeQuery();
				if(rs!=null)
				{
					while(rs.next())
					{
						
							tenantList.add(rs.getInt("mt_pkey"));
					}
					if (_debugLog) {
						_logger.debug("tenantList is successful "+tenantList);
					}
				}
				else{
					if (_debugLog) {
						_logger.error("getTenantsByTgId :: ResultSet is Empty:"); 
					}
			}
			}
			else{
				if (_errorLog) {
					_logger.error("getTenantsByTgId :: Error fetching derby connection:"); 
				}
			}
				
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("Error in creating getTenantsByTgId",e);
			}
		}
		finally{
			try{
				MMConnectionPool.freeConnection(conn);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("getTenantsByTgId :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
		}
		
		if(tenantList.size()==0)
		{
			sb=null;
		}
		else
		{
			for (Integer tenant : tenantList) {
				sb.append(tenant + ",");
			}
			
		}
		
		return sb.toString();
	}
	
	public ArrayList<Integer> getTenantListByTgId(int tgId)
	{
		Connection conn=null;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		ArrayList<Integer> tenantList=new ArrayList<Integer>();
		
		StringBuilder sb=new StringBuilder();
		try {
			
			conn=MMConnectionPool.getDBConnection();
			if(conn!=null)
			{
				String query="SELECT mt_pkey FROM mt_tenant where mt_is_active=1 and mt_is_deleted=0 and mt_tenant_group_pkey=? ";
				pstmt=conn.prepareStatement(query);
				pstmt.setInt(1, tgId);
				rs=pstmt.executeQuery();
				if(rs!=null)
				{
					while(rs.next())
					{
						
							tenantList.add(rs.getInt("mt_pkey"));
					}
					if (_debugLog) {
						_logger.debug("tenantList is successful "+tenantList);
					}
				}
				else{
					if (_debugLog) {
						_logger.error("getTenantsByTgId :: ResultSet is Empty:"); 
					}
			}
			}
			else{
				if (_errorLog) {
					_logger.error("getTenantsByTgId :: Error fetching derby connection:"); 
				}
			}
				
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("Error in creating getTenantsByTgId",e);
			}
		}
		finally{
			try{
				MMConnectionPool.freeConnection(conn);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("getTenantsByTgId :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
		}
		
		
		
		return tenantList;
	}
}
