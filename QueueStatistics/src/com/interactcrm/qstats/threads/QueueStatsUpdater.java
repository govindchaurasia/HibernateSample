package com.interactcrm.qstats.threads;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import com.interactcrm.dbcp.ConnectionPoolManager;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueryBean;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.util.logging.LogHelper;



/**
 * This thread finds waiting contacts in
 *  queue and max waiting duration in that particular queue and updates raw derby table..
 * @author vishakha
 *
 */
public class QueueStatsUpdater implements Runnable{
	
	private int fetcher_Sleeptime;
	private int queueGroup;
	private String quot="'";
	private int channelId;
	private boolean _debugLog 		= false;
    private boolean _errorLog 		= false;
    private boolean _infoLog 		= false;
    private Log _logger 			= null;
    private TenantGroupObj tgGroup;
	private static boolean isRunning	=	true;
	private int tgId;
	private String query="";
	private String summaryDetailsQuery="";
	private String queuegroupInsertQuery = null;

	public QueueStatsUpdater(int queueGroup,int fetcher_Sleeptime,int channelId,TenantGroupObj tgGroup,int tgId){
	
		this.fetcher_Sleeptime	= fetcher_Sleeptime;
		this.queueGroup			= queueGroup;
		this.channelId			= channelId;
		this.tgGroup	=	tgGroup;
		this.tgId=tgId;
		_logger 				= new LogHelper(QueueStatsUpdater.class).getLogger(LogModuleFactory.getModule("QueueStatsUpdater"), String.valueOf(queueGroup)+"_"+String.valueOf(channelId));		
	   
		
        if (_logger != null) {
            _debugLog = _logger.isDebugEnabled();
            _errorLog = _logger.isErrorEnabled();
            _infoLog = _logger.isInfoEnabled();
        }
        QueryBean qBean = QueryFactory.getInstance().getquery("getWaitingContacts");
		String queryForWaitingContacts=qBean.getQueryString();
	    summaryDetailsQuery=queryForWaitingContacts.replace("##quot##", quot).replaceAll("##queueGroupId##", String.valueOf(queueGroup));
      
	}
	/**
	 * The thread reads query from properties file using dbco getProperty method .
	 * The query finds total number of contacts waiting in queue and max waiting duration in that particular queue
	 * the data is then updated in raw table
	 */
	@Override
	public void run() {

		while (tgGroup.isRunning()) {
			if (_infoLog) {
				_logger.info("[QstatsUpdater run] tenantGrpObj " + tgGroup + " 88" + tgGroup.isRunning());
			}
			  
			if (channelId != 2) {
				
				insertQueueGroupData();
				try {
					Thread.sleep(fetcher_Sleeptime * 1000);
				} catch (InterruptedException e) {
					if (_errorLog) {
						_logger.error("QueueStatsUpdater :: " + " ", e);
					}
				}
			}

		}
	}

	
	private void insertQueueGroupData()
	{
		Connection dbSecondaryconnection=null;
		ResultSet dbrs=null;
		PreparedStatement dbstatement = null;
		PreparedStatement derbystatement=null;
		Connection derbyconnection=null;
		ResultSetMetaData rsmd				= null;
		ArrayList<String> insertQueryList	= new ArrayList<String>();
		long epoch=System.currentTimeMillis();
	
	
		
		try {
			dbSecondaryconnection=MMConnectionPool.getSecondaryConnection();
			
			if(dbSecondaryconnection!=null)
			{		
				dbSecondaryconnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				if (this.queuegroupInsertQuery == null){
					 query =ConnectionPoolManager.getInstance().getProperty("WIC","FIND_QUEUEGROUP_WAITING_CONTACTS");
					this.queuegroupInsertQuery = query.replaceAll("##quot##", quot).replaceAll("##queueGroupId##", String.valueOf(queueGroup)).replaceAll("##tenantGroupId##", String.valueOf(tgId));
				}
				
				if (_debugLog) {
					_logger.debug("insertQueuegroupData:: FIND_QUEUEGROUP_WAITING_CONTACTS Query----"+this.queuegroupInsertQuery);
				}
				dbstatement 	=   dbSecondaryconnection.prepareStatement(this.queuegroupInsertQuery,ResultSet.TYPE_SCROLL_INSENSITIVE, 
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
									if (_debugLog) {
										_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.VARCHAR----"+ rsmd.getColumnType(i)+"--->"+columnValues);
									}
								}catch(Exception e){
									columnValues.append("'',");
								}
							}else if(rsmd.getColumnType(i) == Types.INTEGER){
								try{
									
									Integer.parseInt(dbrs.getString(rsmd.getColumnLabel(i)));
									columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");
									if (_debugLog) {
										_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.Integer----"+ rsmd.getColumnType(i)+"--->"+columnValues );
									}
								}catch(Exception e){
									columnValues.append("0,");
								}
							}else if(rsmd.getColumnType(i) == Types.DECIMAL)
							{
								try {

									Integer.parseInt(dbrs.getString(rsmd.getColumnLabel(i)));
									columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");
									if (_debugLog) {
										_logger.debug(
												"generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.Decimal----"
														+ rsmd.getColumnType(i) + "--->" + columnValues);
									}
								} catch (Exception e) {
									columnValues.append("0,");
								}

							}
							
							else if(rsmd.getColumnType(i) == Types.TIMESTAMP){
								
								columnValues.append("'").append(dbrs.getString(rsmd.getColumnLabel(i))).append("',");
								if (_debugLog) {
									_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.Timestamp----"+ rsmd.getColumnType(i)+"--->"+columnValues  );
								} 
							}else if(rsmd.getColumnType(i) == Types.BIGINT){
								try {
									
									Long.parseLong(dbrs.getString(rsmd.getColumnLabel(i)));
									
									if ("QUEUETIME".equals(rsmd.getColumnLabel(i))) {
										columnValues.append("'").append(getFormattedQueueTime(Long.parseLong(""+dbrs.getInt("MILLISECS")))).append("',");
									if (_debugLog) {
											_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.BIGINT--QUeuetime--"+ rsmd.getColumnType(i)+"--->"+columnValues   );
										} 
									} else {
										
										columnValues.append(dbrs.getString(rsmd.getColumnLabel(i))).append(",");
										if (_debugLog) {
											_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Types.BIGINT else----"+ rsmd.getColumnType(i)+"--->"+columnValues  );
										} 
									}
								}catch(Exception e){
									columnValues.append("0,");
			
								}
							}else{
								String str = dbrs.getString(rsmd.getColumnLabel(i));
								if (str != null) {
									if (str.contains("'")) {
										str = str.replaceAll("'", "''");
									}
									
								}
							//	System.out.println("data from else---------------------------"+dbrs.getString(rsmd.getColumnLabel(i)));
								
								columnValues.append("'").append(dbrs.getString(rsmd.getColumnLabel(i))).append("',");
								if (_debugLog) {
									_logger.debug("generateQueueStatsSeed:: rsmd.getColumnType(i) == Else----"+ rsmd.getColumnType(i)+"--->"+columnValues);
								} 
							}														
						}
						columnName 		= columnName.substring(0,columnName.length()-1);
						String columnValue	= columnValues.substring(0,columnValues.length()-1);
						StringBuilder sb=new StringBuilder();
						sb.append("insert into APP.qstats_contact_queuegroup_")
						.append(queueGroup)
						.append("(")
						.append(columnName)
						.append(",version) values(")
						.append(columnValue)
						.append(",")
						.append(epoch)
					    .append(")");
						insertQueryList.add(sb.toString());
					}
					
					if(_debugLog)
					{
						_logger.debug("insert Query list ---->"+insertQueryList);
					}
				//	System.out.println("insert Query list ---->"+insertQueryList);
					
					// Inserting in Derby QueueGroup Table
					int counter=0;
					derbyconnection=MMConnectionPool.getDerbyConnection();
					try {
						for(String executequery:insertQueryList){
							if (derbyconnection != null) {
								derbystatement = derbyconnection.prepareStatement(executequery);
								
			 					derbystatement.executeUpdate();
			 					counter++;
			 				}
							else
							{
								if (_errorLog) {
									_logger.error("[insertQueuegroupData] :: Exception in creating derbyconnection");
								}
							}
					   }
						if(_debugLog)
						{
							_logger.debug("[insertQueuegroupData] ::["+counter+"] No of records inserted successfully in derby for QueueGroup ["+queueGroup+"]");
						}
					}
					catch(Exception e)
					{
						if (_errorLog) {
							_logger.error("[insertQueuegroupData] :: Exception in inserting", e);
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
									_logger.error("[insertQueuegroupData] :: Error in closing connection ", e3);
								}
							}
						}
					}
					
				}
				else
				{
					if (_debugLog) {
						_logger.debug("insertQueuegroupData:: FIND_QUEUEGROUP_WAITING_CONTACTS ResultSet is null");
					}
				}
				
				// Updating raw table for summary data
				updateSummaryDataInRaw(epoch);
				
				// Creating a static Map with QueueGroup and Version
				
				QueueGroupStore.getInstance().setVersion(queueGroup, epoch);
					if (_debugLog) {
						_logger.debug("insertQueuegroupData:: Static map is created with version "+epoch+" and queueGroup "+queueGroup);
					}	
			}
			else
			{
				if (_debugLog) {
					_logger.debug("insertQueuegroupData:: Error in creating secondary connection");
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("insertQueuegroupData :: Error in executing insertQueuegroupData", e);
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
						_logger.error("insertQueuegroupData :: Error in closing statement ", e2);
					}
				}
			}
			if(dbSecondaryconnection!=null)
			{
				try
				{
					MMConnectionPool.freeSecondaryConnection(dbSecondaryconnection);
				}
				catch(Exception e3)
				{
					if (_errorLog) {
						_logger.error("insertQueuegroupData :: Error in closing connection ", e3);
					}
				}
			}
			
		
		}
		
	}
	
	/*
	 * Updating raw table for summary data
	 * */
	
	private void updateSummaryDataInRaw(long version)
	{
		List<Integer> queueList	=	new ArrayList<Integer>();
		
	  Connection derbyconnection1=MMConnectionPool.getDerbyConnection();
	  
	  try {
		  if(derbyconnection1 != null)
		  {
			  PreparedStatement pstmt=derbyconnection1.prepareStatement(summaryDetailsQuery,ResultSet.TYPE_SCROLL_INSENSITIVE, 
					  ResultSet.CONCUR_READ_ONLY);
			pstmt.setLong(1, version);
			if (_debugLog) {
				_logger.debug("updateSummaryDataInRaw:: Summary Details Query "+summaryDetailsQuery+" with version "+version);
			}
			ResultSet rs=pstmt.executeQuery();
			String waitingDuration	=	null;
			String waitingContacts	=	null;
			int queueId	=	0;
			PreparedStatement stmt =null;
		
			
			if(rs.next())
			{
				rs.beforeFirst();		
				while(rs.next())
				{
					waitingDuration	=	rs.getString("TIME");
					waitingContacts	=	rs.getString("WAITING_CONTACTS");
					queueId	=	rs.getInt("QUEUEID");
				
						if (_infoLog) {
							_logger.info("[updateSummaryDataInRaw]::  = Parameters  TIMEDURATION ["+waitingDuration+"]  WAITING_CONTACTS [ "+waitingContacts+"] QUEUEID ["+queueId+"]");
						}
						
						String updateQuery = "update APP.QSTATS_RAW_DATA set D040 =? ,D050 =? where PKEY=?";
						stmt = derbyconnection1.prepareStatement(updateQuery);
						
						if (_infoLog) {
							_logger.info("Updating waiting count and duration :: "+updateQuery+" for queuePkey "+queueId);
						}
						stmt.setInt(		1,  Integer.parseInt(waitingContacts));
						stmt.setString(		2, 	waitingDuration);	
						stmt.setInt(		3, 	queueId);
						
						int i = stmt.executeUpdate();
						if (_infoLog) {
							_logger.info("[updateSummaryDataInRaw]:: = Number of rows updated with waiting count and duration :: "+i);
						}
						queueList.add(queueId);
				}
			
			}
			else
			{
				if(_debugLog)
				{
					_logger.debug("[updateSummaryDataInRaw]:: getWaitingContacts ResultSet is null");
				}
			}
		  }
		  else
		  {
				if (_debugLog) {
					_logger.debug("updateSummaryDataInRaw:: Error in creating derby connection");
				}
		  }
		  
	} catch (Exception e) {
		if (_errorLog) {
			_logger.error("[updateSummaryDataInRaw] :: Exception in inserting", e);
		}
	}
		
		finally
		{
			if(derbyconnection1!=null)
			{
				try {
					MMConnectionPool.freeDerbyConnection(derbyconnection1);
				} catch (Exception e2) {
					_logger.error("[updateSummaryDataInRaw] :: Error in closing connection ", e2);
				}
			}
					
		}
	  updateRawForNonWaitingContacts(queueList);
	}

	@Deprecated
	private void resetQueueGroup(int queueGroupId)
	{
		Connection con = MMConnectionPool.getDerbyConnection();
		try {
			if (con != null) {
				String query = "update APP.QSTATS_RAW_DATA set D040 =0 ,D050 =0 where  QUEUEGROUP_PKEY="
						+ queueGroupId;
				
				PreparedStatement pst = con.prepareStatement(query);
				pst.execute();
				if(_debugLog)
				{
					_logger.debug("[resetQueueGroup] :: Reset All Queues to zero");
				}
			}
		} catch (SQLException e) {
			if (_errorLog) {
				_logger.error("resetQueueGroup :: Error in executing resetQueueGroup", e);
			}
		}
		finally
		{
			try {
				MMConnectionPool.freeDerbyConnection(con);
			} catch (Exception e2) {
				if (_errorLog) {
					_logger.error("insertQueuegroupData :: Error in closing connection ", e2);
				}
			}
		}
		
	}
	
	

	public String getFormattedQueueTime(long totalMinutes)
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
	
	
	/**
	 * This is to clear old count in raw table , if currently no contact waiting in queue
	 * @param queueList
	 */
	
	private void updateRawForNonWaitingContacts(List<Integer> queueList) {
		
		
		StringBuilder sb	=	new StringBuilder();
		String insertQuery="";
		if (_infoLog) {
			_logger.info("QueueStatsUpdater::  = Updating RAW table for non waiting queues :: Other than queues ::"+queueList);
			_logger.info("QueueStatsUpdater::  = QueueList size : "+queueList.size());
		}
		if (queueList.size() == 0) {
			insertQuery = "update APP.QSTATS_RAW_DATA set D040 =0 ,D050 =0 where QUEUEGROUP_PKEY = ?";
			if(_infoLog)
			{
				_logger.info("QueueStatsUpdater::  Queue size is 0,  Query to be execute :: "+insertQuery);	
			}
			
		} else {
			for (Integer queue : queueList) {
				sb.append(queue + ",");
			}
			insertQuery = "update APP.QSTATS_RAW_DATA set D040 =0 ,D050 =0 where PKEY NOT IN ("+sb.substring(0,sb.length()-1)+") and QUEUEGROUP_PKEY = ?";
			if(_infoLog)
			{
				_logger.info("QueueStatsUpdater::  Queue size is "+queueList.size()+"  Query to be execute :: "+insertQuery);	
			}

		}
	
		//if(sb.length() >0){
		
			Connection connection = MMConnectionPool.getDerbyConnection();
			PreparedStatement derbyRawStattement	=	null;
			try{
				/*	if (_infoLog) {
					_logger.info("QueueStatsUpdater::  =Query fired to update the raw with {0 , 0} {waiting contacts , duration} :: "+insertQuery);
				}*/
				derbyRawStattement = connection.prepareStatement(insertQuery);
				derbyRawStattement.setInt(1, queueGroup);
				int i	=	derbyRawStattement.executeUpdate();
				
				if (_infoLog) {
					_logger.info("QueueStatsUpdater::  = No of rows updated :: "+i+" with waiting contact 0 , duration 0 for other queues");
				}
			}catch(Exception e){
				if (_errorLog) {
					_logger.error("Error in updating waiting contacts = 0", e);
				}
			}finally{
				if (derbyRawStattement != null) {
					try {
						derbyRawStattement.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("QueueStatsUpdater ::   "
									+  " ", ex);
						}
					}
					derbyRawStattement = null;
				}
				if (connection != null) {
					try {
						MMConnectionPool.freeDerbyConnection(connection);
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("updateDerbyQueues :: "
									+  " ", ex);
						}
					}
					connection = null;
				}
				
			}
	
	}
	
	

}
