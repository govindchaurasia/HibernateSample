package com.interactcrm.qstats.threads;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.classes.VoiceQueueStatManager;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.util.logging.LogHelper;



public class VoiceFetcher implements Runnable{
	
	private int fetcher_Sleeptime;
	private int queueGroup;
	private int tgId;
	private int channelId;

    private boolean _errorLog 		= false;
    private Log _logger 			= null;

    private Log log;
	/**
	 * Indicates if debug mode is enabled for the logger.
	 */
	private boolean isDebug = false;
	/**
	 * Indicates if error mode is enabled for the logger.
	 */
	private boolean isError = false;
	private TenantGroupObj tgGroup;
	private final String derbyQuery = "SELECT PKEY AS Queue_Pkey FROM APP.QSTATS_RAW_DATA WHERE QUEUEGROUP_PKEY = ? AND T060 = 1 AND TENANTGROUP_PKEY = ? AND CHANNEL = ?";
	private String immediateUpdateQuery = "UPDATE APP.QSTATS_RAW_DATA SET D060 = ?, D080 = ? WHERE PKEY = ?";
	private String scheduledUpdateQuery = "UPDATE APP.QSTATS_RAW_DATA SET D070 = ? WHERE PKEY = ?";
	private String immediateNscheduledUpdateQuery = "UPDATE APP.QSTATS_RAW_DATA SET D060 = ?, D080 = ?, D070 = ? WHERE PKEY = ?";

public VoiceFetcher(int channelId, int tgId,int queueGroup,int fetcher_Sleeptime,TenantGroupObj tgGroup){
		
		this.fetcher_Sleeptime	= fetcher_Sleeptime;
		this.queueGroup			= queueGroup;
		this.channelId			= channelId;
		this.tgId				= tgId;
		this.tgGroup	=	tgGroup;
		log = (new LogHelper(VoiceFetcher.class)).getLogger(LogModuleFactory.getModule("VoiceFetcher"),"VoiceFetcher_" + channelId + "_" + queueGroup);
		isDebug = log.isDebugEnabled();
		isError = log.isErrorEnabled();
		
		if (log.isDebugEnabled()) { 
			log.debug("VoiceFetcher:: fetcher_Sleeptime : "+ fetcher_Sleeptime );
		}

	} 
	private List<Integer> getQueueList() {
		List<Integer> queueList = null;
		Connection derbyConn = null;
		/*if (isDebug) {
			log.debug("getQueueList() About to retrieve Queue List from Derby...");
		}
		if (isDebug) {
			log.debug("[getQueueList]   queueGroup ["+queueGroup+"]  tgId["+tgId+"] channelId["+channelId+"]");
		}*/
		try {
			derbyConn = MMConnectionPool.getDerbyConnection();
			if (derbyConn != null) {
				PreparedStatement derbyStat = derbyConn.prepareStatement(derbyQuery);
				derbyStat.setInt(1, queueGroup);
				derbyStat.setInt(2, tgId);
				derbyStat.setInt(3, channelId);
				ResultSet rs = derbyStat.executeQuery();

				/*
				 * set the values from the result set to a list
				 */

				if (rs.next()) {
					queueList = new ArrayList<Integer>();
					do {
						queueList.add(rs.getInt("Queue_Pkey"));
					} while (rs.next());
				} else {
					if (isDebug) {
						log.debug("getQueueList() No result set obtained from derby while fetching queue list...");
					}
				}				
			}
		} catch (SQLException se) {
			if (_errorLog) {
				_logger.error("[getQueueList] :: Error in Executing Query--- ", se);
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[getQueueList] :: Error in Executing Query--- ", e);
			}
		} finally {
				
			try {
				MMConnectionPool.freeDerbyConnection(derbyConn);
			} catch (Exception e2) {
				if (_errorLog) {
					_logger.error("updateDisplayTable :: Error"+  " table.", e2);
				}
			}
			
		}
		return queueList;
	}

	private String createFetcherQuery(List<Integer> queueList) {
		if (queueList != null && !queueList.isEmpty()) {
			/*
			 * toString gives all elements of the List in a comma separated
			 * format surrounded by [ and ]. Hence, using substring to remove [
			 * at start and the ] at end.
			 */
			String listString = queueList.toString();
			String substring = listString.substring(1, listString.length() - 1);

			if (isDebug) {
				log.debug("createFetcherQuery() fetcher query created successfully...Queue List :: "+queueList);
			}
			
//			return "SELECT mq_pkey AS 'Queue_Pkey', COUNT (CR_PKEY) AS 'Call_Count', CASE WHEN CR_CALLBACK_TYPE = 1 THEN DATEDIFF(SECOND, GETDATE(), MIN(CR_QUEUE_ARRIVAL_TIME )) ELSE NULL END AS 'Longest_Waiting_Time', CR_CALLBACK_TYPE AS 'Callback_Type' FROM CBC_CALL_REQUEST JOIN mq_queues ON CR_QUEUE_EXTENSION = mq_id WHERE mq_pkey IN ("
//					+ substring + ") GROUP BY	mq_pkey, CR_CALLBACK_TYPE";
			
			
			 
//			return "SELECT mq_pkey AS Queue_Pkey, COUNT (CR_PKEY) AS Call_Count, CASE WHEN CR_CALLBACK_TYPE = 1 THEN DATEDIFF(SECOND,  MIN(CR_QUEUE_ARRIVAL_TIME), GETDATE()) ELSE NULL END AS Longest_Waiting_Time, CR_CALLBACK_TYPE AS Callback_Type FROM CBC_CALL_REQUEST JOIN mq_queues ON CR_QUEUE_EXTENSION = mq_id WHERE mq_pkey IN ("
//					+ substring + ") GROUP BY	mq_pkey, CR_CALLBACK_TYPE";
			
			return "SELECT mq_pkey AS Queue_Pkey, COUNT (CR_PKEY) AS Call_Count, CASE WHEN CR_CALLBACK_TYPE = 1 THEN DATEDIFF(SECOND,  MIN(CR_QUEUE_ARRIVAL_TIME), GETDATE()) WHEN CR_CALLBACK_TYPE = 2 THEN DATEDIFF(SECOND,  MIN(CR_ORIG_PREF_DATETIME), GETDATE()) ELSE NULL END AS Longest_Waiting_Time, CR_CALLBACK_TYPE AS Callback_Type FROM CBC_CALL_REQUEST JOIN mq_queues ON CR_QUEUE_EXTENSION = mq_id WHERE mq_pkey IN ("
			+ substring + ") GROUP BY	mq_pkey, CR_CALLBACK_TYPE";
			
//			return "SELECT mq_pkey AS Queue_Pkey, COUNT (CR_PKEY) AS Call_Count, CASE WHEN CR_CALLBACK_TYPE = 1 THEN DATEDIFF(SECOND, MIN(CR_QUEUE_ARRIVAL_TIME), GETDATE()) WHEN CR_CALLBACK_TYPE = 2 THEN DATEDIFF(SECOND,  MIN(CR_ORIG_PREF_DATETIME), GETDATE()) ELSE NULL END AS Longest_Waiting_Time, CASE WHEN CR_CALLBACK_TYPE = 1 OR CR_CALLBACK_TYPE = 2 THEN CR_CALLBACK_TYPE ELSE -1 END AS Callback_Type FROM CBC_CALL_REQUEST RIGHT JOIN mq_queues ON CR_QUEUE_EXTENSION = mq_id WHERE mq_pkey IN ("
//			+ substring + ") GROUP BY	mq_pkey, CR_CALLBACK_TYPE";
			
		} else
			return null;
	}
	
	

	@Override
	public void run() {
		
		HashMap<Integer,CallbackDetails> queueCallbackMap = null;
		
		
		while(tgGroup.isRunning()){
			
			List<Integer> queueList = getQueueList();
			if (isDebug) {
				log.debug("startFetching() Fetcher called, queue list :: " + queueList);
			}
			if (queueList != null && !queueList.isEmpty()) {
				queueCallbackMap = new HashMap<Integer, VoiceFetcher.CallbackDetails>();
				for (int i = 0; i < queueList.size(); i++) {
					Integer queuePkey = queueList.get(i);
					queueCallbackMap.put(queuePkey, new VoiceFetcher.CallbackDetails());
				}
				
				final String fetcherQuery = createFetcherQuery(queueList);
				/*
				 * starting a new thread performing the runnable task that fetches
				 * id from derby for the specified queuegroup_id, tenantgroup_id and
				 * channel_pkey and updates
				 */
				/*new Thread(new Runnable() {
					@Override
					public void run() {*/
						/*
						 * if the list is not empty, get data from CBC_CALL_REQUEST
						 * table and update the immediate callback count, scheduled
						 * callback count and longest waiting call back in the
						 * derby.
						 */
						Connection sqlConn = null;
						Connection derbyConn = null;
						try {
							sqlConn = MMConnectionPool.getDBConnection();
							derbyConn = MMConnectionPool.getDerbyConnection();
							if (sqlConn != null) {
								if (isDebug) {
									log.debug("run() Thread started, about to fetch data. Fetcher Query is ::"
											+ fetcherQuery);
								}
								PreparedStatement dbStat = sqlConn.prepareStatement(fetcherQuery);
								ResultSet resultSet = dbStat.executeQuery();
																
								
								/*
								 * TODO use the above result set to update columns
								 * back in the derby.
								 */								
								while (resultSet.next()) {	
									
									int queuePkey = resultSet.getInt("Queue_Pkey");

									int callbackType = resultSet.getInt("Callback_Type");
									if (callbackType == 1) {
										
										queueCallbackMap.get(queuePkey).setFoundImmediateCallback();

										int callCount = resultSet.getInt("Call_Count");
										int longestWaitingTime = resultSet.getInt("Longest_Waiting_Time");

										if (isDebug) {
											log.debug("run() CallBack Type is 1, about to update data. Call Count : "+callCount +" Longest Waiting Time : "+longestWaitingTime + " Queue Pkey : "+queuePkey);
										}
										
										try {
											PreparedStatement updateDerbyStat = derbyConn.prepareStatement(immediateUpdateQuery);
											updateDerbyStat.setInt(1, callCount);
											updateDerbyStat.setInt(2,longestWaitingTime);
											updateDerbyStat.setInt(3, queuePkey);
											int updatedRows = updateDerbyStat.executeUpdate();
											if (isDebug) {
												log.debug("run() Executing query :"+immediateUpdateQuery+", Parameter callCount["+callCount+"], longestWaitingTime["+longestWaitingTime+"], id(queuePkey)["+queuePkey+"]");
												log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
											}
										
										} catch (SQLException e) {
											if (isError) {
												log.error("run() SQL Exception occurred while updating immediate callback data...", e);
											}
										} catch (Exception e) {
											if (isError) {
												log.error("run() Exception occurred while updating immediate callback data...", e);
											}
										}

									} else if (callbackType == 2) {
										
										queueCallbackMap.get(queuePkey).setFoundScheduleCallback();

										int callCount = resultSet.getInt("Call_Count");
										

										if (isDebug) {
											log.debug("run() CallBack Type is 2, about to update data. Call Count : "+callCount +" Queue Pkey : "+queuePkey);
										}
										
										try {
										PreparedStatement updateDerbyStat = derbyConn.prepareStatement(scheduledUpdateQuery);
										updateDerbyStat.setInt(1, callCount);
										updateDerbyStat.setInt(2, queuePkey);
										int updatedRows = updateDerbyStat.executeUpdate();
										if (isDebug) {
											log.debug("run() Excuting query :"+scheduledUpdateQuery+", Parameter callCount["+callCount+"], id(queuePkey)["+queuePkey+"]");
											log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
										} 
										
										} catch (SQLException e) {
											if (isError) {
												log.error("run() SQL Exception occurred while updating scheduled callback data...", e);
											}
										} catch (Exception e) {
											if (isError) {
												log.error("run() Exception occurred while updating scheduled callback data...", e);
											}
										}
									}
									/*
									else if (callbackType == -1) {
																				
										int callCount = resultSet.getInt("Call_Count");
										int longestWaitingTime = resultSet.getInt("Longest_Waiting_Time");
										int queuePkey = resultSet.getInt("Queue_Pkey");
										
										if (isDebug) {
											log.debug("run() CallBack Type is -1, about to update data. Call Count : "+callCount +" Longest Waiting Time : "+longestWaitingTime + " Queue Pkey : "+queuePkey);
										}
										try {
											PreparedStatement updateDerbyStat = derbyConn.prepareStatement(immediateNscheduledUpdateQuery);
											updateDerbyStat.setInt(1, callCount);
											updateDerbyStat.setInt(2,longestWaitingTime);
											updateDerbyStat.setInt(3,callCount);
											updateDerbyStat.setInt(4, queuePkey);
											int updatedRows = updateDerbyStat.executeUpdate();
											if (isDebug) {
												log.debug("run() Excuting query :"+immediateNscheduledUpdateQuery+", Parameter callCount["+callCount+"], longestWaitingTime["+longestWaitingTime+"], callCount["+callCount+"], id(queuePkey)["+queuePkey+"]");
												log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
											}
											
											
										
										} catch (SQLException e) {
											if (isError) {
												log.error("run() SQL Exception occurred while updating callback data...", e);
											}
										} catch (Exception e) {
											if (isError) {
												log.error("run() Exception occurred while updating callback data...", e);
											}
										}									
									}
									*/
								}
								PreparedStatement updateDerbyStat = null;
								for (Entry<Integer, CallbackDetails> i : queueCallbackMap.entrySet()) {
									CallbackDetails callbackDetails = i.getValue();
									
									if(!callbackDetails.foundImmediateCallback() && !callbackDetails.foundScheduleCallback()){
										updateDerbyStat = derbyConn.prepareStatement(immediateNscheduledUpdateQuery);
										updateDerbyStat.setInt(1, 0);
										updateDerbyStat.setInt(2, 0);
										updateDerbyStat.setInt(3, 0);
										updateDerbyStat.setInt(4, i.getKey());
										int updatedRows = updateDerbyStat.executeUpdate();
										if (isDebug) {
											log.debug("run() Excuting query :"+immediateNscheduledUpdateQuery+", Parameter callCount["+0+"], longestWaitingTime["+0+"], callCount["+0+"], id(queuePkey)["+i.getKey()+"]");
											log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
										}
									}else if(!callbackDetails.foundImmediateCallback()){
										updateDerbyStat = derbyConn.prepareStatement(immediateUpdateQuery);
										updateDerbyStat.setInt(1, 0);
										updateDerbyStat.setInt(2,0);
										updateDerbyStat.setInt(3, i.getKey());
										int updatedRows = updateDerbyStat.executeUpdate();
										if (isDebug) {
											log.debug("run() Excuting query :"+immediateUpdateQuery+", Parameter callCount["+0+"], longestWaitingTime["+0+"], id(queuePkey)["+i.getKey()+"]");
											log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
										}
									}else if(!callbackDetails.foundScheduleCallback()){
										updateDerbyStat = derbyConn.prepareStatement(scheduledUpdateQuery);
										updateDerbyStat.setInt(1, 0);
										updateDerbyStat.setInt(2, i.getKey());
										int updatedRows = updateDerbyStat.executeUpdate();
										if (isDebug) {
											log.debug("run() Excuting query :"+scheduledUpdateQuery+", Parameter callCount["+0+"], id(queuePkey)["+ i.getKey()+"]");
											log.debug("run() data updated in derby successfully...update count :: "+updatedRows);
										} 
									}									
								}
								
								queueCallbackMap = null;
							}
						} catch (SQLException se) {
							se.printStackTrace();
							if (isError) {
								log.error("run() SQL Exception occurred while while retrieving data from CBC_CALL_REQUEST...", se);
							}
						} catch (Exception e) {
							e.printStackTrace();
							if (isError) {
								log.error("run() Exception occurred while retrieving data from CBC_CALL_REQUEST...", e);
							}
						} finally {
							MMConnectionPool.freeDerbyConnection(derbyConn);
							MMConnectionPool.freeConnection(sqlConn);
						}

					/*}
				}).start();*/
			} else {
				if (isDebug) {
					log.debug("startFetching() No queues found..."+queueList);
				}
			}

			try {
				Thread.sleep(fetcher_Sleeptime * 1000);
			} catch (InterruptedException e) {
			//	e.printStackTrace();
				if (isError) {
					log.error("VoiceFetcher :: "
							+  " ", e);
				}
			}
			
		 }	
		}
	
	private class CallbackDetails{
		private boolean foundImmediateCallback = false;
		private boolean foundScheduleCallback = false;
		
		private void setFoundImmediateCallback(){
			foundImmediateCallback = true;
		}
		
		private boolean foundImmediateCallback(){
			return foundImmediateCallback;
		}
		
		private void setFoundScheduleCallback(){
			foundScheduleCallback = true;
		}
		
		private boolean foundScheduleCallback(){
			return foundScheduleCallback;
		}	
		
	}
	

	
}
