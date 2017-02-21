package com.interactcrm.qstats.threads;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.util.logging.LogHelper;

/**
 * This is a voice processor thread which does processing for Avaya data as well as for SIP data
 * 
 *
 */
public class VoiceProcessor implements Runnable{

	int voiceProcessor_Sleeptime;
	int queueGroup;
	int channelId;
	int tgId;
	boolean _debugLog 		= false;
    boolean _errorLog 		= false;
    boolean _infoLog 		= false;
    Log _logger 			= null;
    TenantGroupObj tenantGrpObj;
	private static boolean isRunning	=	true;
	public VoiceProcessor(int channelId, int tgId,int queueGroup,int voiceProcessor_Sleeptime,TenantGroupObj tenantGrpObj){
		this.tgId = tgId;
		this.voiceProcessor_Sleeptime = voiceProcessor_Sleeptime;
		this.queueGroup			= queueGroup;
		this.channelId			= channelId;
		this.tenantGrpObj	=	tenantGrpObj;
		_logger 			= new LogHelper(VoiceProcessor.class).getLogger(LogModuleFactory.getModule("VoiceProcessor"), String.valueOf(queueGroup)+"_"+String.valueOf(channelId));		
	        
        if (_logger != null) {
            _debugLog = _logger.isDebugEnabled();
            _errorLog = _logger.isErrorEnabled();
            _infoLog = _logger.isInfoEnabled();
        }
        
       
        
	}
	
	@Override
	public void run() {
		
		while(tenantGrpObj.isRunning()){			
			updateDisplayTable(channelId, tgId, queueGroup);	
			
			
			updateAggregateTable(channelId,tgId);
		
			try {
				Thread.sleep(voiceProcessor_Sleeptime * 1000);
			} catch (InterruptedException ie) {
				if (_errorLog) {
					_logger.error("VoiceProcessor :: "
							+  " ", ie);
				}
			}
		}
		
		
		
	}

	
	private void updateAggregateTable(int channelId, int tgId) {
		
		if (_logger.isDebugEnabled()) { 
			_logger.debug("updateChannelTable:: Parameters Passed ----channelId["+channelId+"], tgId["+tgId+"]" );
		}
		
		switch(channelId){
    	
        case 2:
     	   		updateForAvayaVoice(channelId,tgId);
     	   		break;
     	   		
        case 10: 
        	updateForSIP(channelId,tgId);
     	    	break;
     	    	
     	default:
     		updateForAvayaVoice(channelId,tgId);
 	   		break;
     	   	
		}

	}

	private void updateForSIP(int channelId, int tgId) {
		Connection dbConnection			 		= null;
		PreparedStatement selectStatement		= null;
		ResultSet selectResultSet				= null;
		PreparedStatement updateStatement		= null;
		int calls_count = 0;

		int agg_D020 = 0,agg_D030=0,agg_D040=0,agg_D060=0,agg_D070=0,agg_D090=0,agg_D100=0,agg_D110=0,agg_D130=0;
		String agg_D051=null;
		 if (_logger.isDebugEnabled()) { 
				_logger.debug("****[SIPVoiceProcessor]*****");
			}
		
		try {
			
	        dbConnection=MMConnectionPool.getDerbyConnection();
	        
	        
	        
	        try{
	        	if(dbConnection!=null){
	        		
	        		String selectFromRaw="SELECT sum(D020) as agg_D010 ,sum(D040) as agg_D040,sum(D030) as agg_D030,sum(D060) as agg_D060,sum(D070) as agg_D070,sum(D090) as agg_D090,sum(D100) as agg_D100,sum(D130) as agg_D130,max(D051) as agg_D051 FROM APP.QSTATS_RAW_DATA WHERE channel = ? AND tenantgroup_pkey = ? ";
	        		 if (_logger.isDebugEnabled()) { 
	        				_logger.debug("VoiceProcessor:: in updateChannelTable channelId and tenant group id passed : "+channelId+" "+tgId );
	        			}
	        		selectStatement=dbConnection.prepareStatement(selectFromRaw);
	        		selectStatement.setInt(1, channelId);
					selectStatement.setInt(2, tgId);
					selectResultSet 		= selectStatement.executeQuery();
					if(selectResultSet!=null){
						while(selectResultSet.next()){	
							agg_D020=selectResultSet.getInt("agg_D010");
							
							agg_D030=selectResultSet.getInt("agg_D030");
							agg_D040=selectResultSet.getInt("agg_D040");
							agg_D060=selectResultSet.getInt("agg_D060");
							agg_D070=selectResultSet.getInt("agg_D070");
							agg_D090=selectResultSet.getInt("agg_D090");
							agg_D100=selectResultSet.getInt("agg_D100");
							agg_D130=selectResultSet.getInt("agg_D130");
							agg_D051=selectResultSet.getString("agg_D051");
						
						}
						if (_logger.isDebugEnabled()) { 
							_logger.debug("updateChannelTable:: selectfromRaw - "+selectFromRaw+",Aggregated Data: " +
									"agg_D020["+agg_D020+"] " +											
									"agg_D030["+agg_D030+"] " +
									"agg_D040["+agg_D040+"] " +
									"agg_D060["+agg_D060+"] " +
									"agg_D070["+agg_D070+"]" +
									"agg_D090["+agg_D090+"] "+
									"agg_D130["+agg_D130+"] " +
									"agg_D051["+agg_D051+"]");
						}
					}
					else{
						if (_errorLog) {
							_logger.error("updateChannelTable :: ResultSet is Empty.");
						}
					}
					
				
					
					String longest_duration = agg_D051.substring(2);
	        		
	        		String selectFromAgentCLI="SELECT count(*) as agg_data  FROM APP.qstats_raw_agent_CLI WHERE channel = ? AND tenantgroup_pkey = ? ";
	        		 if (_logger.isDebugEnabled()) { 
	        				_logger.debug("[SIPVoiceProcessor]:: in updateChannelTable channelId ["+channelId+"] and tenant group id passed ["+tgId+" ]" );
	        			}
	        		selectStatement=dbConnection.prepareStatement(selectFromAgentCLI);
	        		selectStatement.setInt(1, channelId);
					selectStatement.setInt(2, tgId);
					selectResultSet 		= selectStatement.executeQuery();
					if(selectResultSet!=null){
					
						while(selectResultSet.next()){
							calls_count = selectResultSet.getInt("agg_data");
							if (_logger.isDebugEnabled()) { 
								_logger.debug("[SIPVoiceProcessor]:: selectfromRaw - "+selectFromRaw+",Aggregated Data: " +
										"agg_D020["+calls_count+" ]") ;										
									
							}
							
						}
						
					}
					else{
						if (_errorLog) {
							_logger.error("[SIPVoiceProcessor] :: ResultSet is Empty.");
						}
					}
					
					
				
					String insertAggregatedata = "UPDATE APP.qstats_channel_data SET A010 = ?,A020=?,A030=?,A050=?,A060=?,A070=?,A080=?,A090=?,A100=? WHERE channel_pkey = ? AND tenantgroup_pkey = ? ";
					updateStatement 		 = dbConnection.prepareStatement(insertAggregatedata);
					if(_debugLog){
						_logger.debug("[SIPVoiceProcessor] :: "+insertAggregatedata+", With Aggregated Data");
					}
					updateStatement.setInt(1,agg_D040 );
					updateStatement.setInt(2, agg_D020);
					updateStatement.setInt(3, agg_D030);
					//updateStatement.setInt(4, agg_D040);
					updateStatement.setInt(4, agg_D060);
					updateStatement.setInt(5, agg_D070);
					updateStatement.setInt(6, agg_D090);
					updateStatement.setInt(7, agg_D100);
					updateStatement.setString(8, longest_duration);
					updateStatement.setInt(9, calls_count);
				
					updateStatement.setInt(10, channelId);
					updateStatement.setInt(11, tgId);
					updateStatement.executeUpdate();
					
				}
	        	else {
					if (_errorLog) {
						_logger.error("[SIPVoiceProcessor] :: Error fetching db connection ");
					}
				}
	        }catch(Exception e){
					if (_errorLog) {
						_logger.error("[SIPVoiceProcessor] :: Error in Executing Query--- ", e);
					}
				}
	      
	        }
		 catch (Exception e) {
				if (_errorLog) {
					_logger.error("[SIPVoiceProcessor] ::  Error  "
							+  " table.", e);
				}
			} finally {

				if (selectStatement != null) {
					try {
						selectStatement.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("[SIPVoiceProcessor] ::  Error "
									+  " table.", ex);
						}
					}
					selectStatement = null;
				}
				
				if (dbConnection != null) {
					try {
						MMConnectionPool.freeDerbyConnection(dbConnection);
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("[SIPVoiceProcessor] :: Error relasing derby connection", ex);
						}
					}
					//dbConnection = null;
				}
			}
	}

	private void updateForAvayaVoice(int channelId, int tgId
			) {
		 if (_logger.isDebugEnabled()) { 
				_logger.debug("****[AvayaVoiceProcessor]*****");
			}
		Connection dbConnection			 		= null;
		PreparedStatement selectStatement		= null;
		ResultSet selectResultSet				= null;
		PreparedStatement updateStatement		= null;
		int agg_D020 = 0,agg_D030=0,agg_D040=0,agg_D060=0,agg_D070=0,agg_D090=0,agg_D100=0,agg_D110=0,agg_D130=0;
		String agg_D051=null;
		
		
		try {
			
	        dbConnection=MMConnectionPool.getDerbyConnection();
	        
	        
	        
	        try{
	        	if(dbConnection!=null){
	        		//String selectFromRaw="SELECT "+count1+"= sum(D041) FROM APP.QSTATS_RAW_DATA WHERE channel = ? AND tenantgroup_pkey = ? ";
	        		
	        		String selectFromRaw="SELECT sum(D020) as agg_D010 ,sum(D040) as agg_D040,sum(D030) as agg_D030,sum(D060) as agg_D060,sum(D070) as agg_D070,sum(D090) as agg_D090,sum(D100) as agg_D100,sum(D130) as agg_D130,max(D051) as agg_D051 FROM APP.QSTATS_RAW_DATA WHERE channel = ? AND tenantgroup_pkey = ? ";
	        		 if (_logger.isDebugEnabled()) { 
	        				_logger.debug("VoiceProcessor:: in updateChannelTable channelId and tenant group id passed : "+channelId+" "+tgId );
	        			}
	        		selectStatement=dbConnection.prepareStatement(selectFromRaw);
	        		selectStatement.setInt(1, channelId);
					selectStatement.setInt(2, tgId);
					selectResultSet 		= selectStatement.executeQuery();
					if(selectResultSet!=null){
						while(selectResultSet.next()){	
							agg_D020=selectResultSet.getInt("agg_D010");
							agg_D030=selectResultSet.getInt("agg_D030");
							agg_D040=selectResultSet.getInt("agg_D040");
							agg_D060=selectResultSet.getInt("agg_D060");
							agg_D070=selectResultSet.getInt("agg_D070");
							agg_D090=selectResultSet.getInt("agg_D090");
							agg_D100=selectResultSet.getInt("agg_D100");
							agg_D130=selectResultSet.getInt("agg_D130");
							agg_D051=selectResultSet.getString("agg_D051");
						
						}
						if (_logger.isDebugEnabled()) { 
							_logger.debug("updateChannelTable:: selectfromRaw - "+selectFromRaw+",Aggregated Data: " +
									"agg_D020["+agg_D020+"] " +											
									"agg_D030["+agg_D030+"] " +
									"agg_D040["+agg_D040+"] " +
									"agg_D060["+agg_D060+"] " +
									"agg_D070["+agg_D070+"]" +
									"agg_D090["+agg_D090+"] "+
									"agg_D130["+agg_D130+"] " +
									"agg_D051["+agg_D051+"]");
						}
					}
					else{
						if (_errorLog) {
							_logger.error("updateChannelTable :: ResultSet is Empty.");
						}
					}
					
				
					
					String longest_duration = agg_D051.substring(2);
					
					String insertAggregatedata = "UPDATE APP.qstats_channel_data SET A010 = ?,A020=?,A030=?,A050=?,A060=?,A070=?,A080=?,A090=?,A100=? WHERE channel_pkey = ? AND tenantgroup_pkey = ? ";
					updateStatement 		 = dbConnection.prepareStatement(insertAggregatedata);
					if(_debugLog){
						_logger.debug("updateChannelTable :: "+insertAggregatedata+", With Aggregated Data");
					}
					updateStatement.setInt(1,agg_D040 );
					updateStatement.setInt(2, agg_D020);
					updateStatement.setInt(3, agg_D030);
					//updateStatement.setInt(4, agg_D040);
					updateStatement.setInt(4, agg_D060);
					updateStatement.setInt(5, agg_D070);
					updateStatement.setInt(6, agg_D090);
					updateStatement.setInt(7, agg_D100);
					updateStatement.setString(8, longest_duration);
					updateStatement.setInt(9, agg_D130);
				
					updateStatement.setInt(10, channelId);
					updateStatement.setInt(11, tgId);
					updateStatement.executeUpdate();
					
				}
	        	else {
					if (_errorLog) {
						_logger.error("updateChannelTable :: Error fetching db connection ");
					}
				}
	        }catch(Exception e){
					if (_errorLog) {
						_logger.error("updateChannelTable :: Error in Executing Query--- ", e);
					}
				}
	      
	        }
		 catch (Exception e) {
				if (_errorLog) {
					_logger.error("updateChannelTable ::  Error  "
							+  " table.", e);
				}
			} finally {

				if (selectStatement != null) {
					try {
						selectStatement.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("updateChannelTable ::  Error "
									+  " table.", ex);
						}
					}
					selectStatement = null;
				}
				
				if (dbConnection != null) {
					try {
						MMConnectionPool.freeDerbyConnection(dbConnection);
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("updateChannelTable :: Error relasing derby connection", ex);
						}
					}
					//dbConnection = null;
				}
			}
		
		
		
		
	}

	private void updateDisplayTable(int channelId, int tgId,int queueGroup) {

		
		Connection dbConnection			 		= null;
		PreparedStatement selectStatement		= null;
		ResultSet selectResultSet				= null,aggregateResultSet=null;
		PreparedStatement updateStatement		= null;
		//int agg_D010 = 0,agg_D020 = 0,agg_D030=0,agg_D040=0,agg_D060=0,agg_D070=0,agg_D090=0,agg_D100=0,agg_D110=0,agg_D130=0;
		/*String agg_D051=null;*/
		try {
			
	        /*if (_logger.isDebugEnabled()) { 
				_logger.debug("updateDisplayTable:: Parameters Passed ----channelId["+channelId+"], tgId["+tgId+"], queueGroup["+queueGroup+"]" );
			}
	        */
	        dbConnection = MMConnectionPool.getDerbyConnection();
	        
				try{
					if (dbConnection != null) {	
					
						dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
//						String selectfromRaw	= "select PKEY AS PKEY,ID AS ID,CHANNEL AS CHANNEL,PRIORITY AS PRIORITY,TENANT_PKEY AS TENANT_PKEY,TENANTGROUP_PKEY AS TENANTGROUP_PKEY,LU010 AS LU010,LU040 AS LU040,D000 AS D000,D010 AS D010,D020 AS D020,D030 AS D030,D040 AS D040,D050 AS D050,D060 AS D060,D070 AS D070,D080 AS D080,D090 AS D090,T040 AS T040,T041 AS T041,T042 AS T042 from APP.QSTATS_RAW_DATA WHERE QUEUEGROUP_PKEY=? AND IS_ACTIVE=1";
						String selectfromRaw	= "SELECT pkey AS PKEY, D000 AS D000,D040 AS  D040,D051 AS D051,D060 AS D060,D070 AS D070,D080 AS D080,T040 AS T040,T041 AS T041,T042 AS T042,D020 AS D020,D030 AS D030,D140 As D140,D150 As D150,D160 As D160 FROM APP.QSTATS_RAW_DATA WHERE channel = ? AND tenantgroup_pkey = ? AND queuegroup_pkey = ? ";
						//select CASE WHEN A010!=0 THEN A040 ELSE CASE WHEN A020!=0 THEN A030 ELSE pkey END END As Data FROM agg_table
						selectStatement 		= dbConnection.prepareStatement(selectfromRaw);						
						selectStatement.setInt(1, channelId);
						selectStatement.setInt(2, tgId);
						selectStatement.setInt(3, queueGroup);
						
						selectResultSet 		= selectStatement.executeQuery();	
						if(selectResultSet!=null){
							while(selectResultSet.next()){																							
								String D000 = selectResultSet.getString("D000");
								
								int D040 = selectResultSet.getInt("D040");								
								String D051 = selectResultSet.getString("D051");
								int D060 = selectResultSet.getInt("D060");
								int D070 = selectResultSet.getInt("D070");
								int D080 = selectResultSet.getInt("D080");
								int T040 = selectResultSet.getInt("T040");
								String T041 = selectResultSet.getString("T041");
								String T042 = selectResultSet.getString("T042");
							   int D020=selectResultSet.getInt("D020");
								int D030=selectResultSet.getInt("D030");
								int D140=selectResultSet.getInt("D140");
								int D150=selectResultSet.getInt("D150");
								int D160=selectResultSet.getInt("D160");
								 if (_logger.isDebugEnabled()) { 
										_logger.debug("updateDisplayTable:: selectfromRaw - "+selectfromRaw+", PKEY["+selectResultSet.getInt("PKEY")+"] " +
												"D000["+D000+"] " +											
												"D040 Number of Contacts waiting ["+D040+"] " +
												"D051 Duration for Longest call waiting ["+D051+"] " +
												"D060 Call back PENDING Immediate ["+D060+"] " +
												"D070 Call back PENDING scheduled ["+D070+"] " +
												"D080 longest call waiting ["+D080+"]" +
												"T040 Threshold limit for no of contacts waiting ["+T040+"] "+
												"T041 Hexcode of colour in which the contact card will be displayed before breaching threshold ["+T041+"] " +
												"T042 Hexcode of colour in which the contact card will be displayed after breaching threshold. ["+T042+"] " +
												"D020 Call handled since morning ["+D020+"]"+
												"D030 Call Abandoned since morning ["+D030+"]"+
												"D140 Staffed agents ["+D140+"]"+
												"D150 Available agents ["+D150+"]"+
												"D160 AUX agents ["+D160+"]"
												);
									}
								if(D051==null){
									D051 = "00:00:00";
								}
								StringBuilder displayData = new StringBuilder();								
									
									displayData.append(D000)
									.append("<br>");
									if( D040!= 0 || D060!=0 || D070!=0){					
										displayData.append("(")
										.append(D040);
											if( D060!=0 || D070!=0){
												displayData.append("|")
												.append(D060)
												.append("|")
												.append(D070);
											}
										displayData.append(") ");
									}
									
									int hour = 0;
									int minutes = 0;
									int seconds = 0;
									String temp = null;								
									int time = 0; 
									
									StringTokenizer timeTokens = new StringTokenizer(D051,":");
									int numberOfTokens = timeTokens.countTokens();
									int count = 0;
									while(timeTokens.hasMoreElements()){
										count++;
										temp = timeTokens.nextToken();
										if(numberOfTokens == 3){
											if(count == 1){
												hour = Integer.parseInt(temp);
											}else if(count == 2){
												minutes = Integer.parseInt(temp);
											}else if(count == 3){
												seconds = Integer.parseInt(temp);
											}
										}else if (numberOfTokens == 2){
											if(count == 1){
												minutes = Integer.parseInt(temp);
											}else if(count == 2){
												seconds = Integer.parseInt(temp);
											}
										}else if (numberOfTokens == 1){
											if(count == 1){
												seconds = Integer.parseInt(temp);
											}
										}
									}
									
									if(hour != 0){									
										displayData.append(hour+"h ");
										time = time + hour * 60 * 60;
									}
									if(minutes != 0){
										displayData.append(minutes+"m ");
										time = time + minutes * 60 ;
									}
									
									if(seconds != 0){
										displayData.append(seconds+"s ");
										time = time + seconds;
									}				
//									if(hour==0 && minutes==0){
//										displayData.append(seconds+"s ");
//										time = time + seconds;
//									}
									String displayColor = null;
									if(time >= D080 && time > T040){
										displayColor = T042;
									}else if(time >= D080 && time < T040){
										displayColor = T041;
									}else if(time <= D080 && time > T040){
										displayColor = T042;
									}else if(time <= D080 && time < T040){
										displayColor = T041;
									}
									
									String longest_duration = D051;
									if(longest_duration.length()<=7){
										longest_duration	=	"0"+longest_duration;
									}
									//String insertDisplaydata = "UPDATE APP.QSTATS_DISPLAY_DATA SET D000=?,D010=?,D020=?,D030=?,D040=?,D050=?,D060=?,D070=?,D080=?,D090=? WHERE PKEY=?";
									String insertDisplaydata = "UPDATE APP.QSTATS_DISPLAY_DATA SET D010 = ?,D090 = ?, D051=? ,D200  = ? , D210 =? ,D220=? ,D040 = ? WHERE PKEY = ?";
									//String insertDisplaydata = "UPDATE APP.QSTATS_DISPLAY_DATA SET D010 = ?,D090 = ? WHERE PKEY = ?";
									updateStatement 		 = dbConnection.prepareStatement(insertDisplaydata);
									
								//	int auxAgents	=	D140-D150;
									if(_debugLog){
										_logger.debug("updateDisplayTable :: "+insertDisplaydata+", Parameter - D010 ["+displayData.toString()+"], D090 ["+displayColor+"]"+",Parameter -  D051 ["+D051+"]"
												+ "D140[staffed agents] "+D140+" D150 [available agents] "+D150+" D160[Aux agents] "+D160);
									}
									updateStatement.setString(1, displayData.toString());
									updateStatement.setString(2, displayColor);
									updateStatement.setString(3, longest_duration);
									updateStatement.setInt(4,D140);
									updateStatement.setInt(5, D150);
									updateStatement.setInt(6, D160);
									updateStatement.setInt(7, D040);
									updateStatement.setInt(8, selectResultSet.getInt("PKEY"));
									
									updateStatement.executeUpdate();
								
							}
						}else{
							if (_errorLog) {
								_logger.error("updateDisplayTable :: ResultSet is Empty.");
							}
						}
					}else {
						if (_errorLog) {
							_logger.error("updateDisplayTable :: Error fetching db connection ");
						}
					}
				}catch(Exception e){
					if (_errorLog) {
						_logger.error("updateDisplayTable :: Error in Executing Query--- ", e);
					}
				}
				
				        
			if (_logger.isDebugEnabled()) {
				_logger.debug("updateDisplayTable:: Query Executed Successfully.");
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("updateDisplayTable ::  Error  "
						+  " table.", e);
			}
		} finally {

			if (selectStatement != null) {
				try {
					selectStatement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("updateDisplayTable ::  Error "
								+  " table.", ex);
					}
				}
				selectStatement = null;
			}
			
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("updateDisplayTable :: Error"
								+  " table.", ex);
					}
				}
				//dbConnection = null;
			}
		}
	
	
		
	}
	
	
	public static void stopThread(){
		isRunning = false;
	}
	
	public static void startThread(){
		isRunning = true;
	}
	
	

}
