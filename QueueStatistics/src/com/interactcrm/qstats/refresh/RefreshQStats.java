package com.interactcrm.qstats.refresh;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.classes.IQueueStatsManager;
import com.interactcrm.qstats.classes.QueueStatsManagerFactory;
import com.interactcrm.qstats.classes.ThreadCheckerStore;
import com.interactcrm.qstats.dao.IntializationDAO;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.qstats.qm.QueueManagerMap;
import com.interactcrm.qstats.startup.QMTenantGroupStore;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.qstats.threads.CampaignDataProcessor;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;

/**
 * Refreshes QueueStats 
 * The servlet stops all running threads 
 * Deletes data from derby , drops tables and recreates the tables.
 * and then resumes threads..
 * It also deletes internal static maps and recreates again..
 * @author vishakha
 *
 */

public class RefreshQStats extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private  Log _logger = null;
	private  boolean _debugLog = false;
	private  boolean _errorLog = false;
	
	public void init() {
		_logger = new LogHelper(RefreshQStats.class).getLogger(LogModuleFactory.getModule("RefreshQStats"));
		_debugLog = (_logger == null ? false : _logger.isDebugEnabled());
		_errorLog = (_logger == null ? false : _logger.isErrorEnabled());
	}
   
  
    public RefreshQStats() {
        super();
        // TODO Auto-generated constructor stub
    }

    
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(_debugLog){
			_logger.debug("Refreshing Queuestats");
		}
		doPost(request,response);
	}
	
	
	
/*
	@Override
	public String toString() {
		return "RefreshQStats [getInitParameterNames()="
				+ getInitParameterNames() + ", getServletConfig()="
				+ getServletConfig() + ", getServletContext()="
				+ getServletContext() + ", getServletInfo()="
				+ getServletInfo() + ", getServletName()=" + getServletName()
				+ ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ ", toString()=" + super.toString() + "]";
	}*/


	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out	=	response.getWriter();
		int tenantGrpId	=	0;
		String tgId		=	null;
	
		JSONObject jsonObject	= new JSONObject();
	   String tgIds[] = request.getParameterValues("tenantGroupID");
	   
	   if (_debugLog) {
			_logger.debug("[refreshQstats] Fetched parameter from servlet request is :: "+tgIds[0]+"." );
		}
	   
	   String tempString	=	tgIds[0].replaceAll("[\\[\\]]","");
	   String tempArray[]	=	tempString.split(",");
	  // String[] temp	=	tgIds[0].split(",");
	   
	   
	   int lenght	=	tempArray.length;
	    if (_debugLog) {
			_logger.debug("[refreshQstats] Formed array is "+tempArray.toString()+"  Lenght of formed array is :: "+lenght);
		}
	    
	    IntializationDAO initDao = new IntializationDAO();
	   
	   boolean enabledCampaignThreadOnRefresh=ThreadCheckerStore.getInstance().isCampaignThreadEnabled();
	    try{
	    	for (int i=0 ; i < lenght ; i++){
				tgId	=	tempArray[i];
			
				if (_debugLog) {
					_logger.debug("[refreshQstats] Refreshing Qstats for tgId.."+tgId);
				}
				
				
				 tenantGrpId	=	Integer.parseInt(tgId);
				
				if (tenantGrpId == 0) {
					if (_debugLog) {
						_logger.debug("[ refreshQstats ] TenantGrp Id fetched is "
								+ tenantGrpId
								+ " :: Refreshing qstats for all tenantGrps ");
					}
					

					Map<Integer, TenantGroupObj> map = TenantGroupStore.getInstance()
							.getTgObjectMap();

					for (TenantGroupObj obj : map.values()) {
						obj.setRunning(false);
					}

					if (_debugLog) {
						_logger.debug("[ refreshQstats ] Stopped threads for all tenantGrps ");
					}
					map.clear();

					TenantGroupStore.getInstance().createTenantGrpList();
					TenantGroupStore.getInstance().prepareTgObjMap();
					if (_debugLog) {
						_logger.debug("[ refreshQstats ] Inserting fresh data in tenantGrpMap "
								+ TenantGroupStore.getInstance().getTgObjectMap());
					}
					initDao.generateQueueStatsSchema();
					initDao.generateQueueStatsSeed();
					Initializer.getInstance().initializeQueueStatManager();
					if(enabledCampaignThreadOnRefresh)
					{
						if (_debugLog) {
							_logger.debug("[refreshQstats] Refreshing Campaign Processor thread..");
						}
						Initializer.getInstance().startCampaignProcessorThreads();
					}
					else
					{
						if (_debugLog) {
							_logger.debug("[refreshQstats] Campaign Processor thread is disabled..");
						}
					}

				} else {
					if (_debugLog) {
							_logger.debug("[refreshQstats] Refreshing Queuestats for tenatgrp id "
								+ tenantGrpId);
							}
				
					
					
					if(initDao.deleteOldTgData(tgId)){
						
						if (_debugLog) {
							_logger.debug("[refreshQstats] Deleting old data for tenantGrpId"
								+ tenantGrpId);
							}
					}
					
					if(initDao.updateTablesOnRefesh(tgId)){
						if (_debugLog) {
							_logger.debug("[refreshQstats] "
									+ " inserting fresh data tenantGrpId"
								+ tenantGrpId);
							}
					}
					Map<Integer, TenantGroupObj> map = TenantGroupStore.getInstance()
							.getTgObjectMap();
					if (map.containsKey(tenantGrpId)) {
						
						TenantGroupObj obj = TenantGroupStore.getInstance()
								.getTenantGroupObjFromMap(tenantGrpId);
						obj.setRunning(false);
						if (_debugLog) {
							_logger.debug("[refreshQstats] Stopped threads for tenantGrp "
									+ tenantGrpId);
						}

						if (_debugLog) {
							_logger.debug("[refreshQstats] Intitial map of tg against tgObj "
									+ map);
							}
						map.remove(tenantGrpId);
					} else {
						if (_debugLog) {
							_logger.debug("[refreshQstats] request found for new tenantGrpId "
									+ tenantGrpId);
						}

					}

					TenantGroupStore.getInstance().putToTgIdTenangGrpMap(tenantGrpId,
							new TenantGroupObj());
					if (_debugLog) {

						_logger.debug("[refreshQstats] Removed previous tg entry map for tgId "
								+ tenantGrpId
								+ " new map is "
								+ TenantGroupStore.getInstance().getTgObjectMap());

					}

					startThreadsForUpdatedData(tenantGrpId);
					if(enabledCampaignThreadOnRefresh)
					{
						if (_debugLog) {
							_logger.debug("[refreshQstats] Refreshing Campaign Processor thread..");
						}
						startCampaignProcessorThreadsOnRefresh(tenantGrpId);
					}
					else
					{
						if (_debugLog) {
							_logger.debug("[refreshQstats] Campaign Processor thread is disabled..");
						}
					}
				}
				
			}

	    	initDao.generateQueueGroupSchema();
			QMTenantGroupStore.getInstance().getQMTGMap().clear();
			QMTenantGroupStore.getInstance().getTGDetails().clear();
			QueueManagerMap.getInstance().getQMMap().clear();
			QueueGroupStore.getInstance().getVersionMap().clear();
			Initializer.getInstance().intializeQMStore();
			Initializer.getInstance().getAllQueues();
			int serverId =  Integer.parseInt(com.interactcrm.qstats.util.ActivePropertiesReader.getInstance().getProperty("Active.ID", "-1"));
			String dateTime	=	Initializer.getInstance().updateServerLoadTime(serverId);
			if (_debugLog) {
				_logger.debug("[refreshQstats] Server refreshed scussefully , last refreshed time is "+dateTime);
			}
			
			try {
				jsonObject.put("message", "Server refreshed sucessfuly");
				jsonObject.put("status", 0);
			} catch (JSONException e) {
				if (_errorLog) {
					_logger.error("[refreshQstats] Error while forming JSON response",e);
				}
				
			}

	    }catch(Exception e){
	    	try {
				jsonObject.put("message", "Error while refreshing server");
				jsonObject.put("status", 2);
			} catch (JSONException e1) {
				if (_errorLog) {
					_logger.error("[refreshQstats] Error while forming JSON response",e1);
				}
				
			}

	    	
	    }
			
	    if (_debugLog) {
			_logger.debug("[refreshQstats] Response sent is "+jsonObject);
		}
		out.println(jsonObject);
	}


	private void startThreadsForUpdatedData(int tg) {
		

	Connection dbConnection 	= null;
	PreparedStatement statement = null;
	ResultSet resultSet			= null;

	
	try {
		if (_debugLog) {
			_logger.debug("startThredaAfterRefersh for tgPkey "+tg);
		}			

		dbConnection = MMConnectionPool.getDerbyConnection();

		if (dbConnection != null) {
			String qgSelect 	= "SELECT DISTINCT queuegroup_pkey AS QG_PKEY,channel AS CHANNEL_ID,tenantgroup_pkey AS TENANTGROUP_ID FROM APP.QSTATS_RAW_DATA where tenantgroup_pkey=? ";
			statement 			= dbConnection.prepareStatement(qgSelect);
			statement.setInt(1,tg );
			resultSet					= statement.executeQuery();

			if(resultSet != null){
				while(resultSet.next()){
					int channelId	=	resultSet.getInt("CHANNEL_ID");
					int qgrpPkey	=	resultSet.getInt("QG_PKEY");
					int tgId		=	resultSet.getInt("TENANTGROUP_ID");

					if (_debugLog) {
						_logger.debug("QueueGroupId [" +qgrpPkey +"]  channelId ["+channelId+"] TenanGroupId ["+tgId+"]");
					}

					
					IQueueStatsManager absstatsUpdater = QueueStatsManagerFactory.getInstance(channelId, tgId, qgrpPkey);

					if (_debugLog) {
						_logger.debug("Refreshing absstatsUpdater--------" + absstatsUpdater);
					}                                        

					if(absstatsUpdater!=null ){
						if (_debugLog) {
							_logger.debug("Scheduled a Thread for QueueGroup = " +qgrpPkey +"channelId ["+channelId+"] TenanGroupId ["+tgId+"]");
						}         

				

						TenantGroupObj tgObj	=	TenantGroupStore.getInstance().getTenantGroupObjFromMap(tgId);
						absstatsUpdater.startFetching(tgObj);
						absstatsUpdater.startProcessing(tgObj);

					}else{
						if (_debugLog) {
							_logger.debug("No Thread Started for channelId ----" +qgrpPkey +"channelId ["+channelId+"] TenanGroupId ["+tgId+"]");
						} 
					}
				}
			}

		} else {
			if (_errorLog) {
				_logger.error("Refreshing :: Error fetching db connection in cleaning "
						+   " table");
			}
		}
	} catch (Exception e) {
		if (_errorLog) {
			_logger.error("Refreshing :: E Error in cleaning "
					+  " table.", e);
		}
	} finally {

		if (statement != null) {
			try {
				statement.close();
			} catch (Exception ex) {
				if (_errorLog) {
					_logger.error("Refreshing ::  Error in cleaning "
							+  " table.", ex);
				}
			}
			statement = null;
		}
		if (dbConnection != null) {
			try {
				MMConnectionPool.freeDerbyConnection(dbConnection);
			} catch (Exception ex) {
				if (_errorLog) {
					_logger.error("Refreshing :: Error in cleaning "
							+  " table.", ex);
				}
			}
		
		}
	  }
		
	}
	
	public void startCampaignProcessorThreadsOnRefresh(int tgId) {
		PreparedStatement pstmt	=	null; 
		ResultSet resultSet	=	null;
		Connection connection	=	null;
		int campaignId	=	0;
		int tenantGroupId	=	0;
		 String campaignName	=	null;
		 InputStream in	=	null;
		 Properties prop	=	new Properties() ;
		//Te
		try{
			
			if(_debugLog){
				_logger.debug("[startCampaignProcessorThreadsOnRefresh] Intializing and starting thread to process campaign data");
			}
			
			in	=	new FileInputStream(Utility.getAppHome()+File.separator+"SleepInterval.properties");
			prop.load(in);
			connection	=	MMConnectionPool.getDerbyConnection();
			if(connection !=null){
				String query	=	"SELECT campaign_pkey , campaign_name , tenantgroup_pkey from APP.qstats_campaign_data where tenantgroup_pkey=?";
				pstmt	=	connection.prepareStatement(query);
				pstmt.setInt(1,tgId);
				resultSet	=	pstmt.executeQuery();
				while(resultSet.next()){
					campaignId	=	resultSet.getInt("campaign_pkey");
					campaignName	=	resultSet.getString("campaign_name");
					tenantGroupId	=	resultSet.getInt("tenantgroup_pkey");
					
					TenantGroupObj tg	=	TenantGroupStore.getInstance().getTenantGroupObjFromMap(tenantGroupId);
					
					if(_debugLog){
						_logger.debug("[startCampaignProcessorThreadsOnRefresh] Refreshing thread for campaign id "+campaignId);
					}
					new Thread (new CampaignDataProcessor(campaignId, campaignName, Integer.parseInt(prop.getProperty("Sleeptime"),10),tg,tenantGroupId)).start();
				}
				
			}
		}catch(IOException e){
			if(_errorLog){
				_logger.error("[startCampaignProcessorThreadsOnRefresh] Some exception occured while reading sleeptime interval from properties file",e);
			}
			
		}catch(SQLException ex){
			if(_errorLog){
				_logger.error("[startCampaignProcessorThreadsOnRefresh] SQL exception occured",ex);
			}
			
		}catch(Exception e){
			if(_errorLog){
				_logger.error("[startCampaignProcessorThreadsOnRefresh] Some exception ",e);
			}
		}finally{
			if(connection !=null){
				MMConnectionPool.freeDerbyConnection(connection);
			}
		}
		
	}

}
