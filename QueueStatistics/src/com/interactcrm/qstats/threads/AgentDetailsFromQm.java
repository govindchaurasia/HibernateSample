package com.interactcrm.qstats.threads;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;

import java.util.Map;
import java.util.Properties;

import com.interactcrm.qstats.bean.AgentDetails;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.initialize.HttpURLConnector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.mm.constants.Constants;
import com.interactcrm.qstats.qm.QueueManager;
import com.interactcrm.qstats.qm.QueueManagerMap;
import com.interactcrm.qstats.startup.AgentMonitorStore;
import com.interactcrm.qstats.startup.QMTenantGroupStore;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;


public class AgentDetailsFromQm implements Runnable {


	private boolean _errorLog = false;
	private boolean _infoLog = false;
	private Log _logger = null;

	Map	<Integer,AgentDetails>	agentMap	=	new HashMap<Integer,AgentDetails>();
	private InputStream in	=	null;
	private Properties push	=	new Properties ();
	private int sleepTime	=	15;
	public AgentDetailsFromQm() {
		_logger = new LogHelper(AgentDetailsFromQm.class).getLogger(LogModuleFactory.getModule("Agent_Details"));
		if (_logger != null) {
			_errorLog = _logger.isErrorEnabled();
			_infoLog = _logger.isInfoEnabled();
		}
		try {
			in	=	new FileInputStream(Utility.getAppHome() + File.separator + "SleepInterval.properties");
			push.load(in);
			sleepTime	=	Integer.parseInt(push.getProperty("Sleeptime"));
			if(_infoLog){
				_logger.info("Sleep time passed is "+sleepTime);
			}
		} catch (Exception ie) {
			if (_errorLog) {
				_logger.error("Processor :: "
						+  " ", ie);
			}
		}
	}
	
	public void run() {
		
		while(true){			
			getMyAgentData();
			
			try {
				Thread.sleep(sleepTime * 1000);
				if(_infoLog){
					_logger.info("Now thread will sleep for "+sleepTime);
				}
			} catch (Exception ie) {
				if (_errorLog) {
					_logger.error("Processor :: "
							+  " ", ie);
				}
			}
		}
	}
	/**
	 * This method communicates with qm to get AgentMonitor details and staffed agent/aux agent count..
	 * Pass tenantGroupId to QM servlet 
	 */
	private void getMyAgentData(){

	
	    Map<Integer,Integer> qmTGMap = QMTenantGroupStore.getInstance().getQMTGMap();
			if (_infoLog) {
				_logger.info("In Run method of staffed agents");
			}
			try {

				for(Map.Entry<Integer, Integer> entry: qmTGMap.entrySet() ){

					if (_infoLog) {
						_logger.info(" Current tenangroup is--"+entry.getKey());
						_logger.info(" Queue manager is --"+entry.getValue());
					}
					
					QueueManager qManager	= QueueManagerMap.getInstance().getQueueManager(entry.getValue());

					storeAgentStatsDetails(qManager,entry.getKey());
					
					getStaffedAuxDetails(qManager,entry.getKey());

				}
			} catch (Exception e) {
				if (_errorLog) {
					_logger.info("Error in Run of Agent Data  " + e);
				}
			}
	
		
	
	}

	/**
	 * This method finds staffed agent count ,staffed supervisor count ,aux agent count ,aux supervisor count
	 * this method updates the above count directly in display table..
	 * @param qManager
	 * @param tenantGrpId
	 */

	private void getStaffedAuxDetails(QueueManager qManager, Integer tenantGrpId) {
		int queueId	=	0;
		int staffedAgents	=	0;
		int staffedSupervisors	=	0;
		int  aux_agents	=	0;
		int aux_supervisors	=	0;
		String url =  qManager.getURL() + "/GetStaffedAgents";
		String tentgrgid = "tenantGroupID=" + tenantGrpId+"&action=906";                
		String response = HttpURLConnector.postData(url, tentgrgid);

		if (_infoLog) {
			_logger.info("[ getStaffedAuxDetails ] TenantGrp passed ["+ tenantGrpId+"] url fired to get staffed agents [ "+url+"] response fetched is "+response);
			
		}
		if (response != null && !response.equals("")) {
			try {
				JSONObject queueStatsObject = new JSONObject(response);

				JSONArray jsonQueueArr = queueStatsObject.has("QueueStats") ? queueStatsObject.getJSONArray("QueueStats") : null;

				if(jsonQueueArr != null) {
					for (int j = 0; j < jsonQueueArr.length(); j++) {
						JSONObject queueAgentCount = jsonQueueArr
								.getJSONObject(j);
						if (_infoLog) {
							_logger.info("[ getStaffedAuxDetails ] Json Response fetched is from QueueManager is :: "
									+ queueAgentCount);
						}

						queueId = queueAgentCount.getInt("qid");
						staffedAgents = queueAgentCount.getInt("sa");
						staffedSupervisors = queueAgentCount.getInt("ss");
						aux_agents = queueAgentCount.getInt("aux_agents");
						aux_supervisors = queueAgentCount
								.getInt("aux_supervisors");

						/*agent = new AgentDetails(staffedAgents,
								staffedSupervisors, aux_agents, aux_supervisors);
						if (agentMap.containsKey(queueId)) {

							AgentDetails agentDetails = agentMap.get(queueId);
							if (agentDetails.getAux_agents() != aux_agents
									|| agentDetails.getAux_supervisors() != aux_supervisors
									|| agentDetails.getStaffedAgents() != aux_agents
									|| agentDetails.getStaffedSupervisors() != aux_supervisors) {

								updateDisplayTable(queueId, staffedAgents,
										staffedSupervisors, aux_agents,
										aux_supervisors);
							}

						} else {*/

							updateDisplayTable(queueId, staffedAgents,
									staffedSupervisors, aux_agents,
									aux_supervisors);
					/*	}

						agentMap.put(queueId, agent);*/

					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
		
		
	}

	/**
	 * This method calls AgentMonitor servlet to get AgentStats details
	 * @param qManager
	 * @param tenantGrpId
	 */
	private void storeAgentStatsDetails(QueueManager qManager, Integer tenantGrpId) {
		StringBuilder sb 	= new StringBuilder();
		sb.append(Constants.TENANT_GRP_ID).append("=").append(tenantGrpId).append("&").append(Constants.TENANT_GRP_NAME).append("=").append(QMTenantGroupStore.getInstance().getTGDetailsByTGPkey(tenantGrpId));

		if (_infoLog) {
			_logger.info(" [agentStatsDetails] ::Sending request to QM tenant group = " + tenantGrpId);
		}

		String result = qManager.getAgentMonitorDetails(Constants.GET_AGENT_MONITOR_SERVLET, sb ,tenantGrpId+"");
		if (_infoLog) {
			_logger.info(" [agentStatsDetails] :: Result from name for QM = " + result + " for tenant group = " + tenantGrpId);
		}

		AgentMonitorStore.getInstance().addAgentMonitoreData(tenantGrpId, result);

		
	}


	private void  updateDisplayTable(int queueId,int staffedAgents,int staffedSupervisors,int aux_agents,int  aux_supervisors ){

		Connection dbConnection	=	null;
		PreparedStatement	pstmt	=	null;
		/*String agentDetails	=	staffedAgents+" |"+aux_agents ;
		String supervisorDetails	=	staffedSupervisors + "|"+aux_supervisors;*/
		if (_infoLog) {
			_logger.info("Updating display table with parameters :: staffedAgents "+staffedAgents+" aux_agents "+aux_agents+" staffedSupervisors "+staffedSupervisors+" aux_supervisors "+aux_supervisors+" for queuePkey "+queueId);
		}
		try{
			dbConnection	=	MMConnectionPool.getDerbyConnection();
			String query	=	"update APP.QSTATS_DISPLAY_DATA set D200=?,D220=?,D230=?,D240=? where PKEY=?";
			pstmt 		 = dbConnection.prepareStatement(query);

			pstmt.setInt(1,staffedAgents);
			pstmt.setInt(2,aux_agents);
			pstmt.setString(3,Integer.toString(staffedSupervisors));
			pstmt.setString(4,Integer.toString(aux_supervisors));

			pstmt.setInt(5, queueId);
			pstmt.executeUpdate();
			if (_infoLog) {
				_logger.info("Display table updated sucessfully for queuePkey "+queueId);
			}
			// flag	=	true;
		}catch(Exception ex){
			if (_errorLog) {
				_logger.info("Error while updating display table ",ex);
			}

		}finally{

			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("updateDisplayTable ::  Error .", ex);
					}
				}
				pstmt = null;
			}

			if (dbConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("updateDisplayTable :: Error ", ex);
					}
				}
				//dbConnection = null;
			}
		}



	}

}