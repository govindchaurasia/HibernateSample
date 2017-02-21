package com.interactcrm.qstats.initialize;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.derby.drda.NetworkServerControl;

import com.icx.lmverifier.LicenseFactoryProducer;
import com.icx.lmverifier.store.ILicenseReader;
import com.interactcrm.alarm.AlarmGeneratorUtil;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;

import com.interactcrm.qstats.bean.QueueQueueGroupMapping;
import com.interactcrm.qstats.bean.TherSholdBean;
import com.interactcrm.qstats.classes.IQueueStatsManager;
import com.interactcrm.qstats.classes.QueueStatsManagerFactory;
import com.interactcrm.qstats.classes.ThreadCheckerStore;
import com.interactcrm.qstats.constant.QSConstants;
import com.interactcrm.qstats.dao.IntializationDAO;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.qm.QueueManager;
import com.interactcrm.qstats.qm.QueueManagerMap;
import com.interactcrm.qstats.startup.QMTenantGroupStore;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.qstats.threads.AgentDetailsFromQm;
import com.interactcrm.qstats.threads.CampaignDataProcessor;
import com.interactcrm.qstats.threads.DashBoardMasterProcessor;
import com.interactcrm.qstats.threads.ContactPurging;
import com.interactcrm.qstats.util.ActivePropertiesReader;
import com.interactcrm.qstats.version.VersionDOA;
import com.interactcrm.tools.Decrypt;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;


/**
 * Initializer class does the initialization of QueueStats.
 * This class loads necessary data from database and stores it.
 * @author Vipin Singh
 * @version 1.0
 * @since 1.0
 */
public class Initializer {
	private static Initializer _initializer = new Initializer();
	private static Log _logger = new LogHelper(Initializer.class)
	.getLogger(LogModuleFactory.getModule("QueueStatistics"),"Initialization");
	private String _serverID = "";
	private List<Integer> tgList	=	null;
	private static boolean _debugLog 		= false;
    private static boolean _errorLog 		= false;
    private static boolean _infoLog 		= false;
	private boolean enabledCampaignThreads=false;
	private Initializer() {
		
	}

	private InputStream in	=	null;
	@Override
	public String toString() {
		return "Initializer []";
	}

	/**
	 * This is method which will return instance of I{@link Initializer class}.
	 * @return instance of Initializer.
	 */
	public static Initializer getInstance() {
		return _initializer;
	}

	/**
	 * Initializes the QueueStats.
	 * Loads data from database and stores in derby table
	 * Gets the details of TenantGroup, QueueManager and RTCC server
	 * Gets all Tenant Groups.
	 */
	public void init() {
		try {
			if(_infoLog){
				_logger.info("init :: =============== Initializing Server==============================");
			}
			_serverID = com.interactcrm.qstats.util.ActivePropertiesReader.getInstance().getProperty("Active.ID","-1");
			if("-1".equalsIgnoreCase(_serverID)){
				if (_logger.isFatalEnabled()) {
					_logger.fatal("init :: Server ID is not defined correctly. Server ID =[" +
							_serverID + "]");
				}
				return;
			}
			if (_infoLog) {
				_logger.info("Server Id Found in properties is ="+_serverID);
			}
			
			IntializationDAO obj = new IntializationDAO();
			TenantGroupStore.getInstance().createTenantGrpList();
			tgList	=	TenantGroupStore.getInstance().getTenantGrpList();
			
			//Required for refresh functionality
			TenantGroupStore.getInstance().prepareTgObjMap();
			
			//Intializing alaram 
			AlarmGeneratorUtil.getInstance().init(Integer.parseInt(
			ActivePropertiesReader.getInstance().getProperty("Active.ID"))
					, tgList);
			
			int result=VersionDOA.updateModuleRef(_serverID);
			if (_infoLog) {
				_logger.info("No. of rows affected ="+result+" and Current Module Reference = "+VersionDOA.MODULE_RELEASE_REF);
			}
			obj.generateQueueStatsSchema();	
			obj.generateQueueStatsSeed(); 
		    obj.generateQueueGroupSchema();
		    obj.initializeQueryStore();
		 
			intializeQMStore();
			initializeQueueStatManager();
			getAllQueues();
			
			initializeThreadStartup();
		   
			getStaffedAgents(); //Poller ()
			purgingContacts();
			

			//setNoAutoBoot();

			if(_infoLog){
				_logger.info("init :: =============== Server started successfully.==============================");
			}
		} catch (Exception ee) {
			if (_errorLog) {
				_logger.error(ee);
			}
		}
	}


/**
 * To start Queuestats derby in network mode....
 */
	static{
		 if (_logger != null) {
	            _debugLog = _logger.isDebugEnabled();
	            _errorLog = _logger.isErrorEnabled();
	            _infoLog=_logger.isInfoEnabled();
	        }
		
		if(_logger.isDebugEnabled()){
			_logger.debug("Checking Configuration");
		}
		PreparedStatement statement = null;
		Connection connection = null;
		String connectionName=null;
		String userName=null,password=null,serverDomain=null;
		int port=0;
		try {
			if(_logger.isInfoEnabled()){
				_logger.info(Utility.getAppHome()  + File.separator + "primarydb");
			}
			System.setProperty("derby.system.home", Utility.getAppHome() + File.separator + "primarydb"); 
			try{
				connection=MMConnectionPool.getDBConnection();

				ResultSet resultSet;
				if(connection!=null){
					String query="SELECT Connection_Name,DBUsername,DBPassword,ServerDomain,DatabasePort from database_connections where Connection_Name= 'QSDerby'";
					statement=connection.prepareStatement(query);
					resultSet 		= statement.executeQuery();
					if(resultSet != null){
						while(resultSet.next()){
							connectionName=resultSet.getString("Connection_Name");
							userName=resultSet.getString("DBUsername");
							password=resultSet.getString("DBPassword");
							serverDomain=resultSet.getString("ServerDomain");
							port=resultSet.getInt("DatabasePort");
							if(_logger.isDebugEnabled()){
								_logger.debug("ResultSet Found username "+userName+"  connectionName  "+connectionName);
							}
						}
					}else{
						if(_logger.isDebugEnabled()){
							_logger.debug("No ResultSet");
						}
					}

				}
				else{
					if(_logger.isErrorEnabled()){
						_logger.error("NetworkServerControler:: Connection Not Found");
					}
				}
			} catch (Exception e) {
				if (_logger.isErrorEnabled()) {
					_logger.error("NetworkServerControler ::  Error  " , e);
				}
			} finally {

				if (statement != null) {
					try {
						statement.close();
					} catch (Exception ex) {
						if (_logger.isErrorEnabled()) {
							_logger.error(
									"NetworkServerControler ::  Error ", ex);
						}
					}
					statement = null;
				}

				if (connection != null) {
					try {
						MMConnectionPool.freeConnection(connection);
					} catch (Exception ex) {
						if (_logger.isErrorEnabled()) {
							_logger.error(
									"NetworkServerControler :: Error", ex);
						}
					}
					connection = null;
				}
			}
			NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),port, userName, Decrypt.decrypt(password));
			//NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),1527, "root", "root");

			//		NetworkServerControl server = new NetworkServerControl();

			server.start(null);

			try {
				server.setMaxThreads(25);
			} catch (Exception e) {
				e.printStackTrace();
			}


			server.ping();
			if(_logger.isInfoEnabled()){
				_logger.info("init :: =============== Server started successfully.============================== "
						+"\r\n----"+server.getCurrentProperties()+"\r\n"+server.getRuntimeInfo() + "\r\n" +server.getSysinfo()+ "\r\n\r\n\r\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * Future Use
	 * Setting derby properties to No Auto boot
	 * This is done to avoid automatic startup of derby database which causes exception.
	 * @throws Exception
	 */
	public static void setNoAutoBoot() throws Exception {
		CallableStatement cs = null;
		Connection conn = MMConnectionPool.getDerbyConnection();
		if (_logger.isInfoEnabled()) {
			_logger.info("setNoAutoBoot:: SP called for setting No Auto Boot.");
		}
		if (conn != null) {
			try {
				cs = conn
						.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
				cs.setString(1, "derby.database.noAutoBoot");
				cs.setString(2, "true");
				cs.execute();
				if (_logger.isDebugEnabled()) {
					_logger.debug("setNoAutoBoot:: Done with processing of Auto Boot.");
				}

			} catch (Exception e) {
				throw new Exception("Error setting autoboot property", e);
			}
			if (cs != null) {
				try {
					cs.close();
				} catch (Exception e) {
				}
			}
			try {
				MMConnectionPool.freeDerbyConnection(conn);
			} catch (Exception e) {
				if (_logger.isErrorEnabled()) {
					_logger.error("setNoAutoBoot::", e);
				}
			}
		} else {
			throw new Exception("Error retrieving connection");
		}
	}

	

	/**
	 * This method matched the pattern and replaces the data (queueid/tenantGroupList)to form a query 
	 * @param query
	 * @return
	 */
	

	/**
	 * This method updates server refersh time in database , when refersh request is received..
	 * @param serverId
	 * @return
	 */
	public String updateServerLoadTime(int serverId){
		Connection connection = null;
		ResultSet rs = null;
		CallableStatement cs = null;
		String refreshedDate = "";
		if (_debugLog) {
			_logger.debug("updateServerLoadTime:: Initialized RM with server Id = " + serverId);
		}
		try {
			connection =  MMConnectionPool.getDBConnection();
			if (connection != null) {
				cs = connection.prepareCall("{call RGM_UPDATE_REFRESH_TIME(?)}");
				cs.setInt(1, serverId);
				cs.execute();
				rs =  cs.getResultSet();
				while (rs.next()) {
					refreshedDate = rs.getString("UPDATED_TIME");
				}
				if (_infoLog) {
					_logger.info("updateServerLoadTime ::Routing Manager loaded at =" + refreshedDate);
				}
			} else {
				if (_errorLog) {
					_logger.error("updateServerLoadTime :: Database Connection is null");
				}
			}
		} catch (SQLException sqle) {
			if (_errorLog) {
				_logger.error("updateServerLoadTime::", sqle);
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("updateServerLoadTime::", e);
			}
		} finally {
			if (cs != null) {
				try {
					cs.close();
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error("updateServerLoadTime::", e);
					}
				}
				cs = null;
			}
			try {
				MMConnectionPool.freeConnection(connection);
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error("updateServerLoadTime:: Error while closing connection", e);
				}
			}
		}
		return refreshedDate;
	}
	
	/**
	 * Loads tgid and tgname and configured QM details
	 */
	public void intializeQMStore() {

		Connection _connection 	= null;
		ResultSet _resultSet 	= null;
		CallableStatement _cs 	= null;

		try {
			_connection = MMConnectionPool.getDBConnection();
			if (_connection == null) {
				if (_errorLog) {
					_logger.error("intializeQMStore::Failed to get connection to database.");
				}
				return;
			}
			_cs = _connection.prepareCall("{call GET_SERVER_DETAILS_BY_TG()}");
			_resultSet = _cs.executeQuery();

			QMTenantGroupStore qmTenantGrpStoreInstance = QMTenantGroupStore.getInstance();
			QueueManagerMap mapInstance = QueueManagerMap.getInstance(); 

			while (_resultSet.next()) {

				int qmId = _resultSet.getInt("QM_ID");	
				
				qmTenantGrpStoreInstance.addTenantGroupDetails(_resultSet.getInt("PKEY"), _resultSet.getString("NAME"));
				
				qmTenantGrpStoreInstance.addQMTGMapping(_resultSet.getInt("PKEY"), qmId);

			
				if (mapInstance.containsKey(qmId)) {
					continue;
				}								

				QueueManager queueMgr = new QueueManager(qmId, _resultSet.getString("QM_URL"));
				mapInstance.addQM(qmId, queueMgr);	

			}
			if (_debugLog) {
				_logger.debug("Maps formed are QMmap= "
						+ mapInstance.getQMMap()+" \n QMTenantGrpStore map "+qmTenantGrpStoreInstance.getQMTGMap()+" tgDeatails map "+qmTenantGrpStoreInstance.getTGDetails());
			}
		} catch (SQLException sqle) {
			if (_errorLog) {
				_logger.error("intializeQMStore::", sqle);
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("intializeQMStore::", e);
			}
		} finally {
			try {
				if (_cs != null) {
					_cs.close();
				}
				if (_resultSet != null) {
					_resultSet.close();
				}
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error("intializeQMStore::", e);
				}
			}
			try {
				MMConnectionPool.freeConnection(_connection);
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error("intializeQMStore::", e);
				}
			}
		}
	}

	private void getStaffedAgents(){
		new Thread(new AgentDetailsFromQm()).start();
	}

	
	private void purgingContacts() {
		Thread t=new Thread(new ContactPurging());
		t.setName("Contact Purging");
		t.start();
	}
	/**
	 * Starts fetcher and processor threads queuegroup wise
	 */
	public void initializeQueueStatManager(){			

		Connection dbConnection 	= null;
		PreparedStatement statement = null;
		ResultSet resultSet			= null;

		
		try {
			if (_infoLog) {
				_logger.info("initializeQueueStatManager:: Starting threads");
			}			

			dbConnection = MMConnectionPool.getDerbyConnection();

			if (dbConnection != null) {
				String qgSelect 	= "SELECT DISTINCT queuegroup_pkey AS QG_PKEY,channel AS CHANNEL_ID,tenantgroup_pkey AS TENANTGROUP_ID FROM APP.QSTATS_RAW_DATA";
				statement 			= dbConnection.prepareStatement(qgSelect);
				resultSet					= statement.executeQuery();

				if(resultSet != null){
					while(resultSet.next()){
						int channelId	=	resultSet.getInt("CHANNEL_ID");
						int qgrpPkey	=	resultSet.getInt("QG_PKEY");
						int tgId		=	resultSet.getInt("TENANTGROUP_ID");
						
						IQueueStatsManager absstatsUpdater = QueueStatsManagerFactory.getInstance(channelId, tgId, qgrpPkey);

						if(absstatsUpdater!=null ){
							if (_infoLog) {
								_logger.info("Scheduled a Thread for QueueGroup = " +qgrpPkey +" channelId ["+channelId+"] TenanGroupId ["+tgId+"]");
							}         

							//TenantGroupStore	tgGroup	=	new TenantGroupStore().getTenantGroupFromMap(tgId);
						
						/*	absstatsUpdater.startFetching(tgGroup);
							absstatsUpdater.startProcessing(tgGroup);*/
							TenantGroupObj tg	=	TenantGroupStore.getInstance().getTenantGroupObjFromMap(tgId);
						
							absstatsUpdater.startFetching(tg);
							absstatsUpdater.startProcessing(tg);


						}else{
							if (_infoLog) {
								_logger.info("No Thread Started for channelId ----" +qgrpPkey +"channelId ["+channelId+"] TenanGroupId ["+tgId+"]");
							} 
						}
					}
				}

			} else {
				if (_errorLog) {
					_logger.error("initializeQueueStatManager :: Error fetching db connection in cleaning "
							+   " table");
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("initializeQueueStatManager :: E Error in cleaning "
						+  " table.", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("initializeQueueStatManager ::  Error in cleaning "
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
						_logger.error("initializeQueueStatManager :: Error in cleaning "
								+  " table.", ex);
					}
				}
			
			}
		}
	}

	/**
	 * This method starts the thread to get campaign related live data
	 * Threads are run per campaign...
	 */
	public void startCampaignProcessorThreads() {
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
			
			if(_infoLog){
				_logger.info("[campaignProcessor] Intializing and starting thread to process campaign data");
			}
			
			in	=	new FileInputStream(Utility.getAppHome()+File.separator+"SleepInterval.properties");
			prop.load(in);
			connection	=	MMConnectionPool.getDerbyConnection();
			if(connection !=null){
				String query	=	"SELECT campaign_pkey , campaign_name , tenantgroup_pkey from APP.qstats_campaign_data";
				pstmt	=	connection.prepareStatement(query);
				resultSet	=	pstmt.executeQuery();
				while(resultSet.next()){
					campaignId	=	resultSet.getInt("campaign_pkey");
					campaignName	=	resultSet.getString("campaign_name");
					tenantGroupId	=	resultSet.getInt("tenantgroup_pkey");
					
					TenantGroupObj tg	=	TenantGroupStore.getInstance().getTenantGroupObjFromMap(tenantGroupId);
					
					if(_infoLog){
						_logger.info("[campaignProcessor] Starting thread for campaign id "+campaignId);
					}
					new Thread (new CampaignDataProcessor(campaignId, campaignName, Integer.parseInt(prop.getProperty("Sleeptime")),tg,tenantGroupId)).start();
				}
				
			}
		}catch(IOException e){
			if(_errorLog){
				_logger.error("[campaignProcessor] Some exception occured while reading sleeptime interval from properties file",e);
			}
			
		}catch(SQLException ex){
			if(_errorLog){
				_logger.error("[campaignProcessor] SQL exception occured",ex);
			}
			
		}catch(Exception e){
			if(_errorLog){
				_logger.error("[campaignProcessor] Some exception ",e);
			}
		}finally{
			if(connection !=null){
				MMConnectionPool.freeDerbyConnection(connection);
			}
		}
		
	}
	/**
	 * This method starts the dashboard thread , this is specific to fanuc requirement only
	 * The thread will start only if flag is set in properties file
	 */
	private void initializeDashBoardManager(){			

		Connection dbConnection 	= null;
		PreparedStatement statement = null;
		ResultSet resultSet			= null;
		int channel_id = 0;
		final List <DashBoard> dashboardList	=	new ArrayList <DashBoard>();
		try {			

			if (_infoLog) {
				_logger.info("initializeDashBoardManager::");
			}			
			dbConnection = MMConnectionPool.getDBConnection();

			
			if (dbConnection != null) {
				//String query	=	"select dashboard_group_id,channel_id,workgroup_id,queue_id,mq_queuegroup_Id,T040,T041,T042,T050,T051,T052 from dashboard_groups_queue_mapping join mq_queues on queue_id= mq_pkey join mqt_queue_threshold on queue_id=mqt_queue_threshold.pkey";
				String qgSelect 	= "SELECT dashboard_group_id,workgroup_id from dashboard_groups_queue_mapping ";
				statement 			= dbConnection.prepareStatement(qgSelect);
				resultSet					= statement.executeQuery();
				if (_infoLog) {
					_logger.info("initializeDashBoardManager ::Query fired to get dashgroups ids---> "+qgSelect);
				}
				
				if(resultSet != null){
					while(resultSet.next()){
							
						int dashboard_group_id		=	resultSet.getInt("dashboard_group_id");
						int workgroup_id			=	resultSet.getInt("workgroup_id");						
						List<Integer> queueList		= 	getQueues(dashboard_group_id);
						
						updateRawWithDashgroupId(dashboard_group_id,queueList);
						TherSholdBean thersholdbean	=	getThersholdValues(queueList);
					
						if (_infoLog) {
							_logger.info("initializeDashBoardManager :: dashboard_group_id [" +dashboard_group_id +"]  channelId ["+channel_id+"] queueList ["+queueList+"]" );
						}
						
						dashboardList.add(new DashBoard(dashboard_group_id,queueList,thersholdbean));
						
		
					}
					
				}
				
				for(DashBoard dashboard:dashboardList){								
					
					new Thread(
							new DashBoardMasterProcessor(dashboard.getDashboard_group_id(),dashboard.getQueueList(),
									dashboard.getThersholdbean())											
					).start();
				}

				
			} else {
				if (_errorLog) {
					_logger.error("initializeDashBoardManager :: Error fetching db connection.");
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("initializeDashBoardManager :: E Error .", e);
			}	
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("initializeDashBoardManager ::  Error .", ex);
					}
				}
				statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("initializeDashBoardManager :: Error in relasing primary db connection "
								+  " table.", ex);
					}
				}
				//dbConnection = null;
			}
		}

	}

	public int getServerId(){
		return Integer.parseInt(_serverID);
	}
	
	public List<Integer>getTenantGrpList(){
		return tgList;
	}
	

	/**
	 * This method returns list of queues configured aginst the provided dashboardgroup
	 * @param dashgroupId
	 * @return
	 */
	private List<Integer> getQueues(int dashgroupId){
		Connection dbConnection=null;
		
		ResultSet queuesSet = null	;	
		ArrayList<Integer> queueList	=	new ArrayList<Integer>();
		dbConnection = MMConnectionPool.getDBConnection();
		try{
			if(dbConnection!=null){
				String getQueues="SELECT queue_id from dashboard_groups_queue_mapping where dashboard_group_id=? ";
				PreparedStatement queuesStatement=dbConnection.prepareStatement(getQueues);
				queuesStatement.setInt(1,dashgroupId );
				queuesSet	=	queuesStatement.executeQuery();
				while(queuesSet.next()){

					queueList.add(queuesSet.getInt("queue_id"));
					createQueueQueueGroupMap(queuesSet.getInt("queue_id"));
					

				}
			}else{
				if (_errorLog) {
					_logger.error("getQueues :: Error fetching db connection in cleaning "
							+   " table");
				}
			}
		}catch(Exception e){
			if (_errorLog) {
				_logger.error("Error excuating query-->getQueues",e);
			}
		}finally{
			if(dbConnection!=null){
				try{
					MMConnectionPool.freeConnection(dbConnection);
				}catch(Exception ex){
					if (_errorLog) {
						_logger.error("getQueues :: Error in relasing primary db connection "
								+  " table.", ex);
					}
				}
				//dbConnection = null;
			}
		}
		return queueList;
	}
	
	/**
	 * This method returns thersholdValues set for provided queuelist
	 * list is passed for future used , currently same thershold values will be set for all the queues
	 * @param queueList
	 * @return
	 */
	private TherSholdBean getThersholdValues(List<Integer> queueList){
		Connection derbyConnection	=	null;
		PreparedStatement statement	=	null;
		ResultSet resultSet	=	null;
		
		Integer queue=0;
		TherSholdBean therSholdBean	=	new TherSholdBean();
		for (Integer queueID : queueList) {
			queue=queueID;
		}
		//List<TherSholdBean> therSholdList	=	new ArrayList();	
		derbyConnection=MMConnectionPool.getDerbyConnection();
		try{
			if(derbyConnection != null){
				String selectThersholds = "SELECT T051 as T051,T052 as T052,T041 as T041,T042 as T042 FROM APP.QSTATS_RAW_DATA WHERE pkey= ?";
				statement = derbyConnection.prepareStatement(selectThersholds);
				statement.setInt(1, queue);
				if (_debugLog) {
					_logger.debug("getThersholdValues::retrieving resultSEt");
				}
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
				
					therSholdBean.setAboveThersholdColorForContacts(resultSet.getString("T052"));
					therSholdBean.setBelowTherSholdColorForContacts(resultSet.getString("T051"));
					therSholdBean.setBelowTherSholdColorForDuration(resultSet.getString("T041"));
					therSholdBean.setAboveThersholdColorForDuration(resultSet.getString("T042"));
				
				
				}
				if (_debugLog) {
					_logger.debug("getThersholdValues::therSholdBeanFormed"+therSholdBean);
				}
			}else{
				if (_debugLog) {
					_logger.debug("getThersholdValues::derby database connection NULL--");
				}
			}
			
			
		}catch(Exception ex){
			if (_debugLog){
				_logger.debug("Error retrieving thershold values-->",ex);
			}
			
		}finally{
			try{
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("Error while relasing the derby connection ",ex);
				}
			}
		}
		return therSholdBean;
		
	}
	/**
	 * This method updates the dashboard group id against queue in raw table
	 * @param dashgroupId
	 * @param queueList
	 */
	private void updateRawWithDashgroupId(Integer dashgroupId,List<Integer> queueList ){
		Connection derbyConnection	=	null;
		PreparedStatement statement=	null;
		derbyConnection	=	MMConnectionPool.getDerbyConnection();
		try{
			if(derbyConnection !=	null){
				String selectFromRaw="UPDATE APP.qstats_raw_data set D180=? WHERE  pkey IN ($P{queueList}) ";
				String query = parseQuery(selectFromRaw,queueList);
				statement = derbyConnection.prepareStatement(query); 
				statement.setInt(1,dashgroupId);
				statement.executeUpdate();
				if (_debugLog) {
					_logger.debug("raw table updated with dashgroupId "+dashgroupId+" for queueList "+queueList);
				}

			}else{
				if (_debugLog) {
					_logger.error("DerbyConnection null---> can not fetch the connection .");
				}
			}


		}catch(Exception ex){
			if (_debugLog) {
				_logger.error("Error while updating raw table with dashgroupIds.",ex);
			}
		}finally{
			try{
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("createQueueQueueGroupMap :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
			//derbyConnection =	null;

		}
	}
	
	
	private StringBuilder getqueueList(List<Integer> queueList) {
		Iterator<Integer> it = queueList.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			sb.append(it.next()).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		if (_debugLog) {
			_logger.debug("getQueueList--> "+sb);
		}
		return sb;
	}
	
	
	private String parseQuery(String selectFromRaw,List<Integer> queueList) {

		try {
			String re1 = "(\\$)"; // Any Single Character 1
			String re2 = "(P)"; // Variable Name 1
			String re3 = "(\\{)"; // Any Single Character 2
			String re4 = "(queueList)";
			String re5 = "(\\})"; // Any Single Character 3
			String regex = null;

			Pattern p = Pattern.compile(re1 + re2 + re3 + re4 + re5,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher m = p.matcher(selectFromRaw);

			while (m.find()) {
				String c1 = m.group(1);
				String var1 = m.group(2);
				String c2 = m.group(3);
				String alphanum = m.group(4);
				String c3 = m.group(5);
				regex = c1.toString() + var1.toString() + c2.toString()
						+ alphanum.toString() + c3.toString();
			}
			selectFromRaw = selectFromRaw.replace(regex,getqueueList(queueList));
			if (_debugLog) {
				_logger.debug("parseQuery:: query is " + selectFromRaw);
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("parseQuery:: Exception ", e);
			}
		} finally {
			if (_debugLog) {
				_logger.debug("parseQuery:: Finally of queryAnalyser--"
						+ selectFromRaw);
			}
		}

		return selectFromRaw;

	}
	/**
	 * This method saves queuegroup against queuegroup id 
	 * This is required for fanuc requitement .
	 * For fanuc requirement we are using logical entity dashobard group and both voice and email queues can belong to same dashoabrd
	 * 
	 * @param queueId
	 */
	private void createQueueQueueGroupMap(Integer queueId){
		Connection derbyConnection	=	null;
		ResultSet selectResult	=	null;
		PreparedStatement statement =	null;
		QueueQueueGroupMapping queueGroupMap=QueueQueueGroupMapping.getInstance();
		derbyConnection=MMConnectionPool.getDerbyConnection();
		try{
			if(derbyConnection 	!=	null){
				String query	=	"SELECT queuegroup_pkey AS QG_PKEY from APP.QSTATS_RAW_DATA WHERE pkey = ? and channel!=2 ";
				statement 			= derbyConnection.prepareStatement(query);
				statement.setInt(1,queueId);
				selectResult					= statement.executeQuery();

				if(selectResult != null){
					while(selectResult.next()){
						queueGroupMap.addEntry(queueId, selectResult.getInt("QG_PKEY"));
					}

					
				}else{
					if (_debugLog) {
						_logger.error("createQueueQueueGroupMap :: ResultSet is Empty:"); 
					}
				}

			}else{
				if (_errorLog) {
					_logger.error("createQueueQueueGroupMap :: Error fetching derby connection:"); 
				}
			}
			

		}catch(Exception ex){
			if (_errorLog) {
				_logger.error("Error in creating Queue-QueueGroup Map",ex);
			}

		}finally{
			try{
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("createQueueQueueGroupMap :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
			//derbyConnection =	null;
		}
	}
	
	
	public void getAllQueues()
	{
		Connection derbyConnection	=	null;
		ResultSet selectResult	=	null;
		PreparedStatement statement =	null;
		derbyConnection=MMConnectionPool.getDerbyConnection();
		try{
			if(derbyConnection 	!=	null){
				String query	=	"SELECT pkey AS QUEUE from APP.QSTATS_RAW_DATA WHERE channel!=2";
				statement 			= derbyConnection.prepareStatement(query);
				selectResult					= statement.executeQuery();

				if(selectResult != null){
					while(selectResult.next()){
						createQueueQueueGroupMap(selectResult.getInt("QUEUE"));
					}
				}else{
					if (_debugLog) {
						_logger.error("getAllQueues :: ResultSet is Empty:"); 
					}
				}

			}else{
				if (_errorLog) {
					_logger.error("getAllQueues :: Error fetching derby connection:"); 
				}
			}
	
		}catch(Exception ex){
			if (_errorLog) {
				_logger.error("Error in creating getAllQueues",ex);
			}

		}finally{
			try{
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}catch(Exception ex){
				if (_errorLog) {
					_logger.error("createQueueQueueGroupMap :: Error in relasing primary db connection "
							+  " table.", ex);
				}

			}
		}
	
	}
	
	
	private void initializeThreadStartup()
	{
		new ThreadCheckerStore();
		//enabledCampaignThreads=ThreadCheckerStore.getInstance().isCampaignThreadEnabled();
		enabledCampaignThreads=false;
			if(enabledCampaignThreads)
			{
				if (_infoLog) {
					_logger.info("[Initialize] Running Campaign Processor thread..");
				}
			//	startCampaignProcessorThreads();
			}
			else
			{
				if (_infoLog) {
					_logger.info("[Initialize] Campaign Processor thread is disabled..");
				}
			}
			
			/**
			 * This will start fanuc dashboard thread depending on property configured in properties file
			 */
			try{
				Properties prop	=	new Properties();
				in =new FileInputStream(Utility.getAppHome()+File.separator+"FanucDashboard.properties");
				prop.load(in);
				boolean isTure	=	Boolean.parseBoolean(prop.getProperty("startDashboardThread","false"));
				if(isTure){
					if (_infoLog) {
						_logger.info("[Initialize] Running fanuc dashboard thread..");
					}
					initializeDashBoardManager();
				}else{
					if (_infoLog) {
						_logger.info("[Initialize] Fanuc dashboard thread is disabled..");
					}
				}
			}catch(Exception e){
				if (_errorLog) {
					_logger.error("[Intialize] Some excepton occured while reading fanuc dashboard property from properties file..", e);
					
				}
				
			}finally{
				if(in !=null){
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				 }
				}
			}
			for(Integer tgId:tgList)
			{
				try {
					ILicenseReader license = LicenseFactoryProducer.getLicenseFactory().getLicense(tgId);
					boolean isCBCfeatureEnabled = license!=null?license.isFeatureEnabled(QSConstants.AVAYACBCFEATURE,false):false;
				   if(_debugLog)
					{
						_logger.debug("Avaya Callback Feature is "+isCBCfeatureEnabled+ " for tenantGroup "+tgId);
					}
				} catch(Exception e){
					if (_debugLog) {
						_logger.debug("[Intialize] No license found for tgId "+tgId);
						
					}
				
				}
			}
			
	}
	
	private class DashBoard{
		private int dashboard_group_id =0;
		private List<Integer> queueList = null;
		TherSholdBean thersholdbean = null;
		
		/**
		 * @param dashboard_group_id
		 * @param queueList
		 * @param thersholdbean
		 */
		public DashBoard(int dashboard_group_id, List<Integer> queueList,
				TherSholdBean thersholdbean) {
			super();
			this.dashboard_group_id = dashboard_group_id;
			this.queueList = queueList;
			this.thersholdbean = thersholdbean;
		}
		/**
		 * @return the dashboard_group_id
		 */
		public int getDashboard_group_id() {
			return dashboard_group_id;
		}
		/**
		 * @param dashboard_group_id the dashboard_group_id to set
		 */
		public void setDashboard_group_id(int dashboard_group_id) {
			this.dashboard_group_id = dashboard_group_id;
		}
		/**
		 * @return the queueList
		 */
		public List<Integer> getQueueList() {
			return queueList;
		}
		/**
		 * @param queueList the queueList to set
		 */
		public void setQueueList(List<Integer> queueList) {
			this.queueList = queueList;
		}
		/**
		 * @return the thersholdbean
		 */
		public TherSholdBean getThersholdbean() {
			return thersholdbean;
		}
		/**
		 * @param thersholdbean the thersholdbean to set
		 */
		public void setThersholdbean(TherSholdBean thersholdbean) {
			this.thersholdbean = thersholdbean;
		}

		
	}
}
