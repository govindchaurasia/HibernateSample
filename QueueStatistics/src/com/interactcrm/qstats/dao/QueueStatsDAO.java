package com.interactcrm.qstats.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.ChannelProperties;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.util.logging.LogHelper;



public class QueueStatsDAO {

    private static Log _logger = new LogHelper(QueueStatsDAO.class).getLogger(LogModuleFactory.getModule("QueueStatistics"),"Initialization");
    private static boolean _debugLog = false;
    private static boolean _errorLog = false;
    private static boolean _infoLog = false;

    /**
     * Creates AgentDataDAO object and creates logger object for the same.
     */
    public QueueStatsDAO() {
    	if(_logger != null){
    		_debugLog = _logger.isDebugEnabled();
    		_errorLog =  _logger.isErrorEnabled();
    		_infoLog  = _logger.isInfoEnabled();
    	}
    }
    private boolean isResultSetEmptyOrNull(ResultSet resultSet)
			throws SQLException {
		if (resultSet == null) {
			return true;
		}
		boolean next = !resultSet.next();

		// Reverting things done by next()
		resultSet.beforeFirst();
		return next;
	}
    /**
     * Future use
     * @param server_id
     * @param channel_id
     * @return
     */
    public ChannelProperties getChannelProperties(int server_id, int channel_id)
			{

    	_logger.info("getChannelProperties In");
    	_logger.debug(String.format("getChannelProperties Params : %d %d",
				server_id, channel_id));

		// to hold channel properties
		Map<String, String> map = null;

		// sql stuff
		Connection conn = null;
		CallableStatement statement = null;
		ResultSet resultSet = null;

		try {
		// you can get the conncetion from your utiltity
			conn = MMConnectionPool.getDBConnection();
			// better to have String.format or ????, setString()
			String sp = "{ call cdu_getCDUChannelProperties(?,?) }";
			statement = conn.prepareCall(sp, ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			statement.setInt(1, server_id);
			statement.setInt(2, channel_id);
			resultSet = statement.executeQuery();
			// if didnt not get any records , why go ahead , then stop here !!
			// added to check if records are empty
			if (isResultSetEmptyOrNull(resultSet)) {
				
				if (_errorLog) {
					_logger.error(
							"No Properties found in database for channel-id: "
									+ channel_id);
				}
			}

			// lazy int of map
			// map with initial properties 10, as properties seems to be 5-10
			map = new HashMap<String, String>(10);
			while (resultSet.next()) {
				map.put(resultSet.getString(1), resultSet.getString(2));
			}
			_logger.debug("getChannelProperties Got channel properties " + map);
		} catch (SQLException ex) {
			
		}  catch (Exception ex) {
			// too generic will be caught here
			
		} finally {
			// too much of Boiler Code, isnt it ??
			try {
				if (conn != null) {
					MMConnectionPool.freeConnection(conn);
					conn.close();
				}
				if (statement != null) {
					statement.close();
				}
				if (resultSet != null) {
					resultSet.close();
				}
			} catch (SQLException ex) {
				
			}
			}
	
		return new ChannelProperties(map);
	}
	public  List<Integer> getTenantGroupList(){
		
		Connection dbConnection 	=	null;
		PreparedStatement statement =	null;
		ResultSet rs				=	null;
		List<Integer> tgList			= 	new ArrayList<Integer>();
	
		try {
						
			dbConnection = MMConnectionPool.getDBConnection();
			if (dbConnection != null) {
				String queuequery = "select tenant_group_pkey as 'TG_ID' from mspm_server_tenantgroup_mapping where server_pkey='"+Initializer.getInstance().getServerId()+"';";
				if (_infoLog) {
					_logger.info("getTenantGroupList::  = Query Fired"+queuequery);
				}
				statement 			=   dbConnection.prepareStatement(queuequery);
				rs					=   statement.executeQuery();
				while(rs.next()){
					
					//tsStore.putToTgIdTenangGrpMap(rs.getInt("TG_ID"), new TenantGroupStore());
					tgList.add(rs.getInt("TG_ID"));
				}
			} else {
				if (_errorLog) {
					_logger.error("getTenantGroupList :: Error fetching db connection in cleaning "
							+   " table");
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("getTenantGroupList :: "
						+  " ", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("getTenantGroupList ::   "
								+  " ", ex);
					}
				}
				statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("getTenantGroupList :: "
								+  " ", ex);
					}
				}
				dbConnection = null;
			}
		}
		return tgList;
		
	}
	
	
	public StringBuilder getTenantGroupsList(){
		
		Connection dbConnection 	=	null;
		PreparedStatement statement =	null;
		ResultSet rs				=	null;
		List<String> tgList			= 	new ArrayList<String>();
		 StringBuilder sb 			= new StringBuilder();
		try {
					
			dbConnection = MMConnectionPool.getDBConnection();
			if (dbConnection != null) {
				String queuequery = "select tenant_group_pkey as 'TG_ID' from mspm_server_tenantgroup_mapping where server_pkey='"+Initializer.getInstance().getServerId()+"';";
				if (_infoLog) {
					_logger.info("getTenantGroupList::  = Query Fired "+queuequery);
				}
				statement 			=   dbConnection.prepareStatement(queuequery);
				rs					=   statement.executeQuery();
				while(rs.next()){
					tgList.add(rs.getString("TG_ID"));
				}
			} else {
				if (_errorLog) {
					_logger.error("getTenantGroupList :: Error fetching db connection in cleaning "
							+   " table");
				}
			}
			Iterator it			= tgList.iterator();
	        
	        while(it.hasNext()){
	            sb.append(it.next()).append(",");
	        }
	        sb.deleteCharAt(sb.length()-1) ;
	        if (_infoLog) {
				_logger.info("getTenantGroupList::  = List of TenantGroup Comma Separated ["+sb+"]");
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("getTenantGroupList :: "
						+  " ", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("getTenantGroupList ::   "
								+  " ", ex);
					}
				}
				statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("getTenantGroupList :: "
								+  " ", ex);
					}
				}
				dbConnection = null;
			}
		}
		return sb;

	}

	
}
