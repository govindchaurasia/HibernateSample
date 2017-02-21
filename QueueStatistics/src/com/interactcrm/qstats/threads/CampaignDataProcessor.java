package com.interactcrm.qstats.threads;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.TenantGroupObj;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;


/**
 * This class implements {@link Runnable} interface
 * the thread gets live data for all campaigns and updates in qstats_campaign_data derby table
 * The data is then populated on campaign dashboard screenshot....
 * @author vishakha
 *
 */
public class CampaignDataProcessor implements Runnable {

	private int campId;
	private String campName;
	private Log _logger	=	null ;
	private boolean _debugLog	=	false;
	private boolean _errorLog	=	false;
	private boolean _infoLog	=	false;
	private int processorSleepTime	=	0;
	private  Properties prop	= null;
	private  FileInputStream in	=	null;
	private TenantGroupObj tgObj = null;
	public CampaignDataProcessor(int campId , String campName , int processorSleepTime,TenantGroupObj tgObj, int tenantGroupId){
		this.campId	=	campId;
		this.campName	=	campName;
		this.processorSleepTime	=	processorSleepTime;
		this.tgObj	=	tgObj;
		_logger 				= new LogHelper(CampaignDataProcessor.class).getLogger(LogModuleFactory.getModule("CampaignDataProcessor"), String.valueOf(tenantGroupId));		
        
        if (_logger != null) {
            _debugLog = _logger.isDebugEnabled();
            _errorLog = _logger.isErrorEnabled();
            _infoLog = _logger.isInfoEnabled();
        }
        
        
    	prop	=	new Properties();
		try {
			in	=	new FileInputStream(Utility.getAppHome()+File.separator+"CampaignProcessor.properties");
			prop.load(in);
		
		} catch (FileNotFoundException e) {
			if(_errorLog){
				_logger.error("Some exception occured while reading queries from properties file..", e);
			}
		} catch (IOException e) {
			if(_errorLog){
				_logger.error("Some exception occured", e);
			}
		}finally{
			try{
				in.close();
			}catch(Exception e){
				
			}
		}
	}
	
	@Override
	public void run() {
		
		while(tgObj.isRunning()){
			
			getCampaignData(campId);
			fetchDispositionTypes(campId);
		
			try{
				if(_debugLog){
					_logger.debug("[run] Thread sleeping for ["+processorSleepTime+"] seconds");
				}
				Thread.sleep(processorSleepTime *1000);
			}catch(Exception ex){
				if(_errorLog){
					_logger.error("[updateCampaignTable] Some exception occured", ex);
				}
			}
		}
		

	}
	/**
	 * Get campaing data from primary database for e.g live agents , total calls , good connects etc 
	 * @param campId
	 */
	private void getCampaignData(int campId) {
		
		PreparedStatement	pstmt	=	null;
		Connection	dbConnection	=	null;
		ResultSet rSet	=	null;
		ArrayList<String> insertQueryList	=new ArrayList<String>();
		if(_debugLog){
			_logger.debug("[updateCampaignTable] updating campaign derby table with live data");	
		}
		
		try{
			dbConnection	=	MMConnectionPool.getDBConnection();
			if(dbConnection != null){
				String query	=	prop.getProperty("queryForData");
				if(_debugLog){
					_logger.debug("[updateCampaignTable] Firing query for campaignId ["+campId+"]");
				}
				pstmt	=	dbConnection.prepareStatement(query);
				pstmt.setInt(1, campId);
				rSet	=	pstmt.executeQuery();
				
			//	StringBuilder columnValue = new StringBuilder()	;
				String columnValue="";
				StringBuilder columnName = new StringBuilder();
				ResultSetMetaData	rsmd	=	null;
				if(rSet != null){
					rsmd	=	rSet.getMetaData();
					while(rSet.next()){
						
						for(int i=1 ; i<=rsmd.getColumnCount();i++){
							
							if(rsmd.getColumnType(i) == Types.VARCHAR){
								columnValue = "'"+rSet.getString(rsmd.getColumnName(i))+"'";
								
								
							}
						
							if(rsmd.getColumnType(i) == Types.INTEGER){
								columnValue	=	rSet.getString(rsmd.getColumnName(i));
								
							}
							
							if(rsmd.getColumnType(i) == Types.FLOAT){
								columnValue	=	rSet.getString(rsmd.getColumnName(i));
								
							}
							columnName.append(rsmd.getColumnName(i)).append("=").append(columnValue).append(",");
								
						}
						String columnNames 		= columnName.substring(0,columnName.length()-1);
						StringBuilder finalString=new StringBuilder();
						
						 finalString.append("UPDATE APP.qstats_campaign_data SET ")
						 .append(columnNames)
						 .append(" WHERE campaign_pkey=")
						 .append(campId);
						
						insertQueryList.add(finalString.toString());
					/*	if(_debugLog){
							_logger.debug("[updateCampaignTable]--> Update Query "+finalString.toString());
						}*/
					}					
				}
				
			}else{
				if(_debugLog){
					_logger.debug("[updateCampaignTable] Error while fetching primary database connection..");
				}
			}
			
		}catch(Exception ex){
			if(_errorLog){
				_logger.error("[updateCampaignTable] Some exception occured while updating campaign data",ex);
			}
		}finally{
			if(dbConnection!=null){
				MMConnectionPool.freeConnection(dbConnection);
			}
		}
		updateCampaignData(insertQueryList);
	}
	
	/**
	 * update campaign table with live data
	 * @param insertQueryList
	 */
	private void updateCampaignData(ArrayList<String> insertQueryList) {
		
		if(_debugLog){
			_logger.debug("[updateCampaignData] Updating qstats_campaign_data table");
		}
		Connection derbyConnection	=	null;
		PreparedStatement stmt	=	null;
		try{
			derbyConnection	=	MMConnectionPool.getDerbyConnection();
			if(derbyConnection != null){
				for(String query : insertQueryList){
					stmt	=	derbyConnection.prepareStatement(query);
					stmt.executeUpdate();
					
				}
				
				if(_debugLog){
					_logger.debug("[updateCampaignData]  qstats_campaign_data table derby table updated sucessfully");
				}
			}else{
				if(_debugLog){
					_logger.debug("[updateCampaignData] Error while fetching derbyConnection ");
				}
			}
		}catch(Exception e){
			if(_errorLog){
				_logger.error("[updateCampaignData] Exception while updating campaign table ", e);
			}
			
		}finally{
			
			if(derbyConnection!=null){
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}
			
		}
		
	}
	/**
	 * 
	 * @param campId
	 */
	private void fetchDispositionTypes(int campId) {

		
		PreparedStatement	pstmt	=	null;
		Connection	derbyConnection	=	null;
		ResultSet resultSet	=	null;
		List<String> dispositionList	=	new ArrayList<String>();
		if(_debugLog){
			_logger.debug("[getDispositionTypes] updating campaign disposition data in derby table");			
		}
		
		try{
			
			derbyConnection	=	MMConnectionPool.getDerbyConnection();
			if(derbyConnection != null){
				
				pstmt	=	derbyConnection.prepareStatement("SELECT distinct disposition_type from APP.qstats_disposition_data where campaign_pkey = ?");
				pstmt.setInt(1, campId);
				resultSet	=	pstmt.executeQuery();
				while(resultSet.next()){
					dispositionList.add(resultSet.getString("disposition_type"));
					
				}
				
				if(_debugLog){
					_logger.debug("[getDispositionTypes]Disposition list fetched for campaign id ["+campId+"] is ["+dispositionList+"]");
				}
				
			}else{
				if(_errorLog){
					_logger.error("[getDispositionTypes] Error while fetching derby connection");
				}
			}
			
		}catch(Exception ex){
			if(_errorLog){
				_logger.error("[getDispositionTypes] Some exception occured while updating campaign data",ex);
			}
		}finally{
			if(derbyConnection!=null){
				MMConnectionPool.freeDerbyConnection(derbyConnection);
			}
			
			if(resultSet != null){
				try{
					resultSet.close();
				}catch(Exception e){
					if(_errorLog){
						_logger.error("[getDispositionTypes] Error while closing resultset", e);
					}
				}
				
			}
			
			if(pstmt !=null){
				try{
					pstmt.close();
				}catch(Exception e){
					if(_errorLog){
						_logger.error("[getDispositionTypes] Error while closing the statement",e);
					}
				}
			}
		}
		
		updateDispositionTable(dispositionList,campId);
	}
	private void updateDispositionTable(List<String> dispositionList ,int campId) {
		PreparedStatement pstmt	=	null;
		FileInputStream in= null;
		Connection dbConnection	=	null , derbyConnection=null;
		ResultSet	resultSet	=	null;
		ResultSetMetaData	rsmd	=	null;
		List <String> updateQueryList	=	new ArrayList<String>();
		String query	=	prop.getProperty("query");
		for(String disposition : dispositionList){
			try{
				dbConnection	=	MMConnectionPool.getDBConnection();
				if(dbConnection!=null){
					pstmt	=	dbConnection.prepareStatement(query);
					
					pstmt.setInt(1, campId);
					pstmt.setString(2, disposition);
					StringBuilder columnName	=	new StringBuilder();
					StringBuilder columnValue	=	new StringBuilder();
					
					if(_debugLog){
						_logger.debug("[updateDispositionTable] Executing query ["+query+"]+ disposition type is ["+disposition+"] campaign id is ["+campId+"]");
					}
					resultSet	=	pstmt.executeQuery();
					
					
					while(resultSet.next()){
						rsmd	=	resultSet.getMetaData();
						for(int i=1 ; i<=rsmd.getColumnCount();i++){
							
							if(rsmd.getColumnType(i) == Types.VARCHAR){
								columnValue.append("'").append(resultSet.getString(rsmd.getColumnName(i))).append("'");
								
							}
						
							if(rsmd.getColumnType(i) == Types.INTEGER){
								columnValue.append(resultSet.getString(rsmd.getColumnName(i)));
								
							}
							
							if(rsmd.getColumnType(i) == Types.FLOAT){
								columnValue.append(resultSet.getString(rsmd.getColumnName(i)));
								
							}
							columnName.append(rsmd.getColumnName(i)).append("=").append(columnValue).append(",");
							
						}
					}
			
					String columnNames 		= columnName.substring(0,columnName.length()-1); 
					StringBuilder finalString =new StringBuilder();
					finalString.append("UPDATE APP.qstats_disposition_data SET ")
					.append(columnNames)
					.append(" WHERE campaign_pkey=")
					.append(campId)
					.append(" and disposition_type='")
					.append(disposition)
					.append("'");
				
					updateQueryList.add(finalString.toString());
				}
			}catch(Exception ex){
				if(_errorLog){
					_logger.error("[updateDispositionTable] Some exception occured ", ex);
				}
			}finally{

				if(dbConnection !=null){
					MMConnectionPool.freeConnection(dbConnection);
				}
				
				if(pstmt !=null){
					try{
						pstmt.close();
					}catch(Exception e){
						
					}
				}
				
				if(resultSet!=null){
					try{
						resultSet.close();
					}catch(Exception e){
						
					}
				}
			
				
			}
		}
		
	
		
	
		derbyConnection	=	MMConnectionPool.getDerbyConnection();
		if(derbyConnection!=null){
			
			try{
				for(String query1 : updateQueryList){
					pstmt	=	derbyConnection.prepareStatement(query1);
					pstmt.executeUpdate();
					
				}
				if(_debugLog){
					_logger.debug("[updateDispositionTable] Derby Disposition table updated sucessfully...");
				}
			}catch(Exception e){
				if(_errorLog){
					_logger.error("[updateDispositionTable] Some exception occured while updating disposition table", e);
				}
			}finally{
				if(derbyConnection !=null){
					MMConnectionPool.freeDerbyConnection(derbyConnection);
				}
				
				if(pstmt !=null){
					try{
						pstmt.close();
					}catch(Exception e){
						
					}
				}
			}
			
	
		}else{
			if(_debugLog){
				_logger.debug("[updateDispositionTable] Not able to fetch derby connection...");
			}
			
		}
		
		
		
	}

}
