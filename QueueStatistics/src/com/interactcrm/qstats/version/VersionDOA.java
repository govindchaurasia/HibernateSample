package com.interactcrm.qstats.version;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;

import com.interactcrm.qstats.db.MMConnectionPool;

public class VersionDOA {

	public final static String MODULE_RELEASE_REF="109.10";
	
	public static int updateModuleRef(String serverId) {
		Connection dbconn=null;
		// ResultSet dbrs=null;
		CallableStatement dbcs=null;
		int result=0;
		try {
			dbconn=MMConnectionPool.getDBConnection();
			if(dbconn!=null)
			{
				dbconn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				dbcs=dbconn.prepareCall("{call INSERT_UPDATE_INSTALLED_VERSIONS(?,?,?,?)}");
				dbcs.setInt(1,109);
				dbcs.setInt(2, Integer.parseInt(serverId));
				dbcs.setString(3, MODULE_RELEASE_REF);
				dbcs.registerOutParameter(4, Types.INTEGER);
				
				dbcs.executeUpdate();
				
				result=dbcs.getInt(4);
			}
		} catch (Exception e) {
			result=0;
			e.printStackTrace();
			
		}
	
		return result;
	}
}
