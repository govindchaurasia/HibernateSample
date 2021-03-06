package com.interactcrm.qstats.viewderby;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.interactcrm.alarm.Alarm;
import com.interactcrm.alarm.AlarmFactory;
import com.interactcrm.alarm.AlarmGeneratorUtil;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.dao.QueueStatsDAO;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.util.logging.LogHelper;

/**
 * Servlet implementation class ViewTable
 * This servlet is used to view derby tables in browser
 */
public class ViewTable extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    private static Log _logger = new LogHelper(ViewTable.class).getLogger(LogModuleFactory.getModule("QueueStatistics"));
    private static boolean _debugLog = false;
    private static boolean _errorLog = false;
    private static boolean _infoLog = false;
	

    	

    
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ViewTable() {
        super();
        if(_logger != null){
    		_debugLog = _logger.isDebugEnabled();
    		_errorLog =  _logger.isErrorEnabled();
    		_infoLog  = _logger.isInfoEnabled();
    	}
    }
    
    protected void processRequest(HttpServletRequest request,
    	    HttpServletResponse response) throws ServletException, IOException {
    	response.setContentType("text/html;charset=UTF-8");
    	
    	
    	
    	PrintWriter out = response.getWriter();
    	String data = request.getParameter("code");
    	String respData = "";
    	String tableName = "";
    	try {
    	if("raw".equalsIgnoreCase(data)){
    			tableName =	"APP.qstats_raw_data";
    		}else if("display".equalsIgnoreCase(data)){    			
    			tableName = "APP.qstats_display_data";	
    		}
    		else if("channel".equalsIgnoreCase(data)){
    			tableName="APP.qstats_channel_data";
    		}
    		else if("queue_CLI".equalsIgnoreCase(data)){
    			tableName="APP.qstats_raw_queue_CLI";
    		}
    		else if("agent_CLI".equalsIgnoreCase(data)){
    			tableName="APP.qstats_raw_agent_CLI";
    		}
    		else if("fanuc".equalsIgnoreCase(data)){
    			tableName="APP.qstats_fanuc_data";
    		}else if("campaign".equalsIgnoreCase(data)){
    			tableName	=	"APP.qstats_campaign_data";
    		}else if("dispositionData".equalsIgnoreCase(data)){
    			tableName	=	"APP.qstats_disposition_data";
    		}
    		else if("queueGroup".equals(data))
    		{
    			String id=request.getParameter("id");
    			tableName="APP.QSTATS_CONTACT_QUEUEGROUP_"+id;
    		}
    		else if("agents".equals(data))
    		{
    			tableName="APP.qstats_agent_data";
    		}
    		else if("callbackSummary".equals(data))
    		{
    			tableName="APP.qstats_callback_summary";
    		}
    		else if("pendingCallback".equals(data))
    		{
    			tableName="APP.qstats_callback_data";
    		}
    		if("versionMap".equals(data))
    		{
    			Map<Integer, Long> versionMap=QueueGroupStore.getInstance().getVersionMap();
    			 for (Entry<Integer,Long> entry : versionMap.entrySet()) {
    				 out.println("<html>");
    		    	    out.println("<head>");
    		    	    out.println("<title>Agent Data</title>");
    		    	    out.println("</head>");
    		    	    out.println("<body>");
    		    	    out.println("Queuegroup,version<br>");
    		    	    respData=""+entry.getKey()+","+entry.getValue()+"<br>";
    		    	    out.println(respData);
    		    	    out.println("</body>");
    		    	    out.println("</html>");
    			 }
    			
    		}else{
    			
    		
    		
    	    out.println("<html>");
    	    out.println("<head>");
    	    out.println("<title>Agent Data</title>");
    	    out.println("</head>");
    	    out.println("<body>");
    	    respData =  getTableData(tableName);
    	    out.println(respData);
    	    out.println("</body>");
    	    out.println("</html>");
    		}
    	} finally {
    	    out.close();
    	}
        }
    
    
    

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request,response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request,response);
	}
	
	
    /**
     * This method fetches data from requested derby table , formats it to show data in table
     * @returns html table which shows all the existing data from table. 
     * @param tableName : name of table.
	 * @return Table structure containing data.
	 */
	public String getTableData(String tableName) {
		Connection dbConnection = null;
		Statement statement = null;
		ResultSet rs = null;
		
		ResultSetMetaData rsmd = null;
		StringBuffer sb = new StringBuffer();
		try {
			dbConnection = com.interactcrm.qstats.db.MMConnectionPool.getDerbyConnection();
			if (dbConnection != null) {
				String selectSQL = "select * from " + tableName;
				statement = dbConnection.createStatement();
				rs = statement.executeQuery(selectSQL);
				rsmd = rs.getMetaData();
				sb.append("<table border=1><tr>");
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					sb.append("<th>").append(rsmd.getColumnName(i))
							.append("</th>");
				}
				sb.append("</tr>");
				while (rs.next()) {
					sb.append("<tr>");
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						sb.append("<td>").append(rs.getString(i))
								.append("</td>");
					}
					sb.append("</tr>");
				}
				sb.append("</table>");
			} else {
				System.out.println("ERROR");
				if (_errorLog) {
					_logger.error("getTableData :: Error fetching db connection ");
				}
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("getTableData :: Error fetching data", e);
			}
			
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				rs = null;
				if (statement != null) {
					try {
						statement.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error(
									"getTableData :: Error fetching data",
									ex);
						}
					}
					statement = null;
				}
				if (dbConnection != null) {
					try {
						com.interactcrm.qstats.db.MMConnectionPool.freeDerbyConnection(dbConnection);
						// dbConnection.close();

					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error(
									"getTableData :: Error fetching data",
									ex);
						}
					}
					//dbConnection = null;
				}
			} catch (Exception ee) {
				if (_errorLog) {
					_logger.error(
							"getTableData :: Error fetching data", ee);
				}
			}
		}
		return sb.toString();
	}

}
