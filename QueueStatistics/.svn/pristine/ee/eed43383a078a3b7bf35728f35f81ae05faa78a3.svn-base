package com.interactcrm.qstats.servlets;


import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueryBean;

import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.util.logging.LogHelper;

/**
 * Servlet implementation class QueueStatistics
 */
public class GetFanucData extends HttpServlet {
	
	
	private static final long serialVersionUID = 1L;
    
	private static Log _logger = new LogHelper(GetFanucData.class).getLogger(LogModuleFactory.getModule("QueueStatistics"));
    private static boolean _debugLog = false;
    private static boolean _errorLog = false;
    private static boolean _infoLog = false;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetFanucData() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		if(_logger != null){
    		_debugLog = _logger.isDebugEnabled();
    		_errorLog =  _logger.isErrorEnabled();
    		_infoLog  = _logger.isInfoEnabled();
    	}
	}
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		 response.setContentType("text/html;charset=utf-8");
			request.setCharacterEncoding("UTF-8");
			response.setHeader("Access-Control-Allow-Methods","GET, POST");
			response.setHeader("Access-Control-Allow-Credentials", "true");
			//response.addHeader( "Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
			response.setHeader("Access-Control-Allow-Origin","*");
			//response.setContentType("application/json;charset=utf-8");
		PrintWriter out = response.getWriter();
		
			String action = request.getParameter("action");
			String data = request.getParameter("data");			
			if (_infoLog) {
				_logger.info("QueueStatistics :: action :: "+ action);
				_logger.info("QueueStatistics :: data :: "+ data);
			}
						
			JSONObject jsonData	= new JSONObject();;
			String queryString	= "";
			try {
				jsonData = new JSONObject(data);
				queryString = jsonData.getString("query");
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (_infoLog) {
				_logger.info("QueueStatistics :: QueryName from Json data :: "+ queryString);			
			}
			
			Connection conn			= null;
			PreparedStatement pstmt = null;
			QueryBean qBean			= QueryFactory.getInstance().getquery(queryString);
			
			if(qBean == null ){
				out.print("{\"ERROR\" : \"The stated Query --["+queryString+"] is not Found.\"}");
				if (_errorLog) {
					_logger.error("QueueStatistics :: Wrong Query Parameter Passed---"+" Parameter query=["+queryString+"]");
				}
				return;
			}
			else{
				if (_infoLog) {
					_logger.info("QueueStatistics For Fanuc :: QueryFired :: "+ qBean);			
				}
			}
			
			try{
				String qString = qBean.getQueryString();
				 conn			= 	MMConnectionPool.getDerbyConnection();
				 pstmt  		=	conn.prepareStatement(qString);
					ResultSet rs	=	null;
					
					
					rs	=	pstmt.executeQuery();
					
					String queueGroupName=null;Integer queueGroup=0;
					JSONArray fullJson=new JSONArray();
					JSONObject finalJson= new JSONObject();
					
					while(rs.next()){
						JSONArray jsonArray=new JSONArray();
						 queueGroupName=rs.getString("queueGroupName");
						 queueGroup=rs.getInt("queueGroup");
						 
						JSONObject json=new JSONObject();
						json.put("callWaiting", rs.getInt("callWaiting"));
						json.put("color", rs.getString("color_voice_contacts"));
						
						JSONObject json1=new JSONObject();
						json1.put("longestWaiting", rs.getString("longestWaiting"));
						json1.put("color", rs.getString("color_longest_duration"));
						
						JSONObject json2=new JSONObject();
						json2.put("emailWaiting", rs.getString("emailWaiting"));
						json2.put("color", rs.getString("color_email_contacts"));
						
						JSONObject json3=new JSONObject();
						json3.put("staffedAgents", rs.getString("staffedAgents"));
						json3.put("color", rs.getString("default_color"));
						
						JSONObject json4=new JSONObject();
						json4.put("answeredCalls", rs.getString("answeredCalls"));
						json4.put("color", rs.getString("default_color"));
						
						JSONObject json5=new JSONObject();
						json5.put("abandonedCalls", rs.getString("abandonedCalls"));
						json5.put("color", rs.getString("default_color"));
						
						JSONObject json6=new JSONObject();
						json6.put("totalCalls", rs.getString("totalCalls"));
						json6.put("color", rs.getString("default_color"));
						
						JSONObject json7=new JSONObject();
						json7.put("totalEmails", rs.getString("totalEmails"));
						json7.put("color", rs.getString("default_color"));
						
						
						jsonArray.put(json);
						jsonArray.put(json1);
						jsonArray.put(json2);
						jsonArray.put(json3);
						jsonArray.put(json4);
						jsonArray.put(json5);
						jsonArray.put(json6);
						jsonArray.put(json7);
					
						JSONObject datajsonObject=new JSONObject();
					
						datajsonObject.put("queueGroupName", queueGroupName);
						datajsonObject.put("queueGroup", queueGroup);
						datajsonObject.put("data",jsonArray);
					
						
						fullJson.put(datajsonObject);
						
						
						finalJson.put("dashboardObject", fullJson);
						//out.println(finalJson);
						//out.println(finalJson);
						
					}
					out.println(finalJson);
					if(_infoLog){
						_logger.info("Response from the servlet is "+ finalJson);
					}
			
			}catch(Exception e){
				if (_errorLog) {
					_logger.error("Exception FanucData ::",e);
				}
			}finally{
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("FanucData Exception while closing the statement ::   "
									+  " ", ex);
						}
					}
					pstmt = null;
				}
				if (conn != null) {
					try {
						MMConnectionPool.freeDerbyConnection(conn);
					} catch (Exception ex) {
						if (_errorLog) {
							_logger.error("FanucData Exception while closing connection object :: "
									+  " ", ex);
						}
					}
			
				}
			
			}
		
		
	}
}
