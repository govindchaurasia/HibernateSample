package com.interactcrm.qstats.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.JSONException;
import org.json.JSONObject;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueryBean;
import com.interactcrm.qstats.bean.QueueGroupStore;
import com.interactcrm.qstats.bean.QueueQueueGroupMapping;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.qstats.startup.TenantGroupStore;
import com.interactcrm.qstats.util.QueryUtil;
import com.interactcrm.util.logging.LogHelper;


/**
 * Servlet implementation class QueueStatistics
 */
public class GetData extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static Log _logger = new LogHelper(GetData.class).getLogger(LogModuleFactory.getModule("QueueStatistics"));
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;
	private static boolean _infoLog = false;
	private int queueGroupId = 0;
	long startTime =0L;
	long stopTime=0L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public GetData() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
			_infoLog = _logger.isInfoEnabled();
		}
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		request.setCharacterEncoding("UTF-8");
		response.setContentType("text/html;charset=UTF-8");
		PrintWriter out = response.getWriter();
		String action = request.getParameter("action");
		String data = request.getParameter("data");
		if (_infoLog) {
			_logger.info("QueueStatistics :: action :: " + action);
			_logger.info("QueueStatistics :: data :: " + data);
		}

		JSONObject jsonData = new JSONObject();
		String queryString = "";
		try {
			jsonData = new JSONObject(data);
			queryString = jsonData.getString("query");
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (_infoLog) {
			_logger.info("QueueStatistics :: QueryName from Json data :: " + queryString);
		}
		List<Object> list = new ArrayList<Object>();
		Connection conn = null;
		PreparedStatement pstmt = null;
		QueryBean qBean = QueryFactory.getInstance().getquery(queryString);

		if (qBean == null) {
			out.print("{\"ERROR\" : \"The stated Query --[" + queryString + "] is not Found.\"}");
			if (_errorLog) {
				_logger.error("QueueStatistics :: Wrong Query Parameter Passed---" + " Parameter query=[" + queryString
						+ "]");
			}

			return;
		}

		try {
			if (_infoLog) {
				_logger.info("QueueStatistics :: Query Bean :: " + qBean + "qBean.getParameterList()-- "
						+ qBean.getParameterList() + "---getParameterList().size()---"
						+ qBean.getParameterList().size());

			}
			String qString = qBean.getQueryString();
			try {
				 startTime = System.currentTimeMillis();
				for (int i = 0; i < qBean.getParameterList().size(); i++) {
					qString = qString.replace("$P{" + qBean.getParameterList().get(i) + "}",
							jsonData.getString(qBean.getParameterList().get(i)));
					
				
					String val = qBean.getParameterList().get(i);
					QueryUtil queryUtil = QueryUtil.getQueryUtil(queryString);
					qString = queryUtil.getVersion(qString, jsonData, val);
					
					/*qString = qString.replaceAll("##version##", "" + version);
					int parseInt = Integer.parseInt(jsonData.getString(qBean.getParameterList().get(i)));
					if (qString.contains("queueGroupId")) {
						queueGroupId = QueueQueueGroupMapping.getInstance()
								.getQueueGroupId(Integer.parseInt(jsonData.getString(qBean.getParameterList().get(i))));

						qString = qString.replaceAll("##queueGroupId##", "" + queueGroupId);
						if (qString.contains("version")) {
							long version = QueueGroupStore.getInstance().getVersionFromMap(queueGroupId);
							qString = qString.replaceAll("##version##", "" + version);
						}
					}
					if (qString.contains("callback")) {
						if (qString.contains("version")) {
							long version = TenantGroupStore.getInstance().getVersionFromMap(Integer.parseInt(jsonData.getString(qBean.getParameterList().get(i))));
							qString = qString.replaceAll("##version##", "" + version);
						}
					}*/
					
					if(_debugLog)
					{
						_logger.debug("Final QueryString----->"+qString);
					}
				}

			} catch (Exception e) {
				if (_errorLog) {
					_logger.error("Exception in iteration for", e);
				}
			}
			try {
				conn = MMConnectionPool.getDerbyConnection();
				pstmt = conn.prepareStatement(qString);
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error("QueueStatistics :: rs :: " + e);
				}

			}
			ResultSet rs = null;

			rs = pstmt.executeQuery();
			if (_infoLog) {
				_logger.info("QueueStatistics :: rs :: " + rs);
			}
			ResultSetMetaData rsmd = rs.getMetaData();
			int columncount = rsmd.getColumnCount();
			List<String> columnList = new ArrayList<String>();

			for (int i = 0; i < columncount; i++) {
				columnList.add(rsmd.getColumnLabel(i + 1));
			}

			while (rs.next()) {
				HashMap<String, String> jsonObject = new HashMap<String, String>();

				for (String column : columnList) {
					jsonObject.put(column, rs.getString(column));
				}
				
				list.add(convertMapToJSON(jsonObject));
				
			}
			out.print(list);
			long stopTime = System.currentTimeMillis();
			if(_debugLog)
			{
				_logger.debug("JSON Response sent..."+list);				
			}
			System.out.println(list);
			if(_debugLog)
			{
				_logger.debug("JSON Response sent..."+list);
				_logger.debug("Code Elapsed Time "+(stopTime-startTime));
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("Exception QueueStatistics ::", e);
			}
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("QueueStatistics ::   " + " ", ex);
					}
				}
				pstmt = null;
			}
			if (conn != null) {
				try {
					MMConnectionPool.freeDerbyConnection(conn);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("QueueStatistics :: " + " ", ex);
					}
				}
				// conn = null;
			}

		}
	}

	/*
	private String convertMapToJSON(HashMap<String, String> map) {
		boolean firstFlag = true;

		StringBuilder sbd = new StringBuilder();
		sbd.append("{");

		for (String key : map.keySet()) {
			if (firstFlag) {
				firstFlag = false;
				sbd.append("\"").append(key).append("\":\"").append(map.get(key)).append("\"");
			} else {
				sbd.append(",\"").append(key).append("\":\"").append(map.get(key)).append("\"");
			}
		}

		sbd.append("}");

		return sbd.toString();
	}

*/
	
	private String convertMapToJSON(HashMap<String, String> map) {
		
		JSONObject jSONObject;
		jSONObject = new JSONObject();
		for (String key : map.keySet()) {
				
				try {
					jSONObject.put(key, map.get(key));
				} catch (JSONException e) {					
					e.printStackTrace();
				}		
		}

			return jSONObject.toString();
	}
	
	
			
	
}
