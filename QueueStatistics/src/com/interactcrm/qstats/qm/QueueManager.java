package com.interactcrm.qstats.qm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.interactcrm.alarm.Alarm;
import com.interactcrm.alarm.AlarmFactory;
import com.interactcrm.alarm.AlarmGeneratorUtil;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.mm.beans.Event;
import com.interactcrm.mm.constants.Constants;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.qstats.util.ADUtil;
import com.interactcrm.util.logging.LogHelper;

/**
 * QueueManager contains all queue manager specific details and functionalities.
 * Class Name : com.interactcrm.im.qm.QueueManager
 * @author Vipin Singh
 * @version 1.0
 * @since 1.0
 */
public class QueueManager {

	private static Log _logger = new LogHelper(QueueManager.class)
			.getLogger(LogModuleFactory.getModule("Agent_Details"));
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;
	private String URL = "";
	private int pkey = 0;
	private static boolean flag	=	false;
	static {
		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
		}
	}

	public QueueManager(int pkey , String url) {
		this.pkey = pkey;
		this.URL = url;	
	}

	/**
	 * Not in used
	 * Sends specified data to QueueManager.
	 * @param sessionId
	 *            : Session ID of agent
	 * @param servletName
	 *            : QM servlet to be called.
	 * @param data
	 *            : Message to be sent.
	 * @return response from QueueManager.
	 */
	/*public String redirectRequestToQM(String sessionId, String servletName,
			StringBuilder data) {
		String response = "";
		if (_debugLog) {
			_logger.debug("redirectRequestToQM:: SessionId = " + sessionId
					+ ", ServletName = " + servletName + ", data = "
					+ data.toString());
		}
		response = forwardToQM(servletName, data);
		if (response != null && !("".equalsIgnoreCase(response))) {
			return response;
		} else {
			response = ADUtil.getJSON(new Event(Constants.QUEUE_MANAGER_CONNECTION_FAILED,
					"Unable to connect with QM, URL = " + this.URL, ""));
			return response;
		}
	}*/

	@Override
	public String toString() {
		return "QueueManager [PKEY="+ this.pkey +"], [URL=" + URL + "]";
	}
	
	public String getURL(){
		return URL;
		
	}
	/**
	 * Sends specified data to QueueManager. 
	 * This method will return queue id along with QM connection failed event(If QM was down).
	 * This method is used when Deferred contacts or Agent's live contact is received.
	 * @param sessionId
	 *            : Session ID of agent
	 * @param servletName
	 *            : QM servlet to be called.
	 * @param data
	 *            : Message to be sent.
	 * @param json
	 *            :JSON object containing data from Agent Desktop
	 * @return response from QueueManager.
	 */
	/*public String redirectRequestToQM(String sessionId, String servletName,
			StringBuilder data, JSONObject json) {
		String response = "";
		if (_debugLog) {
			_logger.debug("redirectRequestToQM:: SessionId = " + sessionId
					+ ", ServletName = " + servletName + ", data = "
					+ data.toString());
		}
		response = forwardToQM(servletName, data);
		if (response != null && !("".equalsIgnoreCase(response))) {
			return response;
		} else {
			int queueId = 0;
			try {
				if (json != null) {
					queueId = json.getInt(Constants.QUEUE_ID);
				}
			} catch (JSONException e) {
				if (_errorLog) {
					_logger.error("forwardRequestToQM::", e);
				}
			}
			response = ADUtil.getJSON(new Event(Constants.QUEUE_MANAGER_CONNECTION_FAILED,
					"Unable to connect with QM, URL = " + this.URL, queueId, null));
			return response;
		}
	}
*/

	
	/**
	 * Sends request to QM and fetches Agent monitor details
	 * @param servletName : QM servlet name
	 * @param data : Extra parameter to send QM servlet
	 * @return JSON response from QM
	 */
	public synchronized String getAgentMonitorDetails(String servletName, StringBuilder data,String tenangGrpPKey){
		Log logger = new LogHelper(QueueManager.class).getLogger(LogModuleFactory.getModule("Agent_Details"));
		if (logger.isInfoEnabled()) {
			logger.info("getAgentMonitorDetails:: Servlet name = " + servletName + ", parameters = " + data);
		}
		String response =  forwardToQM(servletName, data);
		return response;
	}

	/**
	 * Sends events to Queue Manager.
	 * 
	 * @param notificationData
	 *            : Events to be sent to QM.
	 * @param servletName
	 *            : QM servlet to be called.
	 * @return Response received from QM.
	 */
	private String forwardToQM(String servletName,
			StringBuilder notificationData) {
		String responseString = "";
		URL url = null;
		InputStreamReader responseReader = null;
		InputStream responseStream = null;
		BufferedReader in = null;
		OutputStreamWriter wr = null;
		HttpURLConnection urlCon = null;
		
		List<Integer> tenantGroups	=	Initializer.getInstance().getTenantGrpList();
		
		try {
			String qmUrl = this.URL + "/" + servletName;
			if (_debugLog) {
				_logger.debug("forwardToQM::URL=[" + qmUrl
						+ "], Notification Data = " + notificationData);
			}
			url = new URL(qmUrl);
			urlCon = (HttpURLConnection) url.openConnection();
			String charset = "UTF-8";
			urlCon.setDoOutput(true); // Triggers POST.
			urlCon.setDoInput(true);
			urlCon.setRequestProperty("Accept-Charset", charset);
			urlCon.setRequestProperty("Content-Type",
					"application/x-www-form-urlencoded;charset=" + charset);
			wr = new OutputStreamWriter(urlCon.getOutputStream());
			wr.write(notificationData.toString());
			wr.flush();
			// Get the response
			responseStream = urlCon.getInputStream();
			responseReader = new InputStreamReader(responseStream);
			in = new BufferedReader(responseReader);
			String responseLine = "";
			while ((responseLine = in.readLine()) != null) {
				responseString += responseLine;
			}
			if (_debugLog) {
				_logger.debug("forwardToQM::Response received from QM is:=["
						+ responseString + "]");
			}
		} catch (MalformedURLException e) {
			if (_errorLog) {
				_logger.error("forwardToQM :: error posting data", e);
			}
			
		if(flag==false){
			Alarm alarm = AlarmFactory.createAlarm();
			alarm.setName("Alarm from Queuestats");
			alarm.setPriority(10);
			alarm.setStatus(Alarm.IMMEDIATE);
				alarm.setDescription("QM link breakdown "+e.toString());
				for (Integer tgId : tenantGroups) {
					alarm.setTenantGroupId(tgId);									
					AlarmGeneratorUtil.getInstance().raiseAlarm(alarm);
				}
				if(_debugLog)
					_logger.debug("Alram sent ::"+alarm.getDescription());
			}
			flag	=	true;
			
			
			
		} catch (IOException e) {
			if (_errorLog) {
				_logger.error("forwardToQM :: error posting data", e);
			}
			
			
			
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("forwardToQM :: error posting data", e);
			}
		} finally {
			try {
				if (responseStream != null) {
					responseStream.close();
				}
				if (responseReader != null) {
					responseReader.close();
				}
				if (in != null) {
					in.close(); 
				}
				if (wr != null) {
					wr.close();
				}
				if (urlCon != null) {
					urlCon.disconnect();
				}
			} catch (Exception ee) {
				if (_errorLog) {
					_logger.error("forwardToQM :: error posting data", ee);
				}
			}
		}
		return responseString;
	}
}
