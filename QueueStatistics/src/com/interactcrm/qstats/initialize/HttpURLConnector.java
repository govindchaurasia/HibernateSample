/**
 * Class Name : com.interactcrm.qm.dao.QueueGroupDAO
 * Project Name: QueueManager
 * Version : 1.0
 * @author Meena Rajbhar
 */
package com.interactcrm.qstats.initialize;

import com.interactcrm.alarm.Alarm;
import com.interactcrm.alarm.AlarmFactory;
import com.interactcrm.alarm.AlarmGeneratorUtil;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.dao.QueueStatsDAO;
import com.interactcrm.util.logging.LogHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 *
 * Utility class to connect to URL with parameters and get response
 *
 */
public class HttpURLConnector {
	private static Log _logger =
			new LogHelper(HttpURLConnector.class).getLogger(LogModuleFactory.getModule("Agent_Details"));
	private static boolean flag=false;
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;
	/**
	 * Posts data to a web page and returns its response 
	 * @param strUrl URL to call 
	 * @param postParams Parameters to post 
	 * @return The response returned
	 */
	static {
		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
		}
	}
	
	public static String postData(String strUrl, String postParams) {
		String responseString = "";
		URL url = null;
		InputStreamReader responseReader = null;
		InputStream responseStream = null;
		HttpURLConnection urlCon = null;
		List<Integer> tenantGroups	=	Initializer.getInstance().getTenantGrpList();
		
		try {
			
			url = new URL(strUrl);
			urlCon = (HttpURLConnection) url.openConnection();
			String charset = "UTF-8";
			urlCon.setDoOutput(true); // Triggers POST.
			urlCon.setDoInput(true);
			urlCon.setRequestProperty("Accept-Charset", charset);
			
			urlCon.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + charset);
			OutputStreamWriter wr = new OutputStreamWriter(urlCon.getOutputStream());
			wr.write(postParams);
			wr.flush();
			// Get the response
			
			responseStream = urlCon.getInputStream();
			
			responseReader = new InputStreamReader(responseStream);
			BufferedReader in = new BufferedReader(responseReader);
			String responseLine =  "";

			while ((responseLine = in.readLine()) != null) {
				responseString += responseLine;
			}
			responseStream.close();
			responseReader.close();
			in.close();
			wr.close();
			urlCon.disconnect();
			
		} catch (MalformedURLException e) {
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
			
			if(_debugLog){
				_logger.error("postData :: error posting data ",e);
			}
			
		} catch (IOException e) {
	
			if(_debugLog){
				_logger.error("postData :: error posting data ",e);
				
			}
		} catch (Exception e) {
		
			if(_debugLog){
				_logger.error("postData :: some exception while  posting data :: alarm send ",e);
			}
		} finally{
			urlCon = null;
			responseStream = null;
			responseReader = null;
		}
		return responseString;
	}

	/**
	 * To send get request
	 * @param url : Server URL
	 * @param getParams : Parameters to be send
	 */
	public void getData(String url, String getParams) {}
}
