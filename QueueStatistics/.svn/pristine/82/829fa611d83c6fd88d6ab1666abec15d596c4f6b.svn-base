/**
 * Class Name : com.interactcrm.im.util.ActivePropertiesReader
 * Project Name: InteractionManager
 * Version : 1.0
 */
package com.interactcrm.qstats.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;

/**
 * ActivePropertiesReader contains methods to read values from property file.
 */
public class ActivePropertiesReader {
	private static ActivePropertiesReader ActivePropertiesReader = new ActivePropertiesReader();
	private static Properties push	=	null;
	private static Log _logger = new LogHelper(ActivePropertiesReader.class)
	.getLogger(LogModuleFactory.getModule("QueueStatistics"));
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;
	
	/**
	 * Constructs Instance of ActivePropertiesReader
	 */
	private ActivePropertiesReader(){
		System.out.println("ActivePropertiesReader.ActivePropertiesReader(), In Constructor..");
		_debugLog = (_logger == null ? false : _logger.isDebugEnabled());
		_errorLog = (_logger == null ? false : _logger.isErrorEnabled());
		
	}
	
	/* Block that loads file.*/
	static{
		if (_debugLog) {
			_logger.debug("static block:: Loading file");
		}
		loadFile();
	}

	/**
	 * @return Instance of ActivePropertiesReader
	 */
	public static ActivePropertiesReader getInstance(){
		return ActivePropertiesReader;
	}
	
	/**
	 * Loads the file.
	 */
	public static void loadFile(){	 
		push = new Properties();
		InputStream in = null;
		try{
			if (_debugLog) {
				_logger.debug("loadFile:: Utility.getAppHome(): " + Utility.getAppHome() + "\n loading file active.properties.");
			}
			in = new FileInputStream(Utility.getAppHome() + File.separator + "active.properties");			
			push.load(in);
			if (_debugLog) {
				_logger.debug("loadFile:: load Successfully...");
			}
		}catch (FileNotFoundException e){
			if (_errorLog) {
				_logger.error("loadFile::", e);
			}
		}catch (IOException e){
			if (_errorLog) {
				_logger.error("loadFile::", e);
			}
		}finally{
			try {
			  if(in != null){	
				in.close();
			  }
			} catch (IOException e) {
				if (_errorLog) {
					_logger.error("loadFile::", e);
				}
			}
		}
	}
	
	/**
	 * Fetches specified property value
	 * @param Prop : Property name
	 * @return String containing property value
	 */
	public String getProperty(String Prop) {
		try {
			if (_debugLog) {
				_logger.debug("getProperty:: read , Prop: " + Prop + ", push ="
						+ push + "push.toString(): " + push.toString());
			}
			if (push.containsKey(Prop)) {
				return (push.getProperty(Prop.trim()));
			} else {
				if (_errorLog) {
					_logger.error("getProperty::property missing : "
								+ Prop);
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("getProperty::", e);
			}
		}
		return null;
	}

	/**
	 * Fetches specified property value
	 * @param Prop : Property name
	 * @param defaultValue : default value of property
	 * @return String containing property value
	 */
	public String getProperty(String Prop, String defaultValue){
		try{
			if (_debugLog) {
				_logger.debug("getProperty:: read , Prop: " + Prop + ", push ="
						+ push + "push.toString(): " + push.toString());
			}
			return (push.getProperty(Prop.trim(), defaultValue));
		}catch (Exception e) {
			if (_errorLog) {
				_logger.error("getProperty::", e);
			}
		}
		return null;
	}
}
