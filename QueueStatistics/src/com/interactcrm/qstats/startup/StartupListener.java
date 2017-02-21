package com.interactcrm.qstats.startup;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.exception.LogException;
import com.interactcrm.logging.factory.LogInitializer;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.initialize.Initializer;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;


public class StartupListener implements ServletContextListener{	

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		try {
			System.out.println(" =============== contextInitialized =====================");
			String homePath = Utility.getAppHome();
			String logsPath = Utility.getAppLogPath();

			LogInitializer.initialize(homePath, logsPath);
			System.out.println("contextInitialized :: homePath: " + homePath);
			System.out.println("contextInitialized :: logsPath: " + logsPath);
			Log logger = new LogHelper(StartupListener.class)
					.getLogger(LogModuleFactory
							.getModule("QueueStatistics"),"Initialization");
			if(logger.isDebugEnabled()){
			    logger.debug("contextInitialized :: Loading QueueStatistics.");
			}
		} catch (LogException le) {
			System.out.println("contextInitialized :: Log Exception occured. Logs will not be generated");
			le.printStackTrace();
		} catch (Exception e) {
			System.out.println("contextInitialized :: QueueStatistics start up error");
			e.printStackTrace();
		}
		Initializer.getInstance().init();
		
	}
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		
	}

}
