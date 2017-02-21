package com.interactcrm.qstats.classes;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import com.interactcrm.qstats.startup.TenantGroupObj;

import com.interactcrm.qstats.threads.NVProcessor;
import com.interactcrm.qstats.threads.QueueStatsUpdater;

import com.interactcrm.utils.Utility;


public class NonVoiceQueueStatManager implements IQueueStatsManager{

	

    private int queueGroup;
    private int channelId;
    private int tgId;
    private int fetcher_Sleeptime	=	15;
    private int processor_Sleeptime	=	15;
    
    public NonVoiceQueueStatManager(int channelId,int queueGroup,int tgId) {
        this.queueGroup 	= queueGroup;   
        this.channelId		= channelId;
        this.tgId			= tgId;
        InputStream in	=	null;
        Properties push	=	new Properties();

    	try {
			 in	=	new FileInputStream(Utility.getAppHome() + File.separator + "SleepInterval.properties");
			 push.load(in);
			 fetcher_Sleeptime	=	Integer.parseInt(push.getProperty("Fetcher."+ channelId + ".Sleeptime"));
			 processor_Sleeptime	=	Integer.parseInt(push.getProperty("Processor."+ channelId + ".Sleeptime"));
		} catch (Exception e) {
			
			e.printStackTrace();
		}
        
    
    }
    
	@Override
	public void startProcessing(TenantGroupObj tenantGrpObj) {

		
		Thread thread	=	new Thread(new NVProcessor(queueGroup,processor_Sleeptime,channelId,tgId,tenantGrpObj));
		thread.setName("NVProcessor-"+queueGroup);
		thread.start();
	
	}

	
	@Override
	public void startFetching(TenantGroupObj tenantGrpObj) {

		Thread t = new Thread(new QueueStatsUpdater(queueGroup,fetcher_Sleeptime,channelId,tenantGrpObj,tgId));
		t.setName("QueueStatsUpdater-"+queueGroup);
		t.start();
		
	}
    
	
	

}
