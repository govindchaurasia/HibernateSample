/**
 * 
 */
package com.interactcrm.qstats.classes;

import java.io.File;
import java.io.FileInputStream;

import java.io.InputStream;

import java.util.Properties;

import org.apache.derby.tools.sysinfo;

import com.icx.lmverifier.LicenseFactoryProducer;
import com.icx.lmverifier.store.ILicenseReader;
import com.interactcrm.qstats.constant.QSConstants;
import com.interactcrm.qstats.startup.TenantGroupObj;

import com.interactcrm.qstats.threads.CBCProcessor;
import com.interactcrm.qstats.threads.VoiceFetcher;
import com.interactcrm.qstats.threads.VoiceProcessor;
import com.interactcrm.utils.Utility;

/**
 * @author Ramkumar R
 * 
 */
public class VoiceQueueStatManager implements IQueueStatsManager {

	private int queueGroup;
	private int channelId;
	private int tgId;
	private int fetcher_Sleeptime = 20;
	private int processor_Sleeptime = 20;

	public VoiceQueueStatManager(int channelId, int queueGroup, int tgId) {
		this.queueGroup = queueGroup;
		this.channelId = channelId;
		this.tgId = tgId;
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
		  
		Thread processor=new Thread(new VoiceProcessor(channelId, tgId, queueGroup,
				processor_Sleeptime,tenantGrpObj));
		processor.setName("VoiceProcessor-"+tgId);
		processor.start();
		
		try {
			ILicenseReader license = LicenseFactoryProducer.getLicenseFactory().getLicense(tgId);
			if (license.isFeatureEnabled(QSConstants.AVAYACBCFEATURE,false)) {
				Thread cbcThread = new Thread(new CBCProcessor(channelId, tgId,
					queueGroup, fetcher_Sleeptime, tenantGrpObj));
			cbcThread.setName("Callback-" + queueGroup);
			cbcThread.start();
			}
		} catch (Exception e) {
			System.out.println("No license found for tgId "+tgId);
		}
				
	}
		
	
	@Override
	public void startFetching(TenantGroupObj tenantGrpObj) {
		Thread t  = new Thread(new VoiceFetcher(channelId, tgId, queueGroup,fetcher_Sleeptime,tenantGrpObj));
		t.setName("VoiceFetcher-"+tgId);
		t.start();
	
	}

}
