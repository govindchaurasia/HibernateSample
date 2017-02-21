package com.intearctcrm.test;

import com.icx.lmverifier.LicenseFactoryProducer;
import com.icx.lmverifier.store.ILicenseReader;
import com.interactcrm.logging.exception.LogException;
import com.interactcrm.logging.factory.LogInitializer;
import com.interactcrm.utils.Utility;

public class Test {

	private static ILicenseReader license;

	public static void main(String[] args) {
		boolean a=true;
		if(!a)
		{
			System.out.println(a);
		}
		else
		{System.out.println("false");
		}
			
			
		
		/*String appHome = Utility.getAppHome(); 
		String appLogPath = Utility.getAppLogPath(); 
		try {
			LogInitializer.initialize(appHome, appLogPath);
		} catch (LogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		license = LicenseFactoryProducer.getLicenseFactory().getLicense(74);
		System.out.println("Avaya CB Dashboard-"+license.isFeatureEnabled("Avaya CB Dashboard",false));*/

	}

}
