package com.interactcrm.qstats.classes;


public class QueueStatsManagerFactory {

	public static IQueueStatsManager getInstance(int channelId, int tenantGroupId, int queueGroup) {

		IQueueStatsManager updater = null;
		switch (channelId) {
		case 2:
			boolean enabledCBC = new ThreadCheckerStore().getInstance().isCBCThreadEnabled();
			if (enabledCBC) {
				updater = new VoiceQueueStatManager(channelId, queueGroup, tenantGroupId);
			}
			break;
		case 10: 	// SIP channel
				updater = new VoiceQueueStatManager(channelId, queueGroup, tenantGroupId);
			break;
		case 1:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
			updater = new NonVoiceQueueStatManager(channelId, queueGroup, tenantGroupId);
			break;

		}
		return updater;
	}
}
