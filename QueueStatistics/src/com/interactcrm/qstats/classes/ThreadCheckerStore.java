package com.interactcrm.qstats.classes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.interactcrm.qstats.db.MMConnectionPool;

public class ThreadCheckerStore {

	Connection connection = null;
	PreparedStatement pstmt = null;
	ResultSet rs = null;

	private static class SingletonHelper {
		private static final ThreadCheckerStore INSTANCE = new ThreadCheckerStore();
	}

	public static ThreadCheckerStore getInstance() {
		return SingletonHelper.INSTANCE;
	}

	public boolean isCampaignThreadEnabled() {
		boolean result = false;
		try {
			connection = MMConnectionPool.getDBConnection();
			if (connection != null) {
				String campaignQuery = "SELECT mj_pkey FROM mj_job WHERE mj_is_active=1 and mj_status='START'";
				pstmt = connection.prepareStatement(campaignQuery);
				rs = pstmt.executeQuery();
				if (rs.next()) {
					result = true;
				} else {
					result = false;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					MMConnectionPool.freeConnection(connection);
				} catch (Exception e2) {
					e2.printStackTrace();
				}

			}
		}
		return result;
	}

	public boolean isCBCThreadEnabled() {

		boolean result = false;
		try {
			connection = MMConnectionPool.getDBConnection();
			if (connection != null) {
				String cbcQuery = "SELECT VM_PKEY FROM CBC_VDN_MAPPING WHERE VM_IS_ACTIVE=1";
				pstmt = connection.prepareStatement(cbcQuery);
				rs = pstmt.executeQuery();
				if (rs.next()) {
					result = true;
				} else {
					result = false;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					MMConnectionPool.freeConnection(connection);
				} catch (Exception e2) {
					e2.printStackTrace();
				}

			}

		}
		return result;

	}

}
