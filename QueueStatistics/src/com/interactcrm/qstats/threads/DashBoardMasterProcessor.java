package com.interactcrm.qstats.threads;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.interactcrm.dbcp.ConnectionPoolManager;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueueQueueGroupMapping;
import com.interactcrm.qstats.bean.TherSholdBean;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.util.logging.LogHelper;

public class DashBoardMasterProcessor implements Runnable {

	private int dashboardId;
	TherSholdBean therSholdBean;
	private List<Integer> queueList;
	private boolean _debugLog = false;
	private boolean _errorLog = false;

	private Log _logger = null;
	private Set<Integer> queueGroups = new HashSet<Integer>();

	public DashBoardMasterProcessor(int dashboardId, List<Integer> queueList,
			TherSholdBean therSholdBean) {

		this.dashboardId = dashboardId;
		this.queueList = queueList;
		this.therSholdBean = therSholdBean;

		_logger = new LogHelper(DashBoardMasterProcessor.class).getLogger(
				LogModuleFactory.getModule("DashBoardMasterProcessor"),
				String.valueOf(dashboardId));

		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
		
		}
		if (_debugLog) {
			_logger.debug("DashBoardMasterProcessor:: Parameters Passed ----> dashboardId["
					+ dashboardId
					+ "] processor sleep time[5 Secs] queueList["
					+ queueList + "]");
		}

	}

	@Override
	public void run() {
		while (true) {
			process();
			//TODO sleeptime configurable
			try {
				if (_debugLog) {
					_logger.debug("processor_sleeptime" + 5 * 1000);
				}
				Thread.sleep(5 * 1000);
			} catch (InterruptedException ie) {
				if (_errorLog) {
					_logger.error("initializeDashBoardManager :: " + " ", ie);
				}
			}
		}

	}

	private void process() {
		Connection dbConnection = null;

		try {
			dbConnection = MMConnectionPool.getDerbyConnection();
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error(
						"run :: Error while fetching derby dayabase connection ",
						e);
			}
		}
		//map is maitained to store total emails handled since morning against each queue
		Map<Integer, Integer> queueCountMap = new HashMap<Integer, Integer>();
		for (int queueId : queueList) {
			queueCountMap.put(queueId, 0);
		}
		if (_debugLog) {
			_logger.debug("process :: intializing  queue-count of emails handled map "
					+ queueCountMap);
		}

		Collection<Integer> values = QueueQueueGroupMapping.getInstance()
				.getQueueQueueGroupMap().values();
		for (Integer queueGroupId : values) {
			queueGroups.add(queueGroupId);
		}
		// UPdating fanuc raw table
		getEmailsHandledCount(queueCountMap);
		updateRawTable(queueCountMap, dbConnection);

		// UPdating fanuc display table
		updateFanucDisplayTable(dashboardId, queueList, therSholdBean,
				dbConnection);
		updateFanucEmailDisplayTable(dashboardId, queueList, therSholdBean,
				dbConnection);
		if (dbConnection != null) {
			try {
				MMConnectionPool.freeDerbyConnection(dbConnection);
			} catch (Exception ex) {
				if (_errorLog) {
					_logger.error(
							"run :: Error while releasing derby dayabase connection ",
							ex);
				}
			}
		}
	}

	private void updateFanucEmailDisplayTable(int dashboardId,
			List<Integer> queueList, TherSholdBean therSholdBean,
			Connection dbConnection) {

		Connection derbyConnection = null;
		PreparedStatement selectStatement = null;
		ResultSet selectResultSet = null;
		PreparedStatement updateStatement = null;

		int agg_D040 = 0, agg_D170 = 0, T050 = 0, max_D040 = 0, max_T050 = 0;
		;
		String T051 = null, T052 = null;
		String displayColor = null;

		try {

			if (_debugLog) {
				_logger.debug("updateFanucEmailDisplayTable for email channel :: Parameters Passed ----, dashboardId["
						+ dashboardId + "]");
			}
			derbyConnection = dbConnection;
			try {
				if (dbConnection != null) {
					try {
						String selectFromRaw = "SELECT sum(D040) as agg_D040,sum(D170) as agg_D170,max(D040) as max_D040,max(T050) as max_T050 FROM APP.QSTATS_RAW_DATA WHERE pkey IN ($P{queueList}) and channel=1 ";
						// String selectFromRaw =
						// "SELECT D040 as agg_D040,D170 as agg_D170 FROM APP.QSTATS_RAW_DATA WHERE pkey IN ($P{queueList}) ";
						if (_debugLog) {
							_logger.debug("updateFanucEmailDisplayTable:: in updateFanucDisplayTable dashboardId id passed : "
									+ dashboardId);
						}
						String query = parseQueryy(selectFromRaw);
						selectStatement = dbConnection.prepareStatement(query);

						selectResultSet = selectStatement.executeQuery();

						if (selectResultSet != null) {
							while (selectResultSet.next()) {

								agg_D040 = selectResultSet.getInt("agg_D040");
								agg_D170 = selectResultSet.getInt("agg_D170");
								max_D040 = selectResultSet.getInt("max_D040");
								// max_T040 =
								// selectResultSet.getInt("max_T040");
								max_T050 = selectResultSet.getInt("max_T050");

							}
							if (_debugLog) {
								_logger.debug("updateFanucEmailDisplayTable:: Aggregated data for email--> total contacts waiting "
										+ agg_D040
										+ "  total Emails handled "
										+ agg_D170
										+ " max(contacts waiting found ) "
										+ max_D040
										+ " max(thershold for contacts waiting)"
										+ max_T050);
							}
						} else {
							if (_errorLog) {
								_logger.error("updateFanucEmailDisplayTable :: ResultSet is Empty.");
							}
						}
					} catch (Exception e) {
						if (_errorLog) {
							_logger.error(
									"updateFanucEmailDisplayTable ::  Error fetching aggregate data ",
									e);
						}
					}
					try {
						if (_debugLog) {
							_logger.debug("updateFanucEmailDisplayTable:: Thershold data passed "
									+ therSholdBean
									+ " for queueList "
									+ queueList
									+ " and for dashgroupId "
									+ dashboardId);
						}

						// for (TherSholdBean therShold : therSholdBean){

						// T050 = therShold.getTherSholdForContactsWaiting();
						T051 = therSholdBean
								.getBelowTherSholdColorForContacts();
						T052 = therSholdBean
								.getAboveThersholdColorForContacts();
						displayColor = T051;
						if (max_D040 > max_T050) {
							displayColor = T052;
						}
						// }

						if (_debugLog) {
							_logger.debug("updateFanucEmailDisplayTable:: in updateFanucDisplayTable max[contacts waiting] "
									+ max_D040
									+ " max [Thershold] set for contact waiting "
									+ max_T050
									+ " Hence [Hexcode] calculated for contacts waiting----> "
									+ displayColor);
						}
					} catch (Exception e) {
						if (_errorLog) {
							_logger.error(
									"updateFanucEmailDisplayTable ::  Error Processing For Thershold Coloring for DashboardEmailProcessor",
									e);
						}
					}

					String insertAggregatedata = "UPDATE APP.qstats_fanuc_data SET A030=?,A080=?,A110=?,A120=? WHERE dashboard_group_id=? ";
					updateStatement = dbConnection
							.prepareStatement(insertAggregatedata);
					if (_debugLog) {
						_logger.debug("updateFanucEmailDisplayTable :: "
								+ insertAggregatedata
								+ ", With Aggregated Data");
					}
					updateStatement.setInt(1, agg_D040);
					updateStatement.setInt(2, agg_D170);
					updateStatement.setString(3, displayColor);
					updateStatement.setString(4, T051);
					updateStatement.setInt(5, dashboardId);
					updateStatement.executeUpdate();
					if (_debugLog) {
						_logger.debug("updateFanucEmailDisplayTable :: fanuc display table updated "
								+ "successfully with aggregated email data---> [total emails handled] "
								+ agg_D170
								+ "[total emails waiting] "
								+ agg_D040
								+ "[color for emails waiting] "
								+ displayColor
								+ " for [dashgroup id] "
								+ dashboardId);
					}

				} else {
					if (_errorLog) {
						_logger.error("updateFanucEmailDisplayTable :: Error--> derby db connection null ");
					}
				}
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error(
							"updateFanucEmailDisplayTable :: Error while Executing Query--- ",
							e);
				}
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("updateFanucEmailDisplayTable ::  Error  "
						+ " table.", e);
			}
		} finally {

		}
	}

	private void updateFanucDisplayTable(int dashboardId,
			List<Integer> queueList, TherSholdBean therSholdBean,
			Connection dbConnection) {
		Connection derbyConnection = null;
		PreparedStatement selectStatement = null;
		ResultSet selectResultSet = null;
		PreparedStatement updateStatement = null;
		int D040 = 0, D020 = 0, D030 = 0, D140 = 0, T050 = 0, T040 = 0, max_T040 = 0, max_T050 = 0;
		String D051 = null, T041 = null, T042 = null, T051 = null, T052 = null, displayColorForCalls = null, displayColorForDuration = null, longest_duration = null;
		try {

			if (_debugLog) {
				_logger.debug("updateFanucDisplayTable for voice data :: Parameters Passed ----> dashboardId["
						+ dashboardId + "]");
			}
			derbyConnection = dbConnection;
	
			try {
				if (derbyConnection != null) {
					try {

						String selectFromRaw = "SELECT D020 as D020 ,D040 as D040,D030 as D030,D140 as D140,D051 as D051,T040 as T040,T050 as T050 FROM APP.QSTATS_RAW_DATA WHERE pkey IN ($P{queueList}) and channel=2 ";
						if (_debugLog) {
							_logger.debug("VoiceProcessor:: in updateFanucDisplayTable dashboard id passed : "
									+ dashboardId);
						}
						String query = parseQueryy(selectFromRaw);
						selectStatement = dbConnection.prepareStatement(query);
						selectResultSet = selectStatement.executeQuery();
						if (selectResultSet != null) {
							while (selectResultSet.next()) {
								D020 = selectResultSet.getInt("D020");
								D030 = selectResultSet.getInt("D030");
								D040 = selectResultSet.getInt("D040");
								D051 = selectResultSet.getString("D051");
								D140 = selectResultSet.getInt("D140");
								T040 = selectResultSet.getInt("T040");
								T050 = selectResultSet.getInt("T050");
								if (T040 > max_T040) {
									max_T040 = T040;
								}
								if (T050 > max_T050) {
									max_T050 = T050;
								}
								// max_D040 =
								// selectResultSet.getInt("max_D040");

							}
							if (_debugLog) {
								_logger.debug("updateFanucDisplayTable:: selectfromRaw - "
										+ selectFromRaw
										+ ", Data found for DashboardVoice: "
										+ "D020["
										+ D020
										+ "] "
										+ "D030["
										+ D030
										+ "] "
										+ "D140["
										+ D140
										+ "] "
										+ "D051["
										+ D051
										+ "]"
										+ "D140["
										+ D140
										+ "]");

							}
						} else {
							if (_errorLog) {
								_logger.error("updateFanucDisplayTable :: ResultSet is Empty--> Data for voice can not be fetched ");
							}
						}
					} catch (Exception e) {
						if (_errorLog) {
							_logger.error(
									"updateFanucDisplayTable ::  Error while fetching the data for voice ",
									e);
						}
					}
					if (_debugLog) {
						_logger.debug("updateFanucDisplayTable:: Thershold data passed "
								+ therSholdBean
								+ " for queueList "
								+ queueList
								+ " and for dashgroupId " + dashboardId);
					}

					T041 = therSholdBean.getBelowTherSholdColorForDuration();
					T042 = therSholdBean.getAboveThersholdColorForDuration();
					T051 = therSholdBean.getBelowTherSholdColorForContacts();
					T052 = therSholdBean.getAboveThersholdColorForContacts();
					displayColorForCalls = T051;
					if (D040 > max_T050) {
						displayColorForCalls = T052;
					}

					try {

						if (_debugLog) {
							_logger.debug("updateFanucDisplayTable:: displayColorForCalls--->"
									+ displayColorForCalls
									+ " for dashgroupId " + dashboardId);
						}
						int hour = 0;
						int minutes = 0;
						int seconds = 0;
						String temp = null;
						int time = 0;

						StringTokenizer timeTokens = new StringTokenizer(D051,
								":");
						int numberOfTokens = timeTokens.countTokens();

						int count = 0;
						while (timeTokens.hasMoreElements()) {
							count++;
							temp = timeTokens.nextToken();
							if (numberOfTokens == 3) {
								if (count == 1) {
									hour = Integer.parseInt(temp);

								} else if (count == 2) {
									minutes = Integer.parseInt(temp);
								} else if (count == 3) {
									seconds = Integer.parseInt(temp);
								}
							} else if (numberOfTokens == 2) {
								if (count == 1) {
									minutes = Integer.parseInt(temp);
								} else if (count == 2) {
									seconds = Integer.parseInt(temp);
								}
							} else if (numberOfTokens == 1) {
								if (count == 1) {
									seconds = Integer.parseInt(temp);
								}
							}
						}

						if (hour != 0) {

							time = time + hour * 60 * 60;
						}
						if (minutes != 0) {

							time = time + minutes * 60;
						}

						if (seconds != 0) {

							time = time + seconds;
						}
						//for voice longest duration is fetched from CM
						longest_duration = D051.substring(2);

						// String strDate = sdf.format(time);
						// System.out.println(strDate);
						if (time > T040) {
							displayColorForDuration = T042;
						} else {
							displayColorForDuration = T041;
						}
						if (_debugLog) {
							_logger.debug("updateFanucDisplayTable :: displayColorForDuration -->"
									+ displayColorForDuration
									+ " for dashboardId " + dashboardId);
						}
					} catch (Exception e) {
						if (_errorLog) {
							_logger.error(
									"updateFanucDisplayTable ::  Error while processing for thershold colours ",
									e);
						}
					}
					try {
						// String insertAggregatedata =
						// "UPDATE APP.qstats_fanuc_data SET A020=?,A090=?,A100=? WHERE dashboard_group_id=? ";
						String insertAggregatedata = "UPDATE APP.qstats_fanuc_data SET A010 = ?,A040=?,A050=?,A060=?,A070=?,A020=?,A090=?,A100=?,A120=? WHERE dashboard_group_id=? ";
						updateStatement = derbyConnection
								.prepareStatement(insertAggregatedata);
						if (_debugLog) {
							_logger.debug("updateFanucTable for voice :: "
									+ insertAggregatedata
									+ ", With Aggregated Data");
						}
						updateStatement.setInt(1, D040);

						updateStatement.setInt(2, D140);
						updateStatement.setInt(3, D020);
						updateStatement.setInt(4, D030);
						updateStatement.setInt(5, D020 + D030);
						updateStatement.setString(6, longest_duration);

						updateStatement.setString(7, displayColorForCalls);
						updateStatement.setString(8, displayColorForDuration);
						updateStatement.setString(9, T051);
						updateStatement.setInt(10, dashboardId);
						updateStatement.executeUpdate();
						if (_debugLog) {
							_logger.debug("updateFanucDisplayTable :: fanuc display table updated "
									+ "successfully with aggregated voice data---> [total contacts waiting] "
									+ D040
									+ "[staffed agents for voice ] "
									+ D140
									+ "[total calls answered ] "
									+ D020
									+ "[total calls abandoned ]"
									+ D030
									+ "color for calls waiting "
									+ displayColorForCalls
									+ "color for calls waiting "
									+ displayColorForDuration
									+ " for [dashgroup id] " + dashboardId);
						}
					} catch (Exception e) {
						if (_errorLog) {
							_logger.error(
									"updateFanucDisplayTable ::  Error while updating qstats_fanuc_data ",
									e);
						}
					}

				} else {
					if (_errorLog) {
						_logger.error("updateFanucDisplayTable :: Error fetching db connection ");
					}
				}
			} catch (Exception e) {
				if (_errorLog) {
					_logger.error(
							"updateFanucDisplayTable :: Error while processing for dashboard voice--- ",
							e);
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("updateFanucDisplayTable ::  Error  ", e);
			}
		} finally {

			if (selectStatement != null) {
				try {
					selectStatement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("updateFanucDisplayTable ::  Error "
								+ " table.", ex);
					}
				}
				selectStatement = null;
			}

		}
	}

	private StringBuilder getqueueList(List<Integer> queueList) {
		Iterator<Integer> it = queueList.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			sb.append(it.next()).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		if (_debugLog) {
			_logger.debug("getQueueList--> " + sb);
		}
		return sb;
	}

	private String parseQueryy(String selectFromRaw) {

		try {
			String re1 = "(\\$)"; // Any Single Character 1
			String re2 = "(P)"; // Variable Name 1
			String re3 = "(\\{)"; // Any Single Character 2
			String re4 = "(queueList)";
			String re5 = "(\\})"; // Any Single Character 3
			String regex = null;

			Pattern p = Pattern.compile(re1 + re2 + re3 + re4 + re5,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher m = p.matcher(selectFromRaw);

			while (m.find()) {
				String c1 = m.group(1);
				String var1 = m.group(2);
				String c2 = m.group(3);
				String alphanum = m.group(4);
				String c3 = m.group(5);
				regex = c1.toString() + var1.toString() + c2.toString()
						+ alphanum.toString() + c3.toString();
			}
			selectFromRaw = selectFromRaw.replace(regex,
					getqueueList(queueList));
			if (_debugLog) {
				_logger.debug("parseQuery:: query is " + selectFromRaw);
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("parseQuery:: Exception ", e);
			}
		} finally {
			if (_debugLog) {
				_logger.debug("parseQuery:: Finally of queryAnalyser--"
						+ selectFromRaw);
			}
		}

		return selectFromRaw;

	}

	/**
	 * this nethod finds total emails handled since morning
	 * it reads query from properties file which goes in secondary database and finds count
	 * count is updated in a map against queueId
	 * @param queueCountMap
	 */
	private void getEmailsHandledCount(Map<Integer, Integer> queueCountMap) {

		PreparedStatement emailsStatement = null;
		ResultSet resultSet = null;
		Connection connection = null;

		if (_debugLog) {
			_logger.debug("getEmailsHandledCount:: queueGroups passed to find total emails handled--->"
					+ queueGroups);
		}
		// Map.Entry mapEntry = null;
		connection = MMConnectionPool.getSecondaryConnection();

		try {
			for (Integer queuegroupId : queueGroups) {

				try {

					String query1 = ConnectionPoolManager.getInstance()
							.getProperty("WIC", "FIND_EMAILS_HANDLED");// replaceFirst("quot",
																		// quot).replaceAll("##queueGroupId##",
																		// queueGroup);

					String getEmails = query1.replace("##queueGroupId##",
							String.valueOf(queuegroupId));// .replaceAll("##queueGroupId##",
															// String.valueOf(queueGroup));

					if (connection != null) {
						if (_debugLog) {
							_logger.debug("getEmailsHandledCount::  = Query Fired to get Emails handled--->"
									+ getEmails);
						}
						emailsStatement = connection
								.prepareStatement(getEmails);
						resultSet = emailsStatement.executeQuery();
						while (resultSet.next()) {
							queueCountMap.put(resultSet.getInt("QUEUEID"),
									resultSet.getInt("EMAILS_HANDLED"));
						}
						if (_debugLog) {
							_logger.debug("getEmailsHandledCount :: Updated queueCount map is "
									+ queueCountMap);
						}

					} else {
						if (_debugLog) {
							_logger.debug("getEmailsHandledCount :: Error fetching secondary database connection");
						}
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"getEmailsHandledCount :: Error getting emails handled for queue groupid :: "
										+ queuegroupId, e);
					}
				}

			}
		} catch (Exception ex) {
			if (_errorLog) {
				_logger.error(
						"getEmailsHandledCount :: Error--> while getting emails handled  --- ",
						ex);
			}
		} finally {
			if (connection != null) {
				try {
					MMConnectionPool.freeSecondaryConnection(connection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"getEmailsHandledCount :: Error while closing the secondary connection  "
										+ " ", ex);
					}
				}
			}
		}

	}

	/**
	 * This method updates emails handled count in raw derby table
	 * @param queueCduCount
	 * @param dbConnection
	 */
	private void updateRawTable(Map<Integer, Integer> queueCduCount,
			Connection dbConnection) {
		Connection derbyConnection = null;
		PreparedStatement stmtderby = null;
		ResultSet rs = null;
		Map.Entry mapEntry = null;
		Iterator<Entry<Integer, Integer>> iteratorMap = queueCduCount
				.entrySet().iterator();
		try {
			derbyConnection = dbConnection;

			if (derbyConnection != null) {

				while (iteratorMap.hasNext()) {
					mapEntry = (Map.Entry) iteratorMap.next();

					// FIXME TO BE RE-FACTORED TO ANOTHER METHOD THAT WILL
					// ACCEPT THE ABOVE HASHMAP AS ARGUMENT AND WHILE ITERATING
					// EXECUTE THIS UPDATE QUERY
					//Done
					String insertQuery = "update APP.QSTATS_RAW_DATA set D170=? where PKEY=?";
					stmtderby = derbyConnection.prepareStatement(insertQuery);

					stmtderby.setInt(1, (int) mapEntry.getValue());

					stmtderby.setInt(2, (int) mapEntry.getKey());

					stmtderby.executeUpdate();
					if (_debugLog) {
						_logger.debug("updateRawTable ::With Parameters EMAILS_HANDLED (D170)= "
								+ mapEntry.getValue()
								+ " For queueId (PKEY)=  " + mapEntry.getKey());
					}
				}

			} else {
				if (_errorLog) {
					_logger.error("updateRawTable :: Error fetching derby database connection");
				}
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("updateRawTable :: Error in Executing Query--- ",
						e);
			}

		} finally {
			/*
			 * if (dbConnection != null) {
			 * 
			 * try { MMConnectionPool.freeDerbyConnection(derbyConnection); }
			 * catch (Exception ex) { if (_errorLog) { _logger.error(
			 * "DashboardEmailFetcher :: Error while closing the derby connection "
			 * + " ", ex); } }
			 * 
			 * }
			 */

		}

	}

}
