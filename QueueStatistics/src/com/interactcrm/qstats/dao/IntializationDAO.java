package com.interactcrm.qstats.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.bean.QueryBean;
import com.interactcrm.qstats.db.MMConnectionPool;
import com.interactcrm.qstats.startup.QueryFactory;
import com.interactcrm.util.logging.LogHelper;
import com.interactcrm.utils.Utility;

public class IntializationDAO {
	private static Log _logger = new LogHelper(IntializationDAO.class)
			.getLogger(LogModuleFactory.getModule("QueueStatistics"),
					"Initialization");
	private static boolean _debugLog = false;
	private static boolean _errorLog = false;
	private static boolean _infoLog = false;

	/**
	 * Creates IntializationDAO object and creates logger object for the same.
	 */
	public IntializationDAO() {
	}

	static {
		if (_logger != null) {
			_debugLog = _logger.isDebugEnabled();
			_errorLog = _logger.isErrorEnabled();
			_infoLog = _logger.isInfoEnabled();
		}
	}

	/**
	 * Deletes all older records in table specified by tableName Drops all the
	 * table and recreates
	 * 
	 * @param tableName
	 *            : Name of table
	 * @return true/false as a result of database operations
	 */
	public boolean generateQueueStatsSchema() {

		Connection dbConnection = null;
		PreparedStatement statement = null;
		boolean result = false;
		Properties push = new Properties();
		InputStream in = null;
		TreeMap<Integer, String> queryList = new TreeMap<Integer, String>();
		try {

			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "QueueStatsSchemaGeneration.properties");
			push.load(in);

			Enumeration<Object> keys = push.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);
				queryList.put(Integer.parseInt(key), value);

			}
			if (_debugLog) {
				_logger.debug("initializeDBSchema:: Query Keys fetched from QueueStatsSchemaGeneration----"
						+ queryList.keySet());
			}

			dbConnection = MMConnectionPool.getDerbyConnection();

			for (Integer qKey : queryList.keySet()) {
				try {
					if (dbConnection != null) {
						statement = dbConnection.prepareStatement(queryList
								.get(qKey));
						statement.executeUpdate();
						result = true;
					} else {
						if (_errorLog) {
							_logger.error("initializeDBSchema :: Error fetching db connection in cleaning table");
						}
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"initializeDBSchema :: Error in Executing Query "
										+ qKey + "--- ", e);
					}
				}

			}

			if (_debugLog) {
				_logger.debug("initializeDBSchema:: Query Executed Successfully.");
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("initializeDBSchema ::  Error in cleaning "
						+ " table.", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"initializeDBSchema ::  Error in cleaning "
										+ " table.", ex);
					}
				}
				// statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"initializeDBSchema :: Error in cleaning "
										+ " table.", ex);
					}
				}
				// dbConnection = null;
			}
		}
		return result;
	}

	/**
	 * Populates data in derby tables (raw/derby) for provided tenantGroupList
	 * 
	 * @param tableName
	 *            : Name of table
	 * @return true/false as a result of database operations
	 */
	public boolean generateQueueStatsSeed() {

		Connection dbConnection = null;
		Connection dbConnectionderby = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		boolean result = false;
		Properties push = new Properties();
		InputStream in = null;
		TreeMap<String, String> queryList = new TreeMap<String, String>();
		ArrayList<String> insertQueryList = new ArrayList<String>();

		try {

			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "QueueStatsSeedGeneration.properties");
			push.load(in);

			Enumeration<Object> keys = push.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);
				if (value.contains("TenantGroupPkeyList")) {
					queryList.put(key, parseQuery(value));
				} else {
					queryList.put(key, value);
				}
			}

			if (_debugLog) {
				_logger.debug("generateQueueStatsSeed:: Query keys fetched from QueueStatsSeedGeneration----"
						+ queryList.keySet());
			}

			dbConnection = MMConnectionPool.getDBConnection();

			for (String qKey : queryList.keySet()) {

				if (dbConnection != null) {
					statement = dbConnection.prepareStatement(queryList
							.get(qKey));
					rs = statement.executeQuery();

					if (rs != null) {
						rsmd = rs.getMetaData();

						while (rs.next()) {
							String columnName = "";
							StringBuilder columnValues = new StringBuilder();

							for (int i = 1; i <= rsmd.getColumnCount(); i++) {
								columnName += rsmd.getColumnLabel(i) + ",";

								if (rsmd.getColumnType(i) == Types.VARCHAR) {

									try {
										String str = rs.getString(rsmd
												.getColumnLabel(i));
										if (str != null) {

											if (str.contains("'")) {
												str = str.replaceAll("'", "''");
											}
										}
										columnValues.append("'").append(str).append("',");

									} catch (Exception e) {
										columnValues.append("'',");

									}

								} else if (rsmd.getColumnType(i) == Types.INTEGER) {
									try {
										Integer.parseInt(rs.getString(rsmd
												.getColumnLabel(i)));
										columnValues.append(
												rs.getString(rsmd
														.getColumnLabel(i)))
												.append(",");

									} catch (Exception e) {
										columnValues.append("0,");
									}
								} else if (rsmd.getColumnType(i) == Types.TIMESTAMP) {
									columnValues
											.append("'")
											.append(rs.getString(rsmd
													.getColumnLabel(i)))
											.append("',");

								} else if (rsmd.getColumnType(i) == Types.TINYINT) {
									try {
										Integer.parseInt(rs.getString(rsmd
												.getColumnLabel(i)));
										columnValues.append(
												rs.getString(rsmd
														.getColumnLabel(i)))
												.append(",");

									} catch (Exception e) {
										columnValues.append("0,");

									}
								} else {
									String str = rs.getString(rsmd
											.getColumnLabel(i));
									if (str != null) {

										if (str.contains("'")) {
											str = str.replaceAll("'", "''");
										}
									}
									columnValues.append("'").append(str).append("',");

								}

							}
							columnName = columnName.substring(0,
									columnName.length() - 1);
							String columnValue = columnValues.substring(0,
									columnValues.length() - 1);
							StringBuilder finalString = new StringBuilder();
							finalString.append("insert into APP.").append(qKey)
									.append("(").append(columnName)
									.append(") values (").append(columnValue)
									.append(")");

							insertQueryList.add(finalString.toString());
						}
					} else {

						if (_debugLog) {
							_logger.debug("generateQueueStatsSeed:: ResultSet is Null");
						}
					}

				} else {
					if (_errorLog) {
						_logger.error("generateQueueStatsSeed :: Error fetching db connection ");
					}
				}
			}
			dbConnectionderby = MMConnectionPool.getDerbyConnection();

			for (String query : insertQueryList) {
				try {
					if (dbConnectionderby != null) {
						statement = dbConnectionderby.prepareStatement(query);
						statement.executeUpdate();

					} else {
						if (_errorLog) {
							_logger.error("generateQueueStatsSeed :: Error fetching db connection in cleaning "
									+ " table");
						}
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"generateQueueStatsSeed :: Exception in Insertion query--["
										+ query + "]", e);
					}
				}
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("generateQueueStatsSeed :: E Error in cleaning "
						+ " table.", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"generateQueueStatsSeed ::  Error in cleaning "
										+ " table.", ex);
					}
				}
				statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"generateQueueStatsSeed :: Error in cleaning "
										+ " table.", ex);
					}
				}

			}
			if (dbConnectionderby != null) {
				try {

					MMConnectionPool.freeDerbyConnection(dbConnectionderby);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"generateQueueStatsSeed :: Error in cleaning "
										+ " table.", ex);
					}
				}

			}
		}
		return result;
	}

	/**
	 * This deletes all old data from derby tables , when refersh request is
	 * received
	 * 
	 * @param tgId
	 * @return
	 */
	public boolean deleteOldTgData(String tgId) {

		Connection dbConnection = null;
		PreparedStatement statement = null;
		boolean result = false;
		Properties push = new Properties();
		InputStream in = null;
		TreeMap<Integer, String> queryList = new TreeMap<Integer, String>();
		if (_debugLog) {
			_logger.debug("deleteOldTgData:: Deleteting old data for tgId "
					+ tgId);
		}
		try {

			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "QueueStatsRefresh.properties");
			push.load(in);

			Enumeration<Object> keys = push.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);
				queryList.put(Integer.parseInt(key),
						parseQueryForRefesh(value, tgId));
			}

			if (_debugLog) {
				_logger.debug("deleteOldTgData:: Fetched Queries from QueueStatsRefresh file----"
						+ queryList.keySet());
			}

			dbConnection = MMConnectionPool.getDerbyConnection();

			for (Integer qKey : queryList.keySet()) {
				try {
					if (dbConnection != null) {
						statement = dbConnection.prepareStatement(queryList
								.get(qKey));
						statement.executeUpdate();
						result = true;
					} else {
						if (_errorLog) {
							_logger.error("deleteOldTgData :: Error fetching db connection in cleaning table");
						}
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"deleteOldTgData :: Error in Executing Query ["
										+ qKey + "]--- ", e);
					}
				}

			}

			if (_debugLog) {
				_logger.debug("deleteOldTgData:: Query Executed Successfully.");
			}
		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("deleteOldTgData :: E Error in cleaning "
						+ " table.", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("deleteOldTgData ::  Error in cleaning "
								+ " table.", ex);
					}
				}
				// statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error("deleteOldTgData :: Error in cleaning "
								+ " table.", ex);
					}
				}
				// dbConnection = null;
			}
		}
		return result;

	}

	/**
	 * Here data is populated again derby tables
	 * 
	 * @param tg
	 * @return
	 */
	public boolean updateTablesOnRefesh(String tg) {

		Connection dbConnection = null;
		Connection dbConnectionderby = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		boolean result = false;
		Properties push = new Properties();
		InputStream in = null;
		TreeMap<String, String> queryList = new TreeMap<String, String>();
		ArrayList<String> insertQueryList = new ArrayList<String>();

		if (_debugLog) {
			_logger.debug("Updating raw/display/channel/agents for new changes  ");
		}

		try {

			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "QueueStatsSeedGeneration.properties");
			push.load(in);

			Enumeration<Object> keys = push.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);

				queryList.put(key, parseQueryForRefesh(value, tg));
			}

			if (_debugLog) {
				_logger.debug("Updating raw/display/channel/agents for new changes..Fetched Queries from QueueStatsSeedGeneration "
						+ queryList.keySet());
			}

			dbConnection = MMConnectionPool.getDBConnection();

			for (String qKey : queryList.keySet()) {

				if (dbConnection != null) {
					statement = dbConnection.prepareStatement(queryList
							.get(qKey));
					rs = statement.executeQuery();

					if (rs != null) {
						rsmd = rs.getMetaData();

						while (rs.next()) {
							String columnName = "";
							StringBuilder columnValues = new StringBuilder();

							for (int i = 1; i <= rsmd.getColumnCount(); i++) {
								columnName += rsmd.getColumnLabel(i) + ",";

								if (rsmd.getColumnType(i) == Types.VARCHAR) {

									String str = rs.getString(rsmd
											.getColumnLabel(i));
									if (str != null) {
										if (str.contains("'")) {
											str = str.replaceAll("'", "''");
										}
									}
									try {
										columnValues.append("'").append(str)
												.append("',");

									} catch (Exception e) {
										columnValues.append("'',");

									}

								} else if (rsmd.getColumnType(i) == Types.INTEGER) {
									try {
										Integer.parseInt(rs.getString(rsmd
												.getColumnLabel(i)));
										columnValues.append(
												rs.getString(rsmd
														.getColumnLabel(i)))
												.append(",");

									} catch (Exception e) {
										columnValues.append("0,");

									}
								} else if (rsmd.getColumnType(i) == Types.TIMESTAMP) {
									columnValues
											.append("'")
											.append(rs.getString(rsmd
													.getColumnLabel(i)))
											.append("',");

								} else if (rsmd.getColumnType(i) == Types.TINYINT) {
									try {
										Integer.parseInt(rs.getString(rsmd
												.getColumnLabel(i)));
										columnValues.append(
												rs.getString(rsmd
														.getColumnLabel(i)))
												.append(",");

									} catch (Exception e) {
										columnValues.append("0,");

									}
								} else {
									columnValues
											.append("'")
											.append(rs.getString(rsmd
													.getColumnLabel(i)))
											.append("',");

								}

							}
							columnName = columnName.substring(0,
									columnName.length() - 1);
							String columnValue = columnValues.substring(0,
									columnValues.length() - 1);
							StringBuilder finalString = new StringBuilder();
							finalString.append("insert into APP.").append(qKey)
									.append("(").append(columnName)
									.append(") values(").append(columnValue)
									.append(")");

						
							if (_debugLog) {
								_logger.debug(" [updateTablesOnRefesh ] Updated tables on refresh....");
							}
							insertQueryList.add(finalString.toString());
						}
					} else {

						if (_debugLog) {
							_logger.debug("[updateTablesOnRefesh]:: ResultSet is Null");
						}
					}

				} else {
					if (_errorLog) {
						_logger.error("[updateTablesOnRefesh] :: Error fetching db connection ");
					}
				}
			}

			dbConnectionderby = MMConnectionPool.getDerbyConnection();

			for (String query : insertQueryList) {
				try {
					if (dbConnectionderby != null) {
						statement = dbConnectionderby.prepareStatement(query);
						statement.executeUpdate();

					} else {
						if (_errorLog) {
							_logger.error("[updateTablesOnRefesh] :: Error fetching db connection in cleaning "
									+ " table");
						}
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"[updateTablesOnRefesh] :: Exception in Insertion query--["
										+ query + "]", e);
					}
				}
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("[updateTablesOnRefesh] :: E Error in cleaning "
						+ " table.", e);
			}
		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"[updateTablesOnRefesh] ::  Error in cleaning "
										+ " table.", ex);
					}
				}
				statement = null;
			}
			if (dbConnection != null) {
				try {
					MMConnectionPool.freeConnection(dbConnection);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"[updateTablesOnRefesh] :: Error in cleaning "
										+ " table.", ex);
					}
				}
				dbConnection = null;
			}
			if (dbConnectionderby != null) {
				try {

					MMConnectionPool.freeDerbyConnection(dbConnectionderby);
				} catch (Exception ex) {
					if (_errorLog) {
						_logger.error(
								"[updateTablesOnRefesh] :: Error in cleaning "
										+ " table.", ex);
					}
				}
				dbConnectionderby = null;
			}
		}
		return result;

	}

	private String parseQueryForRefesh(String query, String tgId) {

		try {
			String re1 = "(\\$)"; // Any Single Character 1
			String re2 = "(P)"; // Variable Name 1
			String re3 = "(\\{)"; // Any Single Character 2
			String re4 = "(TenantGroupPkeyList)";
			String re5 = "(\\})"; // Any Single Character 3
			String regex = null;

			Pattern p = Pattern.compile(re1 + re2 + re3 + re4 + re5,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher m = p.matcher(query);

			// if(m.find()){
			while (m.find()) {
				String c1 = m.group(1);
				String var1 = m.group(2);
				String c2 = m.group(3);
				String alphanum = m.group(4);
				String c3 = m.group(5);

				if (_debugLog) {
					_logger.debug("parseQueryForDelete:: alphanum.toString()"
							+ alphanum.toString());
				}
				regex = c1.toString() + var1.toString() + c2.toString()
						+ alphanum.toString() + c3.toString();
			}
			query = query.replace(regex, tgId);

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("parseQueryForDelete:: Exception ", e);
			}
		} finally {
			if (_debugLog) {
				_logger.debug("parseQueryForDelete:: Finally of queryAnalyser--"
						+ query);
			}
		}

		return query;

	}

	private String parseQuery(String query) {
		try {
			String re1 = "(\\$)"; // Any Single Character 1
			String re2 = "(P)"; // Variable Name 1
			String re3 = "(\\{)"; // Any Single Character 2
			String re4 = "(TenantGroupPkeyList)";
			String re5 = "(\\})"; // Any Single Character 3
			String regex = null;

			Pattern p = Pattern.compile(re1 + re2 + re3 + re4 + re5,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher m = p.matcher(query);

			// if(m.find()){
			while (m.find()) {
				String c1 = m.group(1);
				String var1 = m.group(2);
				String c2 = m.group(3);
				String alphanum = m.group(4);
				String c3 = m.group(5);

				regex = c1.toString() + var1.toString() + c2.toString()
						+ alphanum.toString() + c3.toString();
			}
			query = query.replace(regex,
					new QueueStatsDAO().getTenantGroupsList());
			// }

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("parseQuery:: Exception ", e);
			}
		} finally {
			if (_debugLog) {
				_logger.debug("parseQuery:: Finally of queryAnalyser--" + query);
			}
		}

		return query;
	}

	public boolean generateQueueGroupSchema() {
		Connection derbyConnection = null;
		PreparedStatement statement = null;
		boolean result = false;
		TreeMap<Integer, ArrayList<String>> queryList = new TreeMap<Integer, ArrayList<String>>();

		Properties push = new Properties();
		InputStream in = null;
		try {
			if (_debugLog) {
				_logger.debug("generateQueueGroupSchema initailizing");
			}
			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "QueuegroupSchemaGeneration.properties");
			push.load(in);
			derbyConnection = MMConnectionPool.getDerbyConnection();
			Enumeration<Object> keys = push.keys();

			while (keys.hasMoreElements()) {
				ArrayList<String> arr = new ArrayList<String>();
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);

				if (value.contains("##queueGroupId##")) {
					if (derbyConnection != null) {
						String query = "SELECT DISTINCT (queuegroup_pkey) AS QG_PKEY from APP.QSTATS_RAW_DATA WHERE channel <> 2";
						Statement stmt = derbyConnection.createStatement(
								ResultSet.TYPE_SCROLL_INSENSITIVE,
								ResultSet.CONCUR_READ_ONLY);
						ResultSet rs = stmt.executeQuery(query);
						while (rs.next()) {
							String pkey = "" + rs.getInt("QG_PKEY");
							String result1 = value.replaceAll(
									"##queueGroupId##", pkey);
							arr.add(result1);

							if (_debugLog) {
								_logger.debug("generateQueueGroupSchema:: Queries fetched from QueuegroupSchemaGeneration file ["
										+ key
										+ "] for QueueGroup Id ["
										+ pkey
										+ "]----");
							}
						}
					}
				}
				queryList.put(Integer.parseInt(key), arr);
			}

			for (Integer qKey : queryList.keySet()) {
				try {
					if (derbyConnection != null) {
						ArrayList<String> al = queryList.get(qKey);
						for (String s : al) {
							statement = derbyConnection.prepareStatement(s);
							statement.executeUpdate();
							result = true;
						}
						result = true;

					} else {
						if (_errorLog) {
							_logger.error("generateQueueGroupSchema :: Error fetching db connection in cleaning table");
						}
					}
					if (_debugLog) {
						_logger.debug("generateQueueGroupSchema:: Tables created successfully");
					}
				} catch (Exception e) {
					if (_errorLog) {
						_logger.error(
								"generateQueueGroupSchema :: Error in Executing Query "
										+ qKey + "--- ", e);
					}
				}

			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("generateQueueGroupSchema:: Exception ", e);
			}
		} finally {

			if (derbyConnection != null) {
				try {
					MMConnectionPool.freeDerbyConnection(derbyConnection);
				} catch (Exception e3) {
					if (_errorLog) {
						_logger.error(
								"[generateQueueGroupSchema] :: Error in closing connection ",
								e3);
					}
				}
			}

		}
		return result;
	}

	/**
	 * Loads all the queries from Queries.properties .
	 */
	public void initializeQueryStore() {
		try {

			InputStream in = null;
			Properties push = new Properties();
			in = new FileInputStream(Utility.getAppHome() + File.separator
					+ "Queries.properties");
			push.load(in);

			Enumeration<Object> keys = push.keys();

			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = push.getProperty(key);
				QueryBean queryBean = queryAnalyser(value);
				QueryFactory.getInstance().putQuery(key, queryBean);

				if (_debugLog) {
					_logger.debug("intializeQueryStore:: "
							+ QueryFactory.getInstance().getquery(key));
				}
			}

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("intializeQueryStore ::", e);
			}

		}
	}

	private QueryBean queryAnalyser(String query) {
		QueryBean qb = null;

		try {
			LinkedList<String> params = new LinkedList<String>();
			String re1 = "(\\$)"; // Any Single Character 1
			String re2 = "(P)"; // Variable Name 1
			String re3 = "(\\{)"; // Any Single Character 2
			String re4 = "((?:[a-z]*[a-z0-9]*))"; // Alphanum
			String re5 = "(\\})"; // Any Single Character 3
			String regex = null;

			Pattern p = Pattern.compile(re1 + re2 + re3 + re4 + re5,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher m = p.matcher(query);

			while (m.find()) {
				String c1 = m.group(1);
				String var1 = m.group(2);
				String c2 = m.group(3);
				String alphanum = m.group(4);
				String c3 = m.group(5);

				regex = c1.toString() + var1.toString() + c2.toString()
						+ alphanum.toString() + c3.toString();
				params.add(alphanum.toString());
			}
			qb = new QueryBean(query, params);

		} catch (Exception e) {
			if (_errorLog) {
				_logger.error("queryAnalyser:: Exception ", e);
			}
		} finally {
		}
		return qb;
	}

}
