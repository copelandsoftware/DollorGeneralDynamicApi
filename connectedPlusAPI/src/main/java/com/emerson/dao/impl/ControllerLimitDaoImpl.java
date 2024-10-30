package com.emerson.dao.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.emerson.connection.SnowFlakeConnection;
import com.emerson.dao.ControllerLimitDao;
import com.emerson.model.InputDynamicApi;
import com.emerson.model.SiteList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ControllerLimitDaoImpl implements ControllerLimitDao {

	@Autowired
	private AmqpTemplate amqpTemplate;

	@Autowired
	private SnowFlakeConnection snowFlakeConnection;

	@Value("${AzureWebAppURL}")
	private String AzureURl;
	
	@Value("${adm.pmlq.time.daylightsaving}")
	private String daylight;

	@Value("${site.details.endpoint}")
	private String siteEndpoint;
	
	@Value("${site.details.customer}")
	private String customerNameProperty="DOLLAR_GENERAL";
	
	@Value("${point.details.pointname.list}")
	private String pointNameList;
	
	@Value("${site.details.pages}")
	private int pages;
	
	@Value("${customer.list.in.condition}")
	private String condition;
	@Value("${api.Recommendation.list}")
	private String apiRecommendationList;
	
	@Override
	public void controllerLimitCustomerAttributes() {
		/*Gson gson = new Gson();

		List<String> customerList = new ArrayList<>();
		List<String> attributeList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();
				Statement statement1 = connection.createStatement();
				Statement statement2 = connection.createStatement();) {
			String delete = "DELETE FROM CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT";
			String sql = "SELECT * from CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT_CUSTOMER";
			String sql1 = "SELECT * from CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT_ATTRIBUTE";
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSet resultSet1 = statement1.executeQuery(sql1);

			statement2.execute(delete);
			while (resultSet.next()) {

				String customer = resultSet.getString("CUSTOMER");

				customerList.add(customer);
			}
			while (resultSet1.next()) {

				String attrib = resultSet1.getString("ATTRIBUTE");

				attributeList.add(attrib);
			}
			for (String cust : customerList) {
				for (String attribute : attributeList) {
					try {
						Map<String, String> row = new HashMap<>();
						row.put("customer_name", cust);
						row.put("reference_Name", attribute);
						HttpHeaders headers = new HttpHeaders();
						headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
						HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(row), headers);
						String resultS = null;
						RestTemplate restTemplate = new RestTemplate();
						resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
						CustomerList cl = new Gson().fromJson(resultS, CustomerList.class);
						if (cl.isStatus()) {
							amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
						} else {
							resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
							CustomerList cl1 = new Gson().fromJson(resultS, CustomerList.class);
							if (cl1.isStatus()) {
								amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
							}
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						Map<String, String> row = new HashMap<>();
						row.put("customer_name", cust);
						row.put("reference_Name", attribute);
						HttpHeaders headers = new HttpHeaders();
						headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
						HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(row), headers);
						String resultS = null;
						RestTemplate restTemplate = new RestTemplate();
						resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
						CustomerList cl = new Gson().fromJson(resultS, CustomerList.class);
						if (cl.isStatus()) {
							amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
						}
					}
				}
			}

		} catch (Exception e) {

			e.printStackTrace();
		}
*/
	}

	@Override
	public void controllerLimitData(JsonObject resultJson) {
	/*	String customerName = resultJson.get("customer_name").toString().replaceAll("\"", "");
		String attributeName = resultJson.get("reference_Name").toString().replaceAll("\"", "");
		List<ControllerDetails> controllerList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();
				Statement statement1 = connection.createStatement();) {
			String sql = "select * FROM CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT where CUSTOMER='" + customerName
					+ "' AND ATTRIBUTE='" + attributeName + "'";

			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {

				String customer = resultSet.getString("CONTROLLER_DETAILS");
				customer = customer.replace("\n", "").replace("\r", "");
				try {
					ControllerDetails controllerDetails = new Gson().fromJson(customer, ControllerDetails.class);
					controllerList.add(controllerDetails);
				} catch (Exception ex) {
					log.warn(ex.getMessage());
				}
			}
			log.info("Records for customer:" + customerName + " and attribute: " + attributeName + " are: "
					+ controllerList.size());
			int i = 0;
			int batchSize = 15000;
			Calendar calendar = Calendar.getInstance();
			int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
			int year = calendar.get(Calendar.YEAR);
			int week = calendar.get(Calendar.WEEK_OF_YEAR);
			PreparedStatement pstmt = connection.prepareStatement(
					"Insert into CONNECT_PLUS.PUBLIC.ALARM_DATA_CONTROLLER_LIMITS(CUSTOMER,POINT_ID,POINT_NAME,LOG_AVAILABLE,UNIT,APP_TYPE_NAME,"
							+ "APP_INSTANCE_NAME,CONTROLLER_UNIT,VALUE_1,WEEK_NUM,YEAR_NUM,DAY_NUM) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");

			for (ControllerDetails cd : controllerList) {
				pstmt.setString(1, customerName);
				pstmt.setLong(2, cd.getId());
				pstmt.setString(3, cd.getPointName());
				pstmt.setString(4, new Boolean(cd.isLogAvailable()).toString());
				pstmt.setString(5, cd.getUnit());
				pstmt.setString(6, cd.getAppTypeName());
				pstmt.setString(7, cd.getAppInstanceName());
				pstmt.setString(8, cd.getControllerUnit());
				pstmt.setString(9, cd.getValue());
				pstmt.setInt(10, week);
				pstmt.setInt(11, year);
				pstmt.setInt(12, dayOfYear);
				pstmt.addBatch();
				i++;
				if (i % batchSize == 0) {
					log.info("Batch Started");
					pstmt.executeBatch();
					pstmt.clearBatch();
					log.info("Batch inserted successfully, Total count:" + i);
				}
			}
			pstmt.executeBatch();
			log.info("Batch inserted successfully,final Total count:" + i);

		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}

	@Override
	public void controllerLimitCustomerAttributesQueue() {
		/*Gson gson = new Gson();

		List<String> customerList = new ArrayList<>();
		List<String> attributeList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();
				Statement statement1 = connection.createStatement();
				Statement statement2 = connection.createStatement();) {
			String delete = "DELETE FROM CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT";
			String sql = "SELECT * from CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT_CUSTOMER";
			String sql1 = "SELECT * from CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT_ATTRIBUTE";
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSet resultSet1 = statement1.executeQuery(sql1);

			statement2.execute(delete);
			while (resultSet.next()) {

				String customer = resultSet.getString("CUSTOMER");

				customerList.add(customer);
			}
			while (resultSet1.next()) {

				String attrib = resultSet1.getString("ATTRIBUTE");

				attributeList.add(attrib);
			}
			for (String cust : customerList) {
				for (String attribute : attributeList) {
					try {
						Map<String, String> row = new HashMap<>();
						row.put("customer_name", cust);
						row.put("reference_Name", attribute);
						amqpTemplate.convertAndSend("", "adm.controllerLimitInput", gson.toJson(row));

					} catch (Exception ex) {
						ex.printStackTrace();

					}
				}
			}

		} catch (Exception e) {

			e.printStackTrace();
		}
*/
	}

	@Override
	public void appInstanceIdUpdate() {

		List<String> customerList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();) {

			String sql = "SELECT * from CONNECT_PLUS.PUBLIC.CONTROLLER_LIMIT_CUSTOMER";
			ResultSet resultSet = statement.executeQuery(sql);

			while (resultSet.next()) {

				String customer = resultSet.getString("CUSTOMER");

				customerList.add(customer);
			}

			for (String cust : customerList) {
				String sqlUpdate = "UPDATE CONNECT_PLUS.PUBLIC.ALARM_DATA_CONTROLLER_LIMITS CON "
						+ "SET APP_INSTANCE_ID = CONNECT_PLUS." + cust + ".REF_POINT.APP_INSTANCE_ID "
						+ "FROM CONNECT_PLUS." + cust + ".REF_POINT where CON.CUSTOMER= '" + cust
						+ "' and CON.POINT_ID = REF_POINT.POINT_ID "
						+ " and CON.INSERT_TIME >= DATEADD(HOUR, -1, CON.INSERT_TIME)";
				log.info("sqlUpdate:  " + sqlUpdate);
				try (Statement statement1 = connection.createStatement();) {
					statement1.executeUpdate(sqlUpdate);
					log.info("App Instance Id updated for " + cust);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	@Override
	public void getSiteDetails() {
		log.info("daoimpl getSiteDetails start");
		try {
			Gson gson = new Gson();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			for(int i=0;i<=pages;i++) {
				Map<String, Object> row = new HashMap<>();
				row.put("customer_name", customerNameProperty);
				row.put("page_number", i);
				log.info("before publish adm.sitePreprocess");
				amqpTemplate.convertAndSend("", "adm.sitePreprocess", gson.toJson(row));
				log.info("after publish adm.sitePreprocess");
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void getDeviceDetails(String customerName) {

		Gson gson = new Gson();
		List<Long> attributeList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();) {
			String sql = "select * from table(GetlogDetails ())";
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				Map<String, Object> row = new HashMap<>();
				//long customer = resultSet.getLong("SITE_ID");
				row.put("customer_name", "DOLLAR_GENERAL");
				row.put("site_id", resultSet.getLong("SITE_ID"));
				row.put("number", 0);
				row.put("point_id",resultSet.getLong("POINT_ID"));
				row.put("app_instance",resultSet.getLong("APP_INST_ID"));
				row.put("last_time",resultSet.getLong("LAST_TIME"));
				amqpTemplate.convertAndSend("", "adm.site", gson.toJson(row));
				//attributeList.add(customer);
			}
/*
			for (long attribute : attributeList) {
				try {
					for (int i = 0; i <= 31; i++) {
						Map<String, Object> row = new HashMap<>();
						row.put("customer_name", "DOLLAR_GENERAL");
						row.put("site_id", attribute);
						row.put("number", i);
						amqpTemplate.convertAndSend("", "adm.site", gson.toJson(row));
					}
				} catch (Exception ex) {
					ex.printStackTrace();

				}
			}*/
		} catch (Exception es) {
			es.printStackTrace();
		}

	}

	@Override
	public void processDeviceRawData(JsonObject resultJson) {
		Gson gson = new Gson();
		log.info("dao impl process device raw data start");
		List<Long> attributeList = new ArrayList<>();
		long siteId = resultJson.get("site_id").getAsLong();
		String customerName = resultJson.get("customer_name").getAsString();
		log.info("customerName:"+customerName+" siteId:"+siteId);
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();) {
			String sql = "SELECT * from CONNECT_PLUS."+customerName+".DEVICE_RAW where SITE_ID=" + siteId;
			log.info("SQL device1: "+sql);
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {

				long customer = resultSet.getLong("DEVICE_ID");

				attributeList.add(customer);
			}
			log.info("attributeList: "+attributeList.size());
			for (long attribute : attributeList) {
				try {

					Map<String, Object> row = new HashMap<>();
					row.put("customer_name", customerName);
					row.put("site_id", siteId);
					row.put("device_id", attribute);
					log.info("before publish adm.device2");
					amqpTemplate.convertAndSend("", "adm.device2", gson.toJson(row));
					log.info("after publish adm.device2");
				} catch (Exception ex) {
					log.error(ex.getMessage());

				}
			}
		} catch (Exception es) {
			es.printStackTrace();
		}

	}

	@Override
	public void getSiteDetailsProd(String customerName) {
		
		try {
			Gson gson = new Gson();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			Map<String, String> row = new HashMap<>();
			row.put("customer_name", customerName);
			// row.put("reference_Name", attribute);
			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(row), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			resultS = restTemplate.postForObject(siteEndpoint, httpEntity, String.class);
			log.info("result Site" + resultS);
			SiteList sl = new Gson().fromJson(resultS, SiteList.class);
			getDeviceDetailsProd(customerName,0);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		
	}

	@Override
	public void getDeviceDetailsProd(String customerName, long siteId) {
		Gson gson = new Gson();
		List<Long> attributeList = new ArrayList<>();
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();) {
			String sql = "select SITE_ID from CONNECT_PLUS."+customerName+".SITE_RAW";
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				long sitenumber=resultSet.getLong("SITE_ID");
				attributeList.add(sitenumber);
			}

			for (long attribute : attributeList) {
				try {
				
						Map<String, Object> row = new HashMap<>();
						row.put("customer_name", customerName);
						row.put("site_id", attribute);
						
						amqpTemplate.convertAndSend("", "adm.site", gson.toJson(row));
				
				} catch (Exception ex) {
					ex.printStackTrace();

				}
			}
		} catch (Exception es) {
			es.printStackTrace();
		}
	}

	@Override
	public void generateLogs(String customerName) {
		Gson gson = new Gson();
		List<Long> attributeList = new ArrayList<>();
		log.info("daoimpl method generateLogs started");
		
		try (Connection connection = snowFlakeConnection.getConnection();
				Statement statement = connection.createStatement();) {
			String sql = "SELECT DISTINCT " + 
					" T1.SITE_ID AS SITEID," + 
					" T1.POINT_ID AS POINTID," + 
					" T1.APP_INST_ID AS APPID," + 
					" T2.LAST_TIME AS LASTTIME " + 
					" FROM CONNECT_PLUS."+customerName+".CAD_GLOBAL_POINT_MASTER  AS T1 " + 
					"LEFT JOIN ( " + 
					" SELECT " + 
					" POINT_ID," + 
					" MAX(GMT_TIME_STAMP) AS LAST_TIME " + 
					" FROM CONNECT_PLUS."+customerName+".LOGS_RAW " + 
					" GROUP BY POINT_ID " + 
					" ) AS T2 ON " + 
					" T1.POINT_ID = T2.POINT_ID " + 
					" WHERE ((POINT_NAME IN ("+pointNameList+") AND IS_LOG_AVAILABLE))  ";
			log.info("sql: "+sql);
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {

				Map<String, Object> row = new HashMap<>();
				
				row.put("customer_name", customerName);
				row.put("site_id", resultSet.getLong("SITEID"));
				row.put("number", 0);
				row.put("point_id",resultSet.getLong("POINTID"));
				row.put("app_instance",resultSet.getLong("APPID"));
				long l=resultSet.getLong("LASTTIME");
				row.put("last_time",resultSet.getLong("LASTTIME"));
				
				log.info("before publish logs");
				amqpTemplate.convertAndSend("", "adm.logs", gson.toJson(row));
				log.info("after publish logs");
			}

			
		} catch (Exception es) {
			log.error(es.getMessage());
			es.printStackTrace();
		}
	}

	@Override
	public void realtimeDataProcessing() {
		
		try {
			Gson gson = new Gson();
			Map<String, Object> rowMap = new HashMap<>();
			Connection connection = snowFlakeConnection.getConnection();

			Statement statement = connection.createStatement();
			String fromDate = null;
			String toDate = null;
			String time_receivedString = null;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date time_receivedDate=null;
			if (daylight.equalsIgnoreCase("true")) {	
				Calendar cal = Calendar.getInstance();
				// cal.add(Calendar.DATE, -1);
				cal.add(Calendar.HOUR, 3);// EST STage and prod +3 IST -6  3 -5
				cal.add(Calendar.MINUTE,50);// EST STage and prod +20 IST -10  50 -40
				Date chunkDate = cal.getTime();

				/*
				 * chunkDate.add(Calendar.HOUR,-5); chunkDate.setMinutes(00);
				 * chunkDate.setSeconds(00);
				 */
				Calendar cal1 = Calendar.getInstance();
				// cal.add(Calendar.DATE, -1);
				cal1.add(Calendar.HOUR,4);// EST STage and prod +3 IST -5 4 -5
				cal1.add(Calendar.MINUTE, 00);// EST STage and prod +50 IST -40 00 -30
				Date chunkDate1 = cal1.getTime();
				Calendar cal2 = Calendar.getInstance();
				cal2.add(Calendar.HOUR, 4);// EST STage and prod +4 IST -5
				cal2.add(Calendar.MINUTE, 00);// EST STage and prod +00 IST -30
				time_receivedDate = cal2.getTime();

				
				try {
					fromDate = sdf.format(chunkDate);
					toDate = sdf.format(chunkDate1);
					time_receivedString = sdf.format(time_receivedDate);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				Calendar cal = Calendar.getInstance();
				// cal.add(Calendar.DATE, -1);
				cal.add(Calendar.HOUR, 4);// EST STage and prod +3 IST -6
				cal.add(Calendar.MINUTE, 50);// EST STage and prod +20 IST -10
				Date chunkDate = cal.getTime();

				
//				 chunkDate.add(Calendar.HOUR,-5); chunkDate.setMinutes(00);
//				  chunkDate.setSeconds(00);
				 
				Calendar cal1 = Calendar.getInstance();
				// cal.add(Calendar.DATE, -1);
				cal1.add(Calendar.HOUR, 5);
				cal1.add(Calendar.MINUTE, 00);
				Date chunkDate1 = cal1.getTime();
				Calendar cal2 = Calendar.getInstance();
				cal2.add(Calendar.HOUR, 5);
				cal2.add(Calendar.MINUTE, 00);
				time_receivedDate = cal2.getTime();

				
				try {
					fromDate = sdf.format(chunkDate);
					toDate = sdf.format(chunkDate1);
					time_receivedString = sdf.format(time_receivedDate);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			log.info("Get Snowflake data triggered at :" + new Date());
			String sql = "SELECT * from CONNECT_PLUS.PUBLIC.ALARM_DATA_LOG where delayed_flag=false AND INSERT_TIME >='"+ fromDate +
					"' and INSERT_TIME<='" + toDate+ "' AND API_RECOMMENDATION IN ("+apiRecommendationList+") AND COMMENTS NOT IN ('DAFN') AND CUSTOMER IN ("+condition+")";
					
					//" and CUSTOMER IN ('DOLLAR_GENERAL','WOOLWORTHS') AND COMMENTS != 'DAFN' and DELAYED_FLAG = false and API_RECOMMENDATION = 'RECOMMENDATION NOT AVAILABLE'";
		
			log.info("SQL :" + sql);
			ResultSet resultSet = statement.executeQuery(sql);
			
			
			while (resultSet.next()) {
				long pointId=resultSet.getLong("POINT_ID");
				boolean connectPlusFlag=resultSet.getBoolean("CONNECTPLUS_LOG_FLAG");
				boolean baselineFlag=resultSet.getBoolean("BASELINE_LOG_FLAG");
				long alarmId=resultSet.getLong("ALARM_ID");
				String customerName=resultSet.getString("CUSTOMER").replaceAll("\"", "");
				//if(pointId ==0) {
					InputDynamicApi inputdq=new InputDynamicApi();
					inputdq.setApplicationInstance(resultSet.getString("APPLICATION_INSTANCE").replaceAll("\"", ""));
					inputdq.setCustomerName(resultSet.getString("CUSTOMER").replaceAll("\"", ""));
					inputdq.setPointName(resultSet.getString("POINT_NAME").replaceAll("\"", ""));
					inputdq.setUnitName(resultSet.getString("UNIT_NAME").replaceAll("\"", ""));
					inputdq.setAlarmId(resultSet.getLong("ALARM_ID"));
					inputdq.setPointId(pointId);
					String regex = "\\[|\\]";
					String siteName = resultSet.getString("SITE_NAME");
					String[] siteNameArray = siteName.replaceAll("\"", "").replaceAll(regex, "").split(",");
					List<String> siteNameList = new ArrayList<>();
					for (int i = 0; i < siteNameArray.length; i++) {
						siteNameList.add(siteNameArray[i]);
					}
					inputdq.setSiteName(siteNameList);
					try {
					amqpTemplate.convertAndSend("", "adm.dynamicSite", gson.toJson(inputdq));
					}catch(Exception ex) {
						ex.printStackTrace();
					}
					
				/*}else if(pointId!=0 && connectPlusFlag &&!baselineFlag) {
					rowMap.put("point_id", pointId);
					rowMap.put("alarmId", alarmId);
					rowMap.put("customerName", customerName);
					
					try {
						amqpTemplate.convertAndSend("", "adm.pointInput", gson.toJson(rowMap).getBytes());
						}catch(Exception ex1) {
							ex1.printStackTrace();
						}
				}
	*/		}
		
		}catch(Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void dynamicPointsAPi(JsonObject resultJson) {
		Gson gson = new Gson();
		List<Long> attributeList = new ArrayList<>();
		log.info("daoimpl method generateLogs started");
		long pointId=resultJson.get("point_id").getAsLong();
		long alarmId=resultJson.get("alarmId").getAsLong();
		String customerName=resultJson.get("customerName").getAsString();
		if("WOOLWORTHS".equalsIgnoreCase(customerName)) {
			try (Connection connection = snowFlakeConnection.getWoolWorthConnection();
					Statement statement = connection.createStatement();) {
				String sql = "SELECT DISTINCT T1.SITE_ID,T1.APP_INSTANCE_ID,T2.LAST_TIME FROM CAD_GLOBAL_POINT_MASTER AS T1 " + 
						"LEFT JOIN (SELECT POINT_ID,MAX(GMT_TIME_STAMP) AS LAST_TIME FROM LOGS_RAW GROUP BY POINT_ID) AS T2 ON T1.POINT_ID = T2.POINT_ID " + 
						"WHERE T1.POINT_ID ="+pointId;
				log.info("sql: "+sql);
				ResultSet resultSet = statement.executeQuery(sql);
				while (resultSet.next()) {

					Map<String, Object> row = new HashMap<>();
					
					//row.put("customer_name", customerName);
					row.put("site_id", resultSet.getLong("SITE_ID"));
					
					row.put("point_id",pointId);
					row.put("app_instance",resultSet.getLong("APP_INSTANCE_ID"));
					row.put("alarmId",alarmId);
					long l=resultSet.getLong("LAST_TIME");
					row.put("last_time",resultSet.getLong("LAST_TIME"));
				/*	for(int i=0;i<=29;i++) {
						row.put("number", i);
						amqpTemplate.convertAndSend("", "adm.logProcess", gson.toJson(row).getBytes());
					}
				*/				
				}

				
			} catch (Exception es) {
				log.error(es.getMessage());
				es.printStackTrace();
			}
	
		}else {
			try (Connection connection = snowFlakeConnection.getDollerGeneralConnection();
					Statement statement = connection.createStatement();) {
				String sql = "SELECT DISTINCT T1.SITE_ID,T1.APP_INSTANCE_ID,T2.LAST_TIME FROM CAD_GLOBAL_POINT_MASTER AS T1 " + 
						"LEFT JOIN (SELECT POINT_ID,MAX(GMT_TIME_STAMP) AS LAST_TIME FROM LOGS_RAW GROUP BY POINT_ID) AS T2 ON T1.POINT_ID = T2.POINT_ID " + 
						"WHERE T1.POINT_ID ="+pointId;
				log.info("sql: "+sql);
				ResultSet resultSet = statement.executeQuery(sql);
				while (resultSet.next()) {

					Map<String, Object> row = new HashMap<>();
					
					//row.put("customer_name", customerName);
					row.put("site_id", resultSet.getLong("SITE_ID"));
					
					row.put("point_id",pointId);
					row.put("app_instance",resultSet.getLong("APP_INSTANCE_ID"));
					row.put("alarmId",alarmId);
					long l=resultSet.getLong("LAST_TIME");
					row.put("last_time",resultSet.getLong("LAST_TIME"));
					/*for(int i=0;i<=29;i++) {
						row.put("number", i);
						amqpTemplate.convertAndSend("", "adm.logProcess", gson.toJson(row).getBytes());
					}
					*/			
				}

				
			} catch (Exception es) {
				log.error(es.getMessage());
				es.printStackTrace();
			}

		}
		
		
	}

}
