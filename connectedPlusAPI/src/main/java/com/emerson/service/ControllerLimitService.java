package com.emerson.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.emerson.dao.ControllerLimitDao;
import com.emerson.model.Content;
import com.emerson.model.DeviceList;
import com.emerson.model.DynamicSiteList;
import com.emerson.model.PointsList;
import com.emerson.model.SiteList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Service
@Slf4j
public class ControllerLimitService {

	@Value("${AzureWebAppURL}")
	private String AzureURl;

	@Autowired
	private AmqpTemplate amqpTemplate;

	@Autowired
	private ControllerLimitDao controllerLimitDao;

	
	@Value("${device.details.endpoint}")
	private String deviceEndpoint;
	
	@Value("${point.details.endpoint}")
	private String pointEndpoint;
	
	@Value("${logs.details.endpoint}")
	private String logsEndpoint;
			
	@Value("${site.details.endpoint}")
	private String siteEndpoint;
	
	@Value("${dynamicsite.details.endpoint}")
	private String dynamicSiteEndpoint;
	
	@Value("${dg.details.endpoint}")
	private String dgEndpoint;
	
	@Value("${dynamiclogs.details.endpoint}")
	private String dynamiclogsEndpoint;
	
	@Value("${site.details.customer}")
	private String customerNameProperty="DOLLAR_GENERAL";
	
	
	
	public void controllerLimitCustomerAttributes() {
		controllerLimitDao.controllerLimitCustomerAttributes();
	}

	public void controllerLimitData(JsonObject resultJson) {
		controllerLimitDao.controllerLimitData(resultJson);
	}

	public void controllerLimitCustomerAttributesQueue() {
		controllerLimitDao.controllerLimitCustomerAttributesQueue();
	}

	public void appInstanceIdUpdate() {
		controllerLimitDao.appInstanceIdUpdate();
	}

	public void getSiteDetails() {
		log.info("services method getSiteDetails start");
		controllerLimitDao.getSiteDetails();
	}

	public void getDeviceDetails(String customerName) {
		controllerLimitDao.getDeviceDetails(customerName);
	}

	public void controllerLimitInputData(JsonObject resultJson) {
	/*	Gson gson = new Gson();
		String customerName = resultJson.get("customer_name").toString().replaceAll("\"", "");
		String attributeName = resultJson.get("reference_Name").toString().replaceAll("\"", "");
		try {

			Map<String, String> row = new HashMap<>();
			row.put("customer_name", customerName);
			row.put("reference_Name", attributeName);
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(row), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
			CustomerList cl = new Gson().fromJson(resultS, CustomerList.class);
			if (cl.isStatus()) {

			} else {
				resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
				CustomerList cl1 = new Gson().fromJson(resultS, CustomerList.class);

			}
			amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
		} catch (Exception ex) {
			ex.printStackTrace();
			Map<String, String> row = new HashMap<>();
			row.put("customer_name", customerName);
			row.put("reference_Name", attributeName);
			try {
				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
				HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(row), headers);
				String resultS = null;
				RestTemplate restTemplate = new RestTemplate();
				resultS = restTemplate.postForObject(AzureURl, httpEntity, String.class);
				CustomerList cl2 = new Gson().fromJson(resultS, CustomerList.class);
				amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
			} catch (Exception e) {
				amqpTemplate.convertAndSend("", "adm.controllerLimit", gson.toJson(row));
				System.out.println(e.getMessage());
			}
		}*/
	}

	public void processSiteData(JsonObject resultJson) {
		Gson gson = new Gson();
		log.info("services method processSiteData");
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			System.out.println("resultJson sites processing:::"+resultJson.toString());
			resultS = restTemplate.postForObject(deviceEndpoint, httpEntity, String.class);
			System.out.println("resultS sites processing:::"+resultS);
			DeviceList dl = new Gson().fromJson(resultS, DeviceList.class);
			log.info("before publish adm.device1");
			amqpTemplate.convertAndSend("", "adm.device1", gson.toJson(resultJson));
			log.info("after publish adm.device1");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public void processSitePreprocessData(JsonObject resultJson) {
		Gson gson = new Gson();
		log.info("services method processSitePreprocessData");
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			
			resultS = restTemplate.postForObject(siteEndpoint, httpEntity, String.class);
			SiteList sl = new Gson().fromJson(resultS, SiteList.class);
			for(Content content:sl.getContent()) {
				Map<String, Object> row = new HashMap<>();
				row.put("customer_name", customerNameProperty);
				row.put("site_id", content.getId());
				log.info("before publish adm.site");
				amqpTemplate.convertAndSend("", "adm.site", gson.toJson(row));
				log.info("after publish adm.site");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public void processDeviceData(JsonObject resultJson) {
		Gson gson = new Gson();

		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			resultS = restTemplate.postForObject(pointEndpoint, httpEntity, String.class);
			PointsList sl = new Gson().fromJson(resultS, PointsList.class);
			

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void processDeviceRawData(JsonObject resultJson) {
		log.info("Services method process device raw data start");
		controllerLimitDao.processDeviceRawData(resultJson);
	}

	public void processLogsData(JsonObject resultJson) {
		Gson gson = new Gson();

		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			resultS = restTemplate.postForObject(logsEndpoint, httpEntity, String.class);
			SiteList sl = new Gson().fromJson(resultS, SiteList.class);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public void getSiteDetailsProd(String customerName) {
		controllerLimitDao.getSiteDetailsProd(customerName);
	}
	public void getDeviceDetailsProd(String customerName,long siteId) {
		controllerLimitDao.getDeviceDetailsProd(customerName,siteId);
	}
	public void generateLogs(String customerName) {
		log.info("services method generateLogs started");
		controllerLimitDao.generateLogs(customerName);
	}
	
	public void realtimeDataProcessing() {
		log.info("services method realtimeDataProcessing started");
		controllerLimitDao.realtimeDataProcessing();
	}
	
	public void dynamicSiteAPi(JsonObject resultJson) {
		Gson gson = new Gson();
		log.info("services method dynamicSiteAPi");
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = null;
			RestTemplate restTemplate = new RestTemplate();
			
			resultS = restTemplate.postForObject(dgEndpoint, httpEntity, String.class);
			DynamicSiteList sl = new Gson().fromJson(resultS, DynamicSiteList.class);
			log.info("dg Response from MLAPI: "+resultS);
			/*for(int i=0;i<=29;i++) {
				//log.info("Loop started");
				sl.setNumber(i);
				
				amqpTemplate.convertAndSend("", "adm.logProcess", gson.toJson(sl));
				//log.info("after publish adm.site");
			}*/
			
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

	}
	public void dynamicPointsAPi(JsonObject resultJson) {
		log.info("Services method dynamicPointsAPi start");
		controllerLimitDao.dynamicPointsAPi(resultJson);

	}

	public void dynamicLogsAPi(JsonObject resultJson) {
		Gson gson = new Gson();
		log.info("services method dynamicLogsAPi");
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		try {
			
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			
			HttpEntity<String> httpEntity = new HttpEntity<>(gson.toJson(resultJson), headers);
			String resultS = restTemplate.postForObject(dynamiclogsEndpoint, httpEntity, String.class);
			log.info("dynamicLogsAPi Response from MLAPI: "+resultS);
				
			
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

	}
}
