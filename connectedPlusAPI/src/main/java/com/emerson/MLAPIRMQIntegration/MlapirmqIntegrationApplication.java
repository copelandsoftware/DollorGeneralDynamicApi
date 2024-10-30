package com.emerson.MLAPIRMQIntegration;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.emerson.service.ControllerLimitService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

/* @Author :Gauri.Malwe@Emerson.com
MlapirmqIntegrationApplication class is the entry point for Rabbit MQ- Azure ML API integration
 */

@SpringBootApplication(scanBasePackages = { "com.emerson" })
@Slf4j
@EnableRabbit
@EnableScheduling
public class MlapirmqIntegrationApplication {

	@Autowired
	private ControllerLimitService controllerLimitService;

	public static void main(String[] args) {
		SpringApplication.run(MlapirmqIntegrationApplication.class, args);
	}
	/*
	 * @RabbitListener(queues = "adm.sitePreprocess",concurrency="20-20")
	 * 
	 * @RabbitHandler public void processSitePreprocessData(Message message) { try {
	 * String messagereceived = new String(message.getBody());
	 * log.info("Site Preprocess: "+messagereceived);
	 * 
	 * JsonObject messageJson = new Gson().fromJson(messagereceived,
	 * JsonObject.class);
	 * controllerLimitService.processSitePreprocessData(messageJson);
	 * 
	 * } catch (Exception e) { log.info(e.getMessage()); } }
	 * 
	 * @RabbitListener(queues = "adm.site",concurrency="20-20")
	 * 
	 * @RabbitHandler public void processSiteData(Message message) { try { String
	 * messagereceived = new String(message.getBody());
	 * log.info("Site: "+messagereceived);
	 * 
	 * JsonObject messageJson = new Gson().fromJson(messagereceived,
	 * JsonObject.class); controllerLimitService.processSiteData(messageJson);
	 * 
	 * } catch (Exception e) { log.info(e.getMessage()); } }
	 * 
	 * 
	 * @RabbitListener(queues = "adm.device1",concurrency="20-20")
	 * 
	 * @RabbitHandler public void processDeviceRawData(Message message) { try {
	 * String messagereceived = new String(message.getBody());
	 * log.info("Device 1:"+messagereceived);
	 * 
	 * JsonObject messageJson = new Gson().fromJson(messagereceived,
	 * JsonObject.class); controllerLimitService.processDeviceRawData(messageJson);
	 * 
	 * } catch (Exception e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } }
	 * 
	 * @RabbitListener(queues = "adm.device2",concurrency="20-20")
	 * 
	 * @RabbitHandler public void processDeviceData(Message message) { try { String
	 * messagereceived = new String(message.getBody());
	 * log.info("Device 2: "+message);
	 * 
	 * JsonObject messageJson = new Gson().fromJson(messagereceived,
	 * JsonObject.class); controllerLimitService.processDeviceData(messageJson);
	 * 
	 * } catch (Exception e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } }
	 * 
	 * @RabbitListener(queues = "adm.logs",concurrency="20-20")
	 * 
	 * @RabbitHandler public void processLogsData(Message message) { try { String
	 * messagereceived = new String(message.getBody()); log.info("Logs "+message);
	 * 
	 * JsonObject messageJson = new Gson().fromJson(messagereceived,
	 * JsonObject.class); controllerLimitService.processLogsData(messageJson);
	 * 
	 * } catch (Exception e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } }
	 */

	@RabbitListener(queues = "adm.dynamicSite", concurrency = "5-5")
	@RabbitHandler
	public void dynamicSiteAPi(Message message) {
		try {
			String messagereceived = new String(message.getBody());
			log.info("dynamicSiteAPi: " + messagereceived);

			JsonObject messageJson = new Gson().fromJson(messagereceived, JsonObject.class);
			controllerLimitService.dynamicSiteAPi(messageJson);

		} catch (Exception e) {
			log.info(e.getMessage());
		}
	}

	@RabbitListener(queues = "adm.pointInput", concurrency = "5-5")
	@RabbitHandler
	public void dynamicPointsAPi(Message message) {
		try {
			String messagereceived = new String(message.getBody());
			log.info("dynamicPointsAPi: " + messagereceived);

			JsonObject messageJson = new Gson().fromJson(messagereceived, JsonObject.class);
			controllerLimitService.dynamicPointsAPi(messageJson);

		} catch (Exception e) {
			log.info(e.getMessage());
		}
	}

	@RabbitListener(queues = "adm.logProcess", concurrency = "5-5")
	@RabbitHandler
	public void dynamicLogsAPi(Message message) {
		try {
			String messagereceived = new String(message.getBody());
			log.info("dynamic logProcess: " + messagereceived);

			JsonObject messageJson = new Gson().fromJson(messagereceived, JsonObject.class);
			long pointId = messageJson.get("point_id").getAsLong();
			if (pointId != 0) {
				log.info("pointId: " + pointId);
				controllerLimitService.dynamicLogsAPi(messageJson);
			}

		} catch (Exception e) {
			log.info(e.getMessage());
		}
	}
}
