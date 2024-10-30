package com.emerson.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.emerson.MLAPIRMQIntegration.MlapirmqIntegrationApplication;
import com.emerson.service.ControllerLimitService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/")
@Slf4j
public class ControllerLimit {

	
	@Autowired
	private ControllerLimitService controllerLimitService;
	
	
	@GetMapping(value = "/controllerLimitCustomerAttributes")
	public void controllerLimitCustomerAttributes() {
		
		controllerLimitService.controllerLimitCustomerAttributes();
	}
	@GetMapping(value = "/controllerLimitCustomerAttributesQueue")
	public void controllerLimitCustomerAttributesQueue() {
		
		controllerLimitService.controllerLimitCustomerAttributesQueue();
	}
	@GetMapping(value = "/appInstanceIdUpdate")
	public void appInstanceIdUpdate() {
		
		controllerLimitService.appInstanceIdUpdate();
	}
	
	@GetMapping(value = "/getSiteDetails")
	public void getSiteDetails() {
		log.info("call getSiteDetails start");
		controllerLimitService.getSiteDetails();
	}
	@PostMapping(value = "/getDeviceDetails")
	public void getDeviceDetails(String customerName) {
		controllerLimitService.getDeviceDetails(customerName);
	}
	
	@PostMapping(value = "/getSiteDetailsProd")
	public void getSiteDetailsProd(@RequestParam("customerName") String customerName) {
		
		controllerLimitService.getSiteDetailsProd(customerName);
	}
	@PostMapping(value = "/getDeviceDetailsProd")
	public void getDeviceDetailsProd(@RequestParam("customerName") String customerName,@RequestParam("siteId") long siteId) {
		
		controllerLimitService.getDeviceDetailsProd(customerName,siteId);
	}
	
	
	@PostMapping(value = "/generateLogs")
	
	public void generateLogs(@RequestParam("customerName") String customerName) {
		
		controllerLimitService.generateLogs(customerName);
	}
	@GetMapping(value = "/generateLogsNoArg")
	//@Scheduled(cron = "10 10 02 1/1 * ?")
	public void generateLogsNoArg() {
		log.info("call generateLogsNoArg started");
		controllerLimitService.generateLogs("DOLLAR_GENERAL");
	}
	
	
	
	@GetMapping(value = "/realtimeDataProcessing")
	@Scheduled(cron="0 0/10 * 1/1 * ?")
	public void realtimeDataProcessing() {
		log.info("call realtimeDataProcessing started");
		controllerLimitService.realtimeDataProcessing();
	}
}
