package com.emerson.dao;

import com.google.gson.JsonObject;

public interface ControllerLimitDao {
	public void controllerLimitCustomerAttributes();
	public void controllerLimitData(JsonObject resultJson);
	public void dynamicPointsAPi(JsonObject resultJson);
	public void controllerLimitCustomerAttributesQueue();
	public void appInstanceIdUpdate();
	public void getSiteDetails();
	public void getDeviceDetails(String customerName);
	public void processDeviceRawData(JsonObject resultJson);
	public void getSiteDetailsProd(String customerName);
	public void getDeviceDetailsProd(String customerName,long siteId);
	public void generateLogs(String customerName);
	public void realtimeDataProcessing();
}
