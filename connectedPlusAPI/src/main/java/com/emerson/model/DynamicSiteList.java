package com.emerson.model;

import java.math.BigDecimal;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DynamicSiteList {
	String Message;
	String customer_name;
	long site_id;
	long point_id;
	long app_instance;
	long last_time;
	long number;
	long alarm_id;
}
