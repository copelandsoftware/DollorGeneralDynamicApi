package com.emerson.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PointsList {
	String Message;
	String customer_name;
	long site_id;
	long device_id;
	double Time_Taken_SEC;	
}
