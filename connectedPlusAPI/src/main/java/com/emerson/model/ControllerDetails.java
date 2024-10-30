package com.emerson.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ControllerDetails {
	String appInstanceName;
	String appTypeName;
	String controllerUnit;
	long id;
	boolean logAvailable;
	String pointName;
	String unit;
	String value;
}
