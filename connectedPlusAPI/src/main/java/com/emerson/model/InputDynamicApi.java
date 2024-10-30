package com.emerson.model;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class InputDynamicApi {
	private String customerName;
	private List<String> siteName;
	private String applicationInstance;
	private String unitName;
    private String  pointName;
    private long alarmId;
    private long pointId;
}
