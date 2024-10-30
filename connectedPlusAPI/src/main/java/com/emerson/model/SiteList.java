package com.emerson.model;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SiteList {
	String Message;
	String customer_name;
	
	double Time_Taken_SEC;	
	List<Content> content=new ArrayList<>();
}
