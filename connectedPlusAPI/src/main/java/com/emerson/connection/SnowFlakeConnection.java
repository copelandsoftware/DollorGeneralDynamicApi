package com.emerson.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class SnowFlakeConnection {
	@Value("${snowflake.dg.driver}")
	private String driver;
	@Value("${snowflake.dg.user}")
	private String userName;
	@Value("${snowflake.dg.account}")
	private String account;
	@Value("${snowflake.dg.password}")
	private String password;
	@Value("${snowflake.dg.warehouse}")
	private String warehouse;
	@Value("${snowflake.dg.db}")
	private String db;
	@Value("${snowflake.dg.schema}")
	private String schema;
	@Value("${snowflake.dg.role}")
	private String role;
	@Value("${snowflake.dg.url}")
	private String url;
	
	public Connection getConnection() throws Exception {
		Class.forName(driver);
		Properties properties = new java.util.Properties();
		properties.put("user", userName);
		properties.put("password", password);
		properties.put("account", account);
		properties.put("warehouse", warehouse);
		properties.put("db", db);
		properties.put("schema", schema);
		properties.put("role", role);

		//String url = "jdbc:snowflake://emersoncomres.east-us-2.azure.snowflakecomputing.com/";

		return DriverManager.getConnection(url, properties);
	}
	
	public Connection getWoolWorthConnection() throws Exception {
		Class.forName(driver);
		Properties properties = new java.util.Properties();
		properties.put("user", userName);
		properties.put("password", password);
		properties.put("account", account);
		properties.put("warehouse", warehouse);
		properties.put("db", db);
		properties.put("schema", "WOOLWORTHS");
		properties.put("role", role);

		//String url = "jdbc:snowflake://emersoncomres.east-us-2.azure.snowflakecomputing.com/";

		return DriverManager.getConnection(url, properties);
	}
	
	public Connection getDollerGeneralConnection() throws Exception {
		Class.forName(driver);
		Properties properties = new java.util.Properties();
		properties.put("user", userName);
		properties.put("password", password);
		properties.put("account", account);
		properties.put("warehouse", warehouse);
		properties.put("db", db);
		properties.put("schema", "DOLLAR_GENERAL");
		properties.put("role", role);

		//String url = "jdbc:snowflake://emersoncomres.east-us-2.azure.snowflakecomputing.com/";

		return DriverManager.getConnection(url, properties);
	}
}
