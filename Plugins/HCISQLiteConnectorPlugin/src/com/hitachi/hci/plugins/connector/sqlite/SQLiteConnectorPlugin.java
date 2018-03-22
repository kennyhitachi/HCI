package com.hitachi.hci.plugins.connector.sqlite;

import com.google.common.collect.ImmutableList;
import com.hds.ensemble.plugins.jdbc.BaseJdbcConnectorPlugin;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.connector.ConnectorPlugin;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;

public class SQLiteConnectorPlugin extends BaseJdbcConnectorPlugin {
	private static final String PLUGIN_NAME = "SQLite Database";
	private static final String DISPLAY_NAME = "SQLite Database";
	private static final String DESCRIPTION = "Connector for accessing SQLite Database through JDBC.";
	private static final String LONG_DESCRIPTION = "This connector uses the Java Database Connectivity (JDBC) sql-lite-v3.21.0 driver to connect to SQLite databases.\n\nYou use SQL query syntax when configuring a SQLite Database data connection to specify:\n* Which database table to read from\n* Filtering criteria for limiting which database rows are extracted\n* Which columns to include with extracted database rows\n\n";
	@SuppressWarnings("deprecation")
	private static final PluginConfig DEFAULT_CONFIG = ((PluginConfig.Builder) ((PluginConfig.Builder) ((PluginConfig.Builder) PluginConfig
			.builder()
			.addGroup(new ConfigPropertyGroup.Builder("JDBC Settings", null).setConfigProperties(ImmutableList.of(
					new ConfigProperty.Builder(PROPERTY_CONNECTION), new ConfigProperty.Builder(PROPERTY_USERNAME),
					new ConfigProperty.Builder(PROPERTY_PASSWORD), new ConfigProperty.Builder(PROPERTY_BATCH_SIZE)))))
							.addGroup(new ConfigPropertyGroup.Builder("Query Settings", null).setConfigProperties(
									ImmutableList.of(new ConfigProperty.Builder(PROPERTY_SELECT_COLUMNS),
											new ConfigProperty.Builder(PROPERTY_SELECT_FROM),
											new ConfigProperty.Builder(PROPERTY_SELECT_WHERE)))))
													.addGroup(new ConfigPropertyGroup.Builder("Results Settings", null)
															.setConfigProperties(ImmutableList.of(
																	new ConfigProperty.Builder(PROPERTY_RESULT_ID),
																	new ConfigProperty.Builder(
																			PROPERTY_RESULT_DISPLAY_NAME),
																	new ConfigProperty.Builder(
																			PROPERTY_RESULT_VERSION))))).build();

	public SQLiteConnectorPlugin() {
	}

	private SQLiteConnectorPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		super(config, callback);
	}

	public ConnectorPlugin build(PluginConfig config, PluginCallback pluginCallback) throws ConfigurationException {
		return new SQLiteConnectorPlugin(config, pluginCallback);
	}

	public String getName() {
		return PLUGIN_NAME;
	}

	public String getDisplayName() {
		return DISPLAY_NAME;
	}

	public String getDescription() {
		return DESCRIPTION;
	}

	public String getLongDescription() {
		return LONG_DESCRIPTION;
	}

	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	@Override
	protected String getJdbcDriver() {
		return "org.sqlite.JDBC";
	}
    
	@Override
	protected String getBatchSizeLimitPredicate(int batchSize) {
		return "LIMIT " + batchSize;
	}
}
