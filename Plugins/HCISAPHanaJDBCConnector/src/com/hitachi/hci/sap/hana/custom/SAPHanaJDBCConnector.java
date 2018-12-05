/*
 * ========================================================================
 * 
 * Copyright (c) by Hitachi Data Systems, 2018. All rights reserved.
 * 
 * ========================================================================
 */

package com.hitachi.hci.sap.hana.custom;

import com.google.common.collect.ImmutableList;
import com.hds.ensemble.plugins.jdbc.BaseJdbcConnectorPlugin;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.connector.ConnectorPlugin;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;

/**
 * JDBC Connector Plugin for SAP HANA Database.
 *
 */
public class SAPHanaJDBCConnector extends BaseJdbcConnectorPlugin {

    private static final String PLUGIN_NAME = "SAPHANADatabase";
    private static final String DISPLAY_NAME = "SAP HANA Database";
    private static final String DESCRIPTION = "Connector for accessing SAP HANA database through JDBC.";
    private static final String LONG_DESCRIPTION = "This connector uses the Java Database Connectivity (JDBC) ngdbc driver version 2.5.53 to "
            + "connect to SAP HANA databases.\n"
            + "\nYou use SQL query syntax when configuring a " + DISPLAY_NAME
            + " data connection to specify:"
            + "\n* Which database table to read from"
            + "\n* Filtering criteria for limiting which database rows are extracted"
            + "\n* Which columns to include with extracted database rows"
            + "\n"
            + "\n";

    @SuppressWarnings("deprecation")
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig
            .builder()
            .addGroup(new ConfigPropertyGroup.Builder("JDBC Settings", null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_CONNECTION),
                                new ConfigProperty.Builder(PROPERTY_USERNAME),
                                new ConfigProperty.Builder(PROPERTY_PASSWORD),
                                new ConfigProperty.Builder(PROPERTY_BATCH_SIZE))))
            .addGroup(new ConfigPropertyGroup.Builder("Query Settings", null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_SELECT_COLUMNS),
                                new ConfigProperty.Builder(PROPERTY_SELECT_FROM),
                                new ConfigProperty.Builder(PROPERTY_SELECT_WHERE))))
            .addGroup(new ConfigPropertyGroup.Builder("Results Settings", null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_RESULT_ID),
                                new ConfigProperty.Builder(PROPERTY_RESULT_DISPLAY_NAME),
                                new ConfigProperty.Builder(PROPERTY_RESULT_VERSION))))
            .build();

    public SAPHanaJDBCConnector() {
        super();
    }

    private SAPHanaJDBCConnector(PluginConfig config, PluginCallback callback)
            throws ConfigurationException {
        super(config, callback);
    }

    @Override
    public ConnectorPlugin build(PluginConfig config, PluginCallback pluginCallback)
            throws ConfigurationException {
        return new SAPHanaJDBCConnector(config, pluginCallback);
    }

    @Override
    public String getName() {
        return PLUGIN_NAME;
    }

    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public String getLongDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public PluginConfig getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    @Override
    protected String getJdbcDriver() {
        return "com.sap.db.jdbc.Driver";
    }
    
    @Override
    protected String getBatchSizeLimitPredicate(int batchSize) {
        return "LIMIT "+batchSize;
    }
}
