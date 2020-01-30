/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Vantara, 2019. All rights reserved.
 *
 * ========================================================================
 */

package com.hitachi.hci.plugins.stage.custom;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentFieldValue;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.hitachi.hci.plugins.stage.utils.CountMatches;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CountMetadataFieldValueMatchStage implements StagePlugin {

    private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.custom.countMetadataFieldValueMatchStage";
    private static final String NAME = "Count Field Value";
    private static final String DESCRIPTION = "Find the matches within a specific metadata field value and return the matches count";

    private static final String SUBCATEGORY = "Custom";

    private final PluginConfig config;
    private final PluginCallback callback;

    public static final ConfigProperty.Builder SEARCH_FIELD_NAME_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.countMetadataFieldValueMatch.fieldName")
            .setValue("")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Field Name")
            .setUserVisibleDescription("Enter which metadata field name to be used for matching.");
 
    public static final ConfigProperty.Builder REGEX_MATCH_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.countMetadataFieldValueMatch.regexMatch")
            .setValue("")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Regular Expression\\Text\\Delimiter")
            .setUserVisibleDescription("Enter the matching regular expression, text, or delimeter to be searched. For example, search for \"John\" or search for \";\" or search for email \"([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\\\\.[a-zA-Z0-9_-]+)\"");
    
    public static final ConfigProperty.Builder COUNT_FIELD_NAME_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.countMetadataFieldValueMatch.countResult")
            .setValue("FieldValueMatchCountResult")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Count Field Name")
            .setUserVisibleDescription("Enter the new metadata field name for the count result.");
    
    // Configuration Group 1
    // "Default"-type property groups allow each property to be displayed
    // as its own control in the UI
    private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

    static {
        groupProperties.add(SEARCH_FIELD_NAME_PROPERTY);
        groupProperties.add(REGEX_MATCH_PROPERTY);
        groupProperties.add(COUNT_FIELD_NAME_PROPERTY);
    }

    public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_ONE = new ConfigPropertyGroup.Builder(
            "Options", null)
                    .setType(PropertyGroupType.DEFAULT)
                    .setConfigProperties(groupProperties);

    // Default config
    // This default configuration will be returned to callers of getDefaultConfig().
    public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
            .addGroup(PROPERTY_GROUP_ONE)
            .build();

    // Default constructor for new unconfigured plugin instances (can obtain default config)
    public CountMetadataFieldValueMatchStage() {
        config = null;
        callback = null;
    }

    // Constructor for configured plugin instances to be used in workflows
    private CountMetadataFieldValueMatchStage(PluginConfig config, PluginCallback callback)
            throws ConfigurationException {
        this.config = config;
        this.callback = callback;
        validateConfig(config);
    }

    @Override
    public void validateConfig(PluginConfig config) throws ConfigurationException {
        // This method is used to ensure that the specified configuration is valid for this
        // connector, i.e. that required properties are present in the configuration and no invalid
        // values are set.

        // This method handles checking for non-empty and existing required properties.
        // It should typically always be called here.
        Config.validateConfig(getDefaultConfig(), config);

        // Individual property values may be read and their values checked for validity
        if (config.getPropertyValue(SEARCH_FIELD_NAME_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing Search Field Name");
        }
        if (config.getPropertyValue(REGEX_MATCH_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing Regular Expression\\Text\\Delimiter");
        }
        if (config.getPropertyValue(COUNT_FIELD_NAME_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing Output Count Field Name");
        }
    }

    @Override
    public CountMetadataFieldValueMatchStage build(PluginConfig config, PluginCallback callback)
            throws ConfigurationException {
        // This method is used as a factory to create a configured instance of this stage
        return new CountMetadataFieldValueMatchStage(config, callback);
    }

    @Override
    public String getHost() {
        // If this plugin utilizes SSL to talk to remote servers, the hostname of the server being
        // connected to should be returned here. Plugins which do not use SSL can safely return
        // null.
        return null;
    }

    @Override
    public Integer getPort() {
        // If this plugin utilizes SSL to talk to remote servers, the port of the server being
        // connected to should be returned here. Plugins which do not use SSL can safely return
        // null.
        return null;
    }

    @Override
    public PluginSession startSession() throws ConfigurationException,
            PluginOperationFailedException {
        // If this plugin maintains state to communicate with remote servers, a plugin defined
        // PluginSession should be returned here.
        // Plugins which do not use SSL can safely return PluginSession.NOOP_INSTANCE.
        return PluginSession.NOOP_INSTANCE;
    }

    @Override
    public String getName() {
        // The fully qualified class name of this plugin
        return PLUGIN_NAME;
    }

    @Override
    public String getDisplayName() {
        // The name of this plugin to display to end users
        return NAME;
    }

    @Override
    public String getDescription() {
        // A user-visible description string describing this plugin
        return DESCRIPTION;
    }

    @Override
    public String getLongDescription() {
        // A user-visible local description string used to document the behavior of this plugin
        return "";
    }

    @Override
    public PluginConfig getDefaultConfig() {
        // This method is used to specify a default plugin configuration.
        // Configuration properties are defined here, including their type, how they are
        // presented, and whether or not they require valid user input before the connector
        // can be used.
        // This default configuration is also what will appear in the UI when a new instance of your
        // plugin is created.
        return DEFAULT_CONFIG;
    }

    @Override
    public Iterator<Document> process(PluginSession session, Document inputDocument) {
        // This method allows the stage to evaluate and/or transform the input document presented.
        // Stages may evaluate existing metadata in the document, read and process content
        // and/or metadata streams, add additional streams, and add or transform existing metadata.
        final DocumentBuilder docBuilder = callback.documentBuilder().copy(inputDocument);

        // Read the config text from each of these properties
        String searchFieldName = config.getProperty(SEARCH_FIELD_NAME_PROPERTY.getName()).getValue();
        String regExString = config.getProperty(REGEX_MATCH_PROPERTY.getName()).getValue();
        String countFieldName = config.getProperty(COUNT_FIELD_NAME_PROPERTY.getName()).getValue();
        
        // Get the field for search string
        DocumentFieldValue<?> searchFieldValue = docBuilder.getMetadataValue(searchFieldName);  
        String searchFieldValueString = String.valueOf(searchFieldValue);
        
        // Invoke countMatches to find the match and return the count
        int matchResultCount = CountMatches.countMatches(searchFieldValueString, regExString);
        String matchResult = String.valueOf(matchResultCount);
        
        //
        // HCI Document Metadata
        //
        docBuilder.addMetadata(countFieldName, StringDocumentFieldValue.builder().setString(matchResult).build());

        //
        // HCI Document Streams
        //
        return new StreamingDocumentIterator() {
            boolean sentAllDocuments = false;

            // Computes the next Document to return to the processing pipeline.
            // When there are no more documents to return, returns endOfDocuments().
            // This method can be used to consume an Iterator and build Documents
            // for each individual element as this streaming Iterator is consumed.
            @Override
            protected Document getNextDocument() {
                if (!sentAllDocuments) {
                    sentAllDocuments = true;
                    return docBuilder.build();
                }
                return endOfDocuments();
            }
        };
    }

    @Override
    public StagePluginCategory getCategory() {
        return StagePluginCategory.OTHER;
    }

    @Override
    public String getSubCategory() {
        return SUBCATEGORY;
    }
    
}
