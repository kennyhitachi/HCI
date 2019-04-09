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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CompareTwoMetadataFieldValuesStage implements StagePlugin {

    private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.custom.compareTwoMetadataFieldValuesStage";
    private static final String NAME = "Compare Field Values";
    private static final String DESCRIPTION = "Comparing two metadata field values using Java compareToIgnoreCase function. If equal, then returns 0, else non-zero. For other comparision, refer to Java compareToIgnoreCase";

    private static final String SUBCATEGORY = "Custom";

    private final PluginConfig config;
    private final PluginCallback callback;

    public static final ConfigProperty.Builder FIELD_NAME_1_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.compareTwoMetadataFieldValuesStage.fieldName1")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Field Name 1")
            .setUserVisibleDescription("Enter the existing 1st metadata field name (left side) to compare.");
 
    public static final ConfigProperty.Builder FIELD_NAME_2_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.compareTwoMetadataFieldValuesStage.fieldName2")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Field Name 2")
            .setUserVisibleDescription("Enter the existing 2nd metadata field name (right side) to compare.");
    
    public static final ConfigProperty.Builder COMPARE_RESULT_RAW_PROPERTY = new ConfigProperty.Builder()
            .setName("hci.plugins.stage.custom.compareTwoMetadataFieldValuesStage.compareResultRaw")
            .setValue("CompareResultRaw")
            .setType(PropertyType.TEXT)
            .setRequired(true)
            .setUserVisibleName("Compare Result Raw Field Name")
            .setUserVisibleDescription("Enter the new metadata field name for the comparison result raw.");
    
    // Configuration Group 1
    // "Default"-type property groups allow each property to be displayed
    // as its own control in the UI
    private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

    static {
        groupProperties.add(FIELD_NAME_1_PROPERTY);
        groupProperties.add(FIELD_NAME_2_PROPERTY);
        groupProperties.add(COMPARE_RESULT_RAW_PROPERTY);
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
    public CompareTwoMetadataFieldValuesStage() {
        config = null;
        callback = null;
    }

    // Constructor for configured plugin instances to be used in workflows
    private CompareTwoMetadataFieldValuesStage(PluginConfig config, PluginCallback callback)
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
        if (config.getPropertyValue(FIELD_NAME_1_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing 1st Compare Field Name");
        }
        if (config.getPropertyValue(FIELD_NAME_2_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing 2nd Compare Field Name");
        }
        if (config.getPropertyValue(COMPARE_RESULT_RAW_PROPERTY.getName()) == null) {
            throw new ConfigurationException("Missing Output Compare Result Raw Field Name");
        }
    }

    @Override
    public CompareTwoMetadataFieldValuesStage build(PluginConfig config, PluginCallback callback)
            throws ConfigurationException {
        // This method is used as a factory to create a configured instance of this stage
        return new CompareTwoMetadataFieldValuesStage(config, callback);
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

        // In this example, a single metadata field is added to the input Document if the checkbox
        // property created above is set to "true". Its value will be the text entered in the text
        // property above.
        final DocumentBuilder docBuilder = callback.documentBuilder().copy(inputDocument);

        // Read the config text from each of these properties
        String fieldName1 = config.getProperty(FIELD_NAME_1_PROPERTY.getName()).getValue();
        String fieldName2 = config.getProperty(FIELD_NAME_2_PROPERTY.getName()).getValue();
        String compareResultRawFieldName = config.getProperty(COMPARE_RESULT_RAW_PROPERTY.getName()).getValue();
        
        // Get the field names string
        DocumentFieldValue<?> fieldName1Value = docBuilder.getMetadataValue(fieldName1);  
        String fieldName1ValueString = fieldName1Value.toString();

        DocumentFieldValue<?> fieldName2Value = docBuilder.getMetadataValue(fieldName2);  
        String fieldName2ValueString = fieldName2Value.toString();
        
        // Invoke compareResultRaw to compare and return the compareTo result
        int compareResultRaw = compareStringsRaw(fieldName1ValueString, fieldName2ValueString);
        String compareResultRawString = String.valueOf(compareResultRaw);
 
        //
        // HCI Document Metadata
        //
        docBuilder.addMetadata(compareResultRawFieldName, StringDocumentFieldValue.builder().setString(compareResultRawString).build());


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
    
    // Custom function to use the compareToIgnoreCase to compare two values with trimming leading and trailing spaces
    static int compareStringsRaw (String string1, String string2) {
    	string1 = string1.trim();
    	string2 = string2.trim();
    	return string1.compareToIgnoreCase(string2);
    }
    
}
