/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Vantara, 2019. All rights reserved.
 *
 * ========================================================================
 */

package com.hitachi.hci.plugins.stage.custom.UBSTransformation;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.DateDocumentFieldValue;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.hds.ensemble.sdk.model.DocumentFieldValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Date;

public class UBSCustomMapping implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.custom.UBSTransformation.UBSCustomMapping";
	private static final String NAME = "UBS Custom Mapping";
	private static final String DESCRIPTION = "UBS Custom Mapping which include mapping and tagging with default values.";

	private static final String LONG_DESCRIPTION = "UBS Custom Mapping."
			+ "\n\nThis stage is intended specifically for UBS Custom Mapping and Tagging specific details." + "\n"
			+ "\nUBS Mapping is used to mapping the existing metadata field values into the new metadata declared on the configuration field value."
			+ "\n"
			+ "\nUBS Tagging is used to create new tags that support single or multiple values. For multivalued field, use the comma as the separator between fields."
			+ "\n" + "\n";

	private static final String SUBCATEGORY = "Custom";

	private static final String UBS_Mapping_Group_Name = "UBS Mapping";
	private static final String UBS_Tagging_Group_Name = "UBS Tagging";
	private static final String Date_Format = "yyyy-MM-dd'T'HH:mm:ss.SSSSS";
	private static final String Default_Category_Value = "default category value";
	private static final String Default_Regulated_Entity_Value = "UBS Financial Services Inc.";

	private final PluginConfig config;
	private final PluginCallback callback;

	// UBS Mapping configuration group
	// Field name is the metadata in the document
	// Field value is the new metadata to be created with the value from the
	// original field name
	private static List<ConfigProperty.Builder> UBSMapping = new ArrayList<>();

	static {
		// Default properties in a single value or multivalues table may be included but
		// can
		// be deleted by the end user.
		UBSMapping.add(new ConfigProperty.Builder().setName("HCI_createdDateString").setValue("OS_File_Created_Date"));
		UBSMapping.add(new ConfigProperty.Builder().setName("HCI_displayName").setValue("OS_File_Name"));
		UBSMapping.add(new ConfigProperty.Builder().setName("HCI_path").setValue("OS_File_Location"));
		UBSMapping
				.add(new ConfigProperty.Builder().setName("HCI_modifiedDateString").setValue("OS_File_Modified_Date"));
	}

	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_MAPPING = new ConfigPropertyGroup.Builder(
			UBS_Mapping_Group_Name, null).setType(PropertyGroupType.KEY_VALUE_TABLE).setConfigProperties(UBSMapping);

	// Configuration UBS Tagging
	// A group may be set as a KEY_VALUE_TABLE. This overrides the type and
	// configuration of
	// any property contained in it and all properties become rows in a table.
	// This group type is used for variable-length tables with a two text entry
	// fields in each
	// row. Rows may be added and deleted as desired by the user.
	private static List<ConfigProperty.Builder> UBSTagging = new ArrayList<>();

	static {
		// Default properties in a single value or multi-values table may be included
		// but can
		// be deleted by the end user.
		UBSTagging.add(
				new ConfigProperty.Builder().setName("Category").setValue(Default_Category_Value).setRequired(true));
		UBSTagging.add(new ConfigProperty.Builder().setName("Sub_Category").setValue(""));
		UBSTagging.add(new ConfigProperty.Builder().setName("Record_Type").setValue(""));
		UBSTagging
				.add(new ConfigProperty.Builder().setName("Regulated_Entity").setValue(Default_Regulated_Entity_Value));
		UBSTagging.add(new ConfigProperty.Builder().setName("Regulatory_Citation").setValue(""));
		UBSTagging.add(new ConfigProperty.Builder().setName("UBS_Policy_Code").setValue(""));
	}

	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_TAGGING = new ConfigPropertyGroup.Builder(
			UBS_Tagging_Group_Name, null).setType(PropertyGroupType.KEY_VALUE_TABLE).setConfigProperties(UBSTagging);

	// Default config
	// This default configuration will be returned to callers of getDefaultConfig().
	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(PROPERTY_GROUP_MAPPING)
			.addGroup(PROPERTY_GROUP_TAGGING).build();

	// Default constructor for new unconfigured plugin instances (can obtain default
	// config)
	public UBSCustomMapping() {
		config = null;
		callback = null;
	}

	// Constructor for configured plugin instances to be used in workflows
	private UBSCustomMapping(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		validateConfig(config);
	}

	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		// This method is used to ensure that the specified configuration is valid for
		// this
		// connector, i.e. that required properties are present in the configuration and
		// no invalid
		// values are set.

		// This method handles checking for non-empty and existing required properties.
		// It should typically always be called here.
		Config.validateConfig(getDefaultConfig(), config);

		// Individual property values may be read and their values checked for validity

	}

	@Override
	public UBSCustomMapping build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		// This method is used as a factory to create a configured instance of this
		// stage
		return new UBSCustomMapping(config, callback);
	}

	@Override
	public String getHost() {
		// If this plugin utilizes SSL to talk to remote servers, the hostname of the
		// server being
		// connected to should be returned here. Plugins which do not use SSL can safely
		// return
		// null.
		return null;
	}

	@Override
	public Integer getPort() {
		// If this plugin utilizes SSL to talk to remote servers, the port of the server
		// being
		// connected to should be returned here. Plugins which do not use SSL can safely
		// return
		// null.
		return null;
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		// If this plugin maintains state to communicate with remote servers, a plugin
		// defined
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
		// A user-visible local description string used to document the behavior of this
		// plugin
		return LONG_DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		// This method is used to specify a default plugin configuration.
		// Configuration properties are defined here, including their type, how they are
		// presented, and whether or not they require valid user input before the
		// connector
		// can be used.
		// This default configuration is also what will appear in the UI when a new
		// instance of your
		// plugin is created.
		return DEFAULT_CONFIG;
	}

	@Override
	public Iterator<Document> process(PluginSession session, Document inputDocument) {

		final DocumentBuilder docBuilder = callback.documentBuilder().copy(inputDocument);

		// Get all the metadata names for the document
		Set<String> metadataNames = docBuilder.getMetadataNames();
		String metadataNamesString = metadataNames.toString();

		// Processing UBS Mapping
		String UBSMappingFieldName;
		String UBSMappingFieldValue;

		// int UBSMappingFieldCount = config.getGroup("UBS
		// Mapping").getProperties().size();
		List<ConfigProperty> UBSMappingConfigFieldNames = config.getGroup(UBS_Mapping_Group_Name).getProperties();

		for (int i = 0; i < UBSMappingConfigFieldNames.size(); i++) {
			// Get the field
			UBSMappingFieldName = String.valueOf(UBSMappingConfigFieldNames.get(i).getName());
			UBSMappingFieldValue = String.valueOf(UBSMappingConfigFieldNames.get(i).getValue());

			if (metadataNamesString.contains(UBSMappingFieldName)) {
				// Found the field name in the metadata names
				// Get the field value string
				DocumentFieldValue<?> metadataFieldValue = docBuilder.getMetadataValue(UBSMappingFieldName);
				String metadataFieldValueString = String.valueOf(metadataFieldValue);
				
				metadataFieldValueString = metadataFieldValueString.replaceAll("\\[", "").replaceAll("\\]", "");

				// Single field value
				if (UBSMappingFieldName.toLowerCase().contains("date")) {
					// Field name has a "date" sub-string
					SimpleDateFormat formatter = new SimpleDateFormat(Date_Format);
					Date dateValue;
					try {
						dateValue = formatter.parse(metadataFieldValueString);
						docBuilder.addMetadata(UBSMappingFieldValue,
								DateDocumentFieldValue.builder().setDate(dateValue).build());
					} catch (ParseException e) {
						// Unable to parse the date value, insert string value instead.
						docBuilder.addMetadata(UBSMappingFieldValue,
								StringDocumentFieldValue.builder().setString(metadataFieldValueString).build());
						e.printStackTrace();
					}
				} else {
					// Non date field
					docBuilder.addMetadata(UBSMappingFieldValue,
							StringDocumentFieldValue.builder().setString(metadataFieldValueString).build());
				}
			} else {
				// Metadata not found
			}
		}

		// Processing UBS Tagging
		String UBSTaggingFieldName = "";
		String UBSTaggingFieldValue = "";

		List<ConfigProperty> UBSTaggingConfigFieldNames = config.getGroup(UBS_Tagging_Group_Name).getProperties();

		for (int i = 0; i < UBSTaggingConfigFieldNames.size(); i++) {
			UBSTaggingFieldName = String.valueOf(UBSTaggingConfigFieldNames.get(i).getName());
			UBSTaggingFieldValue = String.valueOf(UBSTaggingConfigFieldNames.get(i).getValue());

			if (UBSTaggingFieldValue.contains(",") && UBSTaggingFieldValue.length() > 1) {
				// Multiple field values
				Set<String> tagColumns = new HashSet<>(Arrays.asList(UBSTaggingFieldValue.split(",")).stream()
						.map(String::trim).collect(Collectors.toList()));
				docBuilder.addMetadata(UBSTaggingFieldName,
						StringDocumentFieldValue.builder().setStrings(tagColumns).build());
			} else {
				// Single field value
				if (UBSTaggingFieldName.toLowerCase().contains("date")) {
					// Field name has a "date" sub-string
					UBSTaggingFieldValue = UBSTaggingFieldValue.replaceAll("\\[", "").replaceAll("\\]", "");
					SimpleDateFormat formatter = new SimpleDateFormat(Date_Format);
					Date dateValue;
					try {
						dateValue = formatter.parse(UBSTaggingFieldValue);
						docBuilder.addMetadata(UBSTaggingFieldName,
								DateDocumentFieldValue.builder().setDate(dateValue).build());
					} catch (ParseException e) {
						// Unable to parse the date value, insert string value instead.
						docBuilder.addMetadata(UBSTaggingFieldName,
								StringDocumentFieldValue.builder().setString(UBSTaggingFieldValue).build());
						e.printStackTrace();
					}
				} else {
					// Non date field
					docBuilder.addMetadata(UBSTaggingFieldName,
							StringDocumentFieldValue.builder().setString(UBSTaggingFieldValue).build());
				}
			}
		}

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
		// This method is used to determine which top level category the
		// stage plugin should be grouped in for UI display purposes.
		return StagePluginCategory.OTHER;
	}

	@Override
	public String getSubCategory() {
		// This method is used to determine which sub-category the stage
		// plugin should be grouped in for UI display purposes.
		return SUBCATEGORY;
	}
}
