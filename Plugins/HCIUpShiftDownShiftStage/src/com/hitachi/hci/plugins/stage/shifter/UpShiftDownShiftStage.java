/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2018. All rights reserved.
 *
 * ========================================================================
 */

package com.hitachi.hci.plugins.stage.shifter;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
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

public class UpShiftDownShiftStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.shifter.upShiftDownShiftStage";
	private static final String PLUGIN_DISPLAY_NAME = "Shifter Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage either upshifts or downshifts a metadata field value based on format selected by the user.";
	private static final String UPSHIFT = "UpShift";
	private static final String DOWNSHIFT = "DownShift";
	private final PluginConfig config;
	private final PluginCallback callback;

	private static String inputFieldName = "MetaData Field";

	// Radio options list
	private static List<String> radioOptions = new ArrayList<>();

	static {
		radioOptions.add(UPSHIFT);
		radioOptions.add(DOWNSHIFT);
	}
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("").setRequired(true)
			.setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the metadata field whose value needs to be formatted");

	// Override existing
	public static final ConfigProperty.Builder OVERRIDE_FIELD_VALUE = new ConfigProperty.Builder()
			.setName("hci.override.value").setType(PropertyType.CHECKBOX).setValue("true").setRequired(false)
			.setUserVisibleName("Override Value")
			.setUserVisibleDescription("Check this option to override existing metadata field value.");
	// Radio selection
	public static final ConfigProperty.Builder PROPERTY_UPSHIFT_DOWNSHIFT = new ConfigProperty.Builder()
			.setName("shiftFlag").setType(PropertyType.RADIO).setOptions(radioOptions).setValue(radioOptions.get(1))
			.setRequired(true).setUserVisibleName("MetaData Field Format")
			.setUserVisibleDescription("Select one property to format metadata value to uppercase or lowercase");
	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_UPSHIFT_DOWNSHIFT);
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
		groupProperties.add(OVERRIDE_FIELD_VALUE);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private UpShiftDownShiftStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public UpShiftDownShiftStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public UpShiftDownShiftStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new UpShiftDownShiftStage(config, callback);
	}

	public String getHost() {
		return null;
	}

	public Integer getPort() {
		return null;
	}

	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		return PluginSession.NOOP_INSTANCE;
	}

	public void validateConfig(PluginConfig config) throws ConfigurationException {
		Config.validateConfig((Config) this.getDefaultConfig(), (Config) config);
		if (config == null) {
			throw new ConfigurationException("No configuration for Shifter Stage");
		}
	}

	public String getDisplayName() {
		return PLUGIN_DISPLAY_NAME;
	}

	public String getName() {
		return PLUGIN_NAME;
	}

	public String getDescription() {
		return PLUGIN_DESCRIPTION;
	}

	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	public Iterator<Document> process(PluginSession session, Document document)
			throws ConfigurationException, PluginOperationFailedException {
		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(document);

		Boolean overwrite = Boolean
				.valueOf(this.config.getPropertyValueOrDefault(OVERRIDE_FIELD_VALUE.getName(), Boolean.FALSE.toString()));

		String shiftFlag = this.config.getPropertyValue(PROPERTY_UPSHIFT_DOWNSHIFT.getName());

		String fieldName = this.config.getPropertyValue(PROPERTY_INPUT_FIELD_NAME.getName());

		String metadataValue = docBuilder.getStringMetadataValue(fieldName);
		StringBuilder formattedValue = new StringBuilder();

		if (UPSHIFT.equalsIgnoreCase(shiftFlag)) {
			formattedValue.append(metadataValue.toUpperCase());
		} else {
			formattedValue.append(metadataValue.toLowerCase());
		}

		if (overwrite) {
				docBuilder.setMetadata(fieldName, StringDocumentFieldValue.builder().setString(formattedValue.toString()).build());
		} else {
				docBuilder.addMetadata(fieldName, StringDocumentFieldValue.builder().setString(formattedValue.toString()).build());
		}

		// return Iterators.singletonIterator(docBuilder.build());
		return new StreamingDocumentIterator() {
			boolean sentAllDocuments = false;

			// Computes the next Document to return to the processing pipeline.
			// When there are no more documents to return, returns
			// endOfDocuments().
			// This method can be used to consume an Iterator and build
			// Documents
			// for each individual element as this streaming Iterator is
			// consumed.
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

	public StagePluginCategory getCategory() {
		return StagePluginCategory.ENRICH;
	}

	public String getSubCategory() {
		return "Custom";
	}

	public static String getPluginName() {
		return PLUGIN_NAME;
	}

	public static String getPluginDisplayName() {
		return PLUGIN_DISPLAY_NAME;
	}

	public static String getPluginDescription() {
		return PLUGIN_DESCRIPTION;
	}

}
