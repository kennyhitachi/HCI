package com.hitachi.hci.plugins.stage.custom;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

public class CurrentDateTimeStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.custom.currentDateTimeStage";
	private static final String PLUGIN_DISPLAY_NAME = "Current Date Time Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage adds a new metadata Field called currentDateTime to the document.\n ";
	private final PluginConfig config;
	private final PluginCallback callback;

	private static final String CURRENT_DATE_TIME_FIELD = "currentDateTime";
	private static final String TIME_ZONE = "GMT";
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	private static String inputFieldName = "Field";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue(CURRENT_DATE_TIME_FIELD).setRequired(true)
			.setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the field which holds the current date time value");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private CurrentDateTimeStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public CurrentDateTimeStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CurrentDateTimeStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new CurrentDateTimeStage(config, callback);
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
			throw new ConfigurationException("No configuration for Encryption File Detection Stage");
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

		String fieldName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				CURRENT_DATE_TIME_FIELD);

		sdf.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
		docBuilder.setMetadata(fieldName, DateDocumentFieldValue.builder().setDate(new Date()).build());

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
