package com.hds.hci.plugins.stage.custom;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;

import com.hds.ensemble.logging.HdsLogger;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
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

public class CustomHDIFileNameExtractor implements StagePlugin {

	public static final Logger logger = HdsLogger.getLogger();
	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.customHDIFileNameExtractor";
	private static final String PLUGIN_DISPLAY_NAME = "Custom HDI FileName Extractor";
	private static final String PLUGIN_DESCRIPTION = "Extracts the HDI FileName from the URL decoded filePath.";
	private static final String OPTIONS_GROUP_NAME = "Options";
	private static final String SEARCH_GROUP_NAME = "Search Text";
	private static final String DEFAULT_INPUT_STREAM_NAME = "HCI_text";

	private final PluginConfig config;
	private final PluginCallback callback;

	private String inputFieldName = "HDI_File_Path";
	private String outputFieldName = "HDI_File_Name";

	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName("inputFieldName").setValue("HDI_File_Path").setRequired(true)
			.setUserVisibleName("HDI File Path Field").setUserVisibleDescription(
					"Name of the Field that contains the decoded file path (default: HDI_File_Path)");

	// Text field
	public static final ConfigProperty.Builder PROPERTY_OUTPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName("outputFieldName").setValue("HDI_File_Name").setRequired(true)
			.setUserVisibleName("HDI File Name Field").setUserVisibleDescription(
					"Name of the Field that should contain the HDI file name (default: HDI_File_Name)");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
		groupProperties.add(PROPERTY_OUTPUT_FIELD_NAME);
	}

	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private CustomHDIFileNameExtractor(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);
		this.setInputFieldName(
				this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(), "HDI_File_Path"));
		this.outputFieldName = this.config.getPropertyValueOrDefault(PROPERTY_OUTPUT_FIELD_NAME.getName(),
				"HDI_File_Name");

	}

	public CustomHDIFileNameExtractor() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CustomHDIFileNameExtractor build(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		return new CustomHDIFileNameExtractor(config, callback);
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
			throw new ConfigurationException("No configuration for HDI File Name Extraction Stage");
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
		DocumentBuilder docBuilder;

		docBuilder = this.callback.documentBuilder().copy(document);

		try {
			String inputFieldVal = docBuilder.getStringMetadataValue(this.getInputFieldName().trim());
			String hdiFileName = inputFieldVal.substring(inputFieldVal.lastIndexOf("/") + 1, inputFieldVal.length());
			if (hdiFileName.indexOf(".") > 0) {
				docBuilder.addMetadata(this.outputFieldName,
						StringDocumentFieldValue.builder().setString(hdiFileName).build());
			}
		} catch (Exception ex) {
			logger.warn("Failed to Extract HDI File Name", (Throwable) ex);
			throw new PluginOperationFailedException(ex.getMessage(), (Throwable) ex);
		}
		//return Iterators.singletonIterator(docBuilder.build());
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

	public StagePluginCategory getCategory() {
		return StagePluginCategory.OTHER;
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

	public static String getOptionsGroupName() {
		return OPTIONS_GROUP_NAME;
	}

	public static String getDefaultInputStreamName() {
		return DEFAULT_INPUT_STREAM_NAME;
	}

	public static String getSearchGroupName() {
		return SEARCH_GROUP_NAME;
	}

	public String getInputFieldName() {
		return inputFieldName;
	}

	public void setInputFieldName(String inputFieldName) {
		this.inputFieldName = inputFieldName;
	}
}
