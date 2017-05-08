package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.logging.HdsLogger;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DoubleDocumentFieldValue;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;

public class CustomAnnotationStagePlugin implements StagePlugin {

	public static final Logger logger = HdsLogger.getLogger();
	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.customAnnotationStagePlugin";
	private static final String PLUGIN_DISPLAY_NAME = "Custom Construct Annotation";
	private static final String PLUGIN_DESCRIPTION = "Constructs a custom metadata stream for an action stage writeAnnotation";
	private static final String OPTIONS_GROUP_NAME = "Options";
	private static final String SEARCH_GROUP_NAME = "Search Text";
	private static final String DEFAULT_INPUT_STREAM_NAME = "HCI_text";

	private final PluginConfig config;
	private final PluginCallback callback;

	private String inputFieldName = "HDI_File_Path";
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

	private CustomAnnotationStagePlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);
		this.setInputFieldName(
				this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(), "HDI_File_Path"));
		this.config.getPropertyValueOrDefault(PROPERTY_OUTPUT_FIELD_NAME.getName(), "HDI_File_Name");

	}

	public CustomAnnotationStagePlugin() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CustomAnnotationStagePlugin build(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		return new CustomAnnotationStagePlugin(config, callback);
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
			String cm = "<Root>WroteAnnotationDefault</Root>";
			InputStream stream = new ByteArrayInputStream(cm.getBytes(StandardCharsets.UTF_8));
			docBuilder.appendStream("HCP_customMetadata", stream);
			DoubleDocumentFieldValue df = (DoubleDocumentFieldValue) docBuilder.getMetadataValue("HCP_customMetadata");
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
