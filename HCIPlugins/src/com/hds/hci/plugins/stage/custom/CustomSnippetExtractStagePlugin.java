package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.logging.HdsLogger;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.WordUtils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

public class CustomSnippetExtractStagePlugin implements StagePlugin {

	public static final Logger logger = HdsLogger.getLogger();
	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.customSnippetExtractStage";
	private static final String PLUGIN_DISPLAY_NAME = "Custom Snippet Extraction";
	private static final String PLUGIN_DESCRIPTION = "Extracts a snippet text field from a text stream and formats the snippet based on the selected configuration.";
	private static final String OPTIONS_GROUP_NAME = "Options";
	private static final String DEFAULT_INPUT_STREAM_NAME = "HCI_text";
	private static final String DEFAULT_OUTPUT_FIELD_NAME = "HCI_custom_snippet";
	private static final String STD_SNIPPET_FIELD_NAME = "HCI_snippet";
	private static final String UPSHIFT = "UpShift";
	private static final String DOWNSHIFT = "DownShift";
	private static final String CAMEL = "CamelCase";
	private static final int DEFAULT_MAX_SNIPPET_LENGTH = 12000;
	private static final int DEFAULT_SNIPPET_START_OFFSET = 0;

	// Adding all possible delimiters to make camelCaseing robust.
	private static final char[] DELIMITERS = { '/', ' ', '=', ',', '.', '{', '}', '\\', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', '0' };

	private final PluginConfig config;
	private final PluginCallback callback;
	private int maxSnippetLength = 12000;
	private int startOffset = 0;

	private String streamName = "HCI_text";
	private String outputFieldName = "HCI_custom_snippet";
	private String rawSnippetField = "HCI_snippet";

	// Radio options list
	private static List<String> radioOptions = new ArrayList<>();

	static {
		radioOptions.add(UPSHIFT);
		radioOptions.add(DOWNSHIFT);
		radioOptions.add(CAMEL);
	}

	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName("inputStreamName").setValue("HCI_text").setRequired(true).setUserVisibleName("Text Input Stream")
			.setUserVisibleDescription("Name of the source stream to read text from (default: HCI_text)");

	// Text Field
	public static final ConfigProperty.Builder PROPERTY_OUTPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName("outputFieldName").setValue("HCI_custom_snippet").setRequired(true)
			.setUserVisibleName("Custom Snippet Output Field").setUserVisibleDescription(
					"Name of the target field to write snippet text to (default: HCI_custom_snippet)");

	// Text Field
	public static final ConfigProperty.Builder PROPERTY_SNIPPET_START_OFFSET = new ConfigProperty.Builder()
			.setName("snippetStartOffset").setValue(Integer.toString(0)).setRequired(false)
			.setUserVisibleName("Snippet Start Offset")
			.setUserVisibleDescription("The offset into the stream to extract characters from (default: 0)");

	// Text Field
	public static final ConfigProperty.Builder PROPERTY_MAX_SNIPPET_LENGTH = new ConfigProperty.Builder()
			.setName("snippetLength").setValue("12000").setRequired(false)
			.setUserVisibleName("Maximum Snippet Character Length").setUserVisibleDescription(
					"The number of characters to extract into the HCI_custom_snippet field (default: 12000)");

	// Radio selection
	public static final ConfigProperty.Builder PROPERTY_UPSHIFT_DOWNSHIFT = new ConfigProperty.Builder()
			.setName("shiftFlag").setType(PropertyType.RADIO).setOptions(radioOptions).setValue(radioOptions.get(1))
			.setRequired(true).setUserVisibleName("Snippet Format").setUserVisibleDescription(
					"Select one property to format custom snippet to uppercase , lowercase or camelCase");

	// Check Box
	public static final ConfigProperty.Builder PROPERTY_CHECK_RAW_SNIPPET = new ConfigProperty.Builder()
			.setName("rawSnippet")
			// A CHECKBOX property will display a checkbox-like control for a
			// boolean value.
			.setType(PropertyType.CHECKBOX).setValue("false").setRequired(true)
			.setUserVisibleName("Capture Original Snippet")
			.setUserVisibleDescription("Check this option to capture the original raw unformatted snippet");

	// Text Field
	public static final ConfigProperty.Builder PROPERTY_RAW_SNIPPET = new ConfigProperty.Builder()
			.setName("rawSnippetFeild").setValue("HCI_snippet").setRequired(true)
			.setUserVisibleName("Original Snippet Output Field")
			.setUserVisibleDescription(
					"Name of the target field to write the original snippet text to (default: HCI_snippet)")
			.setPropertyVisibilityTrigger("rawSnippet", "true");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_STREAM_NAME);
		groupProperties.add(PROPERTY_OUTPUT_FIELD_NAME);
		groupProperties.add(PROPERTY_SNIPPET_START_OFFSET);
		groupProperties.add(PROPERTY_MAX_SNIPPET_LENGTH);
		groupProperties.add(PROPERTY_UPSHIFT_DOWNSHIFT);
		groupProperties.add(PROPERTY_CHECK_RAW_SNIPPET);
		groupProperties.add(PROPERTY_RAW_SNIPPET);
	}

	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private CustomSnippetExtractStagePlugin(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);
		this.streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(), "HCI_text");
		this.outputFieldName = this.config.getPropertyValueOrDefault(PROPERTY_OUTPUT_FIELD_NAME.getName(),
				"HCI_custom_snippet");
		this.rawSnippetField = this.config.getPropertyValueOrDefault(PROPERTY_RAW_SNIPPET.getName(), "HCI_snippet");
		this.maxSnippetLength = Integer.valueOf(this.config.getPropertyValueOrDefault(
				PROPERTY_MAX_SNIPPET_LENGTH.getName(), Integer.toString(this.maxSnippetLength)));
		this.startOffset = Integer.valueOf(this.config.getPropertyValueOrDefault(
				PROPERTY_SNIPPET_START_OFFSET.getName(), Integer.toString(this.startOffset)));
	}

	public CustomSnippetExtractStagePlugin() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CustomSnippetExtractStagePlugin build(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		return new CustomSnippetExtractStagePlugin(config, callback);
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
		String val;
		int value;
		try {
			val = config.getPropertyValue(PROPERTY_MAX_SNIPPET_LENGTH.getName());
			if (val != null) {
				Integer.valueOf(val);
			}
		} catch (Exception e) {
			throw new ConfigurationException(
					"Invalid integer value for property: " + PROPERTY_MAX_SNIPPET_LENGTH.getName());
		}
		try {
			val = config.getPropertyValue(PROPERTY_SNIPPET_START_OFFSET.getName());
			if (val != null && (value = Integer.valueOf(val).intValue()) < 0) {
				throw new ConfigurationException(
						"Offset cannot be negative for property: " + PROPERTY_SNIPPET_START_OFFSET.getName());
			}
		} catch (Exception e) {
			throw new ConfigurationException(
					"Invalid integer value for property: " + PROPERTY_SNIPPET_START_OFFSET.getName());
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
		
		if (this.maxSnippetLength <= 0 || !document.getStreamNames().contains(this.streamName)) {
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

		String shiftFlag = config.getPropertyValue(PROPERTY_UPSHIFT_DOWNSHIFT.getName());
		Boolean enableRawSnippet = Boolean.valueOf(
				this.config.getPropertyValueOrDefault(PROPERTY_CHECK_RAW_SNIPPET.getName(), Boolean.FALSE.toString()));

		try {
			InputStream inputStream = this.callback.openNamedStream(document, this.streamName);
			Throwable throwable = null;
			try {

				if (inputStream != null) {
					InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");
					if (this.startOffset > 0) {
						reader.skip(this.startOffset);
					}
					char[] buffer = new char[this.maxSnippetLength];
					int charsRead = IOUtils.read(reader, buffer);
					String snippet = String.copyValueOf(buffer, 0, charsRead);

					// check if raw snippet is requested
					if (enableRawSnippet) {
						docBuilder.setMetadata(this.rawSnippetField,
								StringDocumentFieldValue.builder().setString(snippet).build());
					}

					// Format snippet according to the selected configuration
					if (UPSHIFT.equalsIgnoreCase(shiftFlag)) {
						snippet = snippet.toUpperCase();
					} else if (CAMEL.equalsIgnoreCase(shiftFlag)) {
						snippet = WordUtils.capitalizeFully(snippet, DELIMITERS);
					} else {
						snippet = snippet.toLowerCase();
					}

					docBuilder.setMetadata(this.outputFieldName,
							StringDocumentFieldValue.builder().setString(snippet).build());
				}
			} catch (Throwable reader) {
				throwable = reader;
				throw reader;
			} finally {
				if (inputStream != null) {
					if (throwable != null) {
						try {
							inputStream.close();
						} catch (Throwable reader) {
							throwable.addSuppressed(reader);
						}
					} else {
						inputStream.close();
					}
				}
			}
		} catch (Exception ex) {
			logger.warn("Failed to open snippet text input stream", (Throwable) ex);
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

	public static String getDefaultOutputFieldName() {
		return DEFAULT_OUTPUT_FIELD_NAME;
	}

	public static int getDefaultMaxSnippetLength() {
		return DEFAULT_MAX_SNIPPET_LENGTH;
	}

	public static String getUpshift() {
		return UPSHIFT;
	}

	public static String getDownshift() {
		return DOWNSHIFT;
	}

	public static String getStdSnippetFieldName() {
		return STD_SNIPPET_FIELD_NAME;
	}

	public static String getCamel() {
		return CAMEL;
	}

	public String getRawSnippetField() {
		return rawSnippetField;
	}

	public void setRawSnippetField(String rawSnippetField) {
		this.rawSnippetField = rawSnippetField;
	}

	public static int getDefaultSnippetStartOffset() {
		return DEFAULT_SNIPPET_START_OFFSET;
	}
}
