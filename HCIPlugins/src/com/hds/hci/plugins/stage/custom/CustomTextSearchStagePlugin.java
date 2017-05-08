package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.logging.HdsLogger;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;

public class CustomTextSearchStagePlugin implements StagePlugin {

	public static final Logger logger = HdsLogger.getLogger();
	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.customTextSearchStage";
	private static final String PLUGIN_DISPLAY_NAME = "Custom Text Search";
	private static final String PLUGIN_DESCRIPTION = "Searches a given text in a document and returns if the text exists or not.";
	private static final String OPTIONS_GROUP_NAME = "Options";
	private static final String SEARCH_GROUP_NAME = "Search Text";
	private static final String DEFAULT_INPUT_STREAM_NAME = "HCI_text";

	private final PluginConfig config;
	private final PluginCallback callback;

	private String streamName = "HCI_text";

	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName("inputStreamName").setValue("HCI_text").setRequired(true).setUserVisibleName("Text Input Stream")
			.setUserVisibleDescription("Name of the source stream to read text from (default: HCI_text)");

	private static List<ConfigProperty.Builder> searchProperties = new ArrayList<>();

	private static List<String> optionsList = new ArrayList<>();

	static {
		optionsList.add("Search Phrases");
	}

	private static List<ConfigProperty.Builder> optionProperties = new ArrayList<>();

	static {
		optionProperties.add(PROPERTY_INPUT_STREAM_NAME);
	}

	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(optionProperties))
			.addGroup(new ConfigPropertyGroup.Builder("Search Text", null).setType(PropertyGroupType.SINGLE_VALUE_TABLE)
					.setOptions(optionsList).setConfigProperties(searchProperties))
			.build();

	private CustomTextSearchStagePlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);
		this.streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(), "HCI_text");

	}

	public CustomTextSearchStagePlugin() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CustomTextSearchStagePlugin build(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		return new CustomTextSearchStagePlugin(config, callback);
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
			throw new ConfigurationException("No configuration for Text Search Stage");
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
		
		if (!document.getStreamNames().contains(this.streamName)) {
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

		ArrayList<String> wordList = new ArrayList<String>();

		for (ConfigPropertyGroup group : this.config.getGroups()) {
			if (group.getName().equals("Options"))
				continue;
			for (ConfigProperty property : group.getProperties()) {
				if ("Search Phrases".equals(property.getName()))
					continue;
				wordList.add(property.getName());
			}
		}

		try {
			InputStream inputStream = this.callback.openNamedStream(document, this.streamName);
			Throwable throwable = null;
			try {
				if (inputStream != null) {
					InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");

					BufferedReader br = new BufferedReader(reader);
					String line;
					while ((line = br.readLine()) != null) {
						line = line.toString().toLowerCase();
						for (Iterator<String> iterator = wordList.iterator(); iterator.hasNext();) {
							String word = iterator.next();

							if (!wordList.isEmpty() && line.contains(word.toLowerCase())) {
								docBuilder.addMetadata(word,
										StringDocumentFieldValue.builder().setString("true").build());
								iterator.remove();
							}

						}
						if (wordList.isEmpty()) {
							break;
						}
					}
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

	public static String getSearchGroupName() {
		return SEARCH_GROUP_NAME;
	}
}
