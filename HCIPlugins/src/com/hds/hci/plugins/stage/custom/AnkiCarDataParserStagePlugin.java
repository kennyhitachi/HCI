package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.logging.HdsLogger;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;


	public class AnkiCarDataParserStagePlugin implements StagePlugin {

		public static final Logger logger = HdsLogger.getLogger();
		private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.ankiCarDataParserStagePlugin";
		private static final String PLUGIN_DISPLAY_NAME = "Anki Car Data Parser";
		private static final String PLUGIN_DESCRIPTION = "Parses data received from an anki car";
		private static final String OPTIONS_GROUP_NAME = "Options";
		//private static final String SEARCH_GROUP_NAME = "Search Text";
		private static final String DEFAULT_INPUT_STREAM_NAME = "HCI_text";

		private final PluginConfig config;
		private final PluginCallback callback;

		private String inputFieldName = "";
		// Text field
		public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
				.setName("inputFieldName").setValue("").setRequired(true)
				.setUserVisibleName("").setUserVisibleDescription(
						"");

		// Text field
		public static final ConfigProperty.Builder PROPERTY_OUTPUT_FIELD_NAME = new ConfigProperty.Builder()
				.setName("outputFieldName").setValue("").setRequired(true)
				.setUserVisibleName("").setUserVisibleDescription(
						"");
		private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

		static {
			groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
			groupProperties.add(PROPERTY_OUTPUT_FIELD_NAME);
		}
		// Property Group
		private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
				.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

		private AnkiCarDataParserStagePlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
			this.config = config;
			this.callback = callback;
			this.validateConfig(this.config);
			this.setInputFieldName(
					this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(), "HDI_File_Path"));
			this.config.getPropertyValueOrDefault(PROPERTY_OUTPUT_FIELD_NAME.getName(), "HDI_File_Name");

		}

		public AnkiCarDataParserStagePlugin() {
			this.config = this.getDefaultConfig();
			this.callback = null;
		}

		public AnkiCarDataParserStagePlugin build(PluginConfig config, PluginCallback callback)
				throws ConfigurationException {
			return new AnkiCarDataParserStagePlugin(config, callback);
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
            String time="",handle="",data="",line="";
            File file = new File("/opt/hci/output.csv");
			try {
				InputStream inputStream = this.callback.openNamedStream(document, "HCI_text");
				
				if (inputStream != null) {
					InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");
					
					List<String> lineStrings = IOUtils.readLines(reader);
					
					for (int i = 0; i < lineStrings.size(); i++) {
					
				if (line.toString().startsWith("> ACL Data RX:")) {
				        String[] linesplits = line.split(" ");
				        time = linesplits[linesplits.length-1];
				}
				if (line.startsWith("Handle:")) {
					    String[] handleSplits = line.split(" ");
					     handle = handleSplits[1];
				}
				if (line.startsWith("Data:")) {
					    String[] dataSplits = line.split(" ");
					    data = dataSplits[1];
				}
				
				
				//String header = "time,handle,data";
				String newLine = time+","+handle+","+data+"\n";
				
				if (newLine.length() > 10) {
				// if file doesnt exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}

				// true = append file
				FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
				@SuppressWarnings("resource")
				BufferedWriter bw = new BufferedWriter(fw);

				bw.write(newLine);
				}
					}
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

		public String getInputFieldName() {
			return inputFieldName;
		}

		public void setInputFieldName(String inputFieldName) {
			this.inputFieldName = inputFieldName;
		}
	}

