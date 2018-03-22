package com.hitachi.hci.plugins.stage.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.splunk.journal.EventData;
import com.splunk.journal.RawdataJournalReader;

public class SplunkArchiveBucketReader implements StagePlugin {
	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.stage.splunk.splunkArchiveBucketReader";
	private static final String PLUGIN_DISPLAY_NAME = "Splunk Archive Bucket Reader";
	private static final String PLUGIN_DESCRIPTION = "This stage reads a splunk archive journal file (journal.gz) and extracts Event Data.\n "
			+ "Metadata is extracted from each event and added to the document metadata";
	private final PluginConfig config;
	private final PluginCallback callback;

	private final Pattern whiteSpace = Pattern.compile("\\s");

	private static String inputFieldName = "Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("HCI_content").setRequired(true).setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the stream that points to the archived journal.gz file");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private SplunkArchiveBucketReader(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public SplunkArchiveBucketReader() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public SplunkArchiveBucketReader build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new SplunkArchiveBucketReader(config, callback);
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
			throw new ConfigurationException("No configuration for Splunk Archive Bucket Reader Stage");
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

		String filename = document.getStringMetadataValue(StandardFields.FILENAME);

		String extension = getExtension(filename);
		if (null != extension && !extension.equals("gz")) {
			throw new PluginOperationRuntimeException(new PluginOperationFailedException(
					"Failed to process document.Encountered an invalid splunk archive file " + filename));
		}

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);

		try {
			RawdataJournalReader journal = RawdataJournalReader.getReaderForGzipCompressedStream(inputStream);

			for (EventData eventData : journal) {
				String eventAsString = new String(eventData.getRawContents(), RawdataJournalReader.UTF_8);
				String metadataAsString = formatMetaInfo(eventData);
				// eventData.getFileOffset(), eventData.getEventTime(),
				// eventData.getSource(), eventData.getHost(),
				// eventData.getSourcetype(), eventAsString, metadataAsString);

				docBuilder.addMetadata("splunk_event",
						StringDocumentFieldValue.builder().setString(eventAsString).build());
				docBuilder.addMetadata("splunk_metadata",
						StringDocumentFieldValue.builder().setString(metadataAsString).build());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

	private String getExtension(String name) {
		int idx = name.lastIndexOf(".");
		if (idx == -1) {
			return null;
		}
		return name.substring(idx + 1);
	}

	private String formatMetaInfo(EventData eventData) {
		Map<String, Object> fields = eventData.getFields();
		StringBuilder builder = new StringBuilder();
		builder.append("splunk_indextime::").append(eventData.getIndexTime());
		for (Map.Entry<String, Object> entry : fields.entrySet()) {
			builder.append(" ");
			builder.append(this.quoteIfNeeded(entry.getKey()));
			builder.append("::");
			builder.append(this.quoteIfNeeded("" + entry.getValue()));
		}
		return builder.toString();
	}

	private String quoteIfNeeded(String str) {
		if (null == str) {
			return "\"\"";
		}
		if (this.whiteSpace.matcher(str).find()) {
			return "\"" + str + "\"";
		}
		return str;
	}

	public StagePluginCategory getCategory() {
		return StagePluginCategory.EXTRACT;
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
