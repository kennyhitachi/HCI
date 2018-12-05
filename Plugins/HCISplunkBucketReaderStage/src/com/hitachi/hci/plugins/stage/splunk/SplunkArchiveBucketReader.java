package com.hitachi.hci.plugins.stage.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.LongDocumentFieldValue;
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
	private final static Pattern apacheLog = Pattern.compile("(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)\\s?(\\S+)?\\s?(\\S+)?\" (\\d{3}|-) (\\d+|-)\\s?\"?([^\"]*)\"?\\s?\"?([^\"]*)?\"?$");
	
	private final static String IPAddress = "IPAddress";
	private final static String UserAgent = "Agent";
	private final static String UserName = "UserName";
	private final static String DateTimestamp = "DateTimeStamp";
	private final static String Action = "Action";
	private final static String FilePath = "FilePath";
	private final static String Agent = "Agent";
	private final static String StatusCode = "StatusCode";
	private final static String BytesTransferred = "BytesTransferred";
	private final static String Namespace = "Namespace";
	
	private final PluginConfig config;
	private final PluginCallback callback;

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
		this.callback.documentBuilder().copy(document);

		String filename = document.getStringMetadataValue(StandardFields.FILENAME);
		String extension = getExtension(filename);
		if (null != extension && !extension.equals("gz")) {
			throw new PluginOperationRuntimeException(new PluginOperationFailedException(
					"Failed to process document.Encountered an invalid splunk archive file " + filename));
		}

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);

		RawdataJournalReader journal = null;
		try {
			journal = RawdataJournalReader.getReaderForGzipCompressedStream(inputStream);
		} catch (IOException e) {
			throw new PluginOperationFailedException(
					"Unable to read journal file. Please check the logs for more details");
		}

		Iterator<EventData> eventIter = journal.iterator();

		return new StreamingDocumentIterator() {
			boolean sentAllDocuments = false;
			private int count = 0;

			@Override
			protected Document getNextDocument() {
				if (!sentAllDocuments) {
					if (!eventIter.hasNext()) {
						sentAllDocuments = true;
					}
					return buildEventDocument(document, "event", eventIter.next(), ++count);
				}
				closeStream();
				return endOfDocuments();
			}

			private void closeStream() {
				try {
					inputStream.close();
				} catch (IOException e) {
					// Do Nothing. Eat Exception.
				}
			}

		};

	}

	private Document buildEventDocument(Document inputDocument, String eventField, EventData event, int count) {
		
		if(event == null){
			return inputDocument;
		}
		
		DocumentBuilder docBuilder = callback.documentBuilder();
		Map<String, Object> fields = event.getFields();
		String eventString = new String(event.getRawContents(),StandardCharsets.UTF_8).trim();
		Matcher eventMatch = apacheLog.matcher(eventString);

		// Standard required fields
		docBuilder.setMetadata(StandardFields.ID, StringDocumentFieldValue.builder()
				.setString(inputDocument.getUniqueId() + "-" + Long.toString(count)).build());
		docBuilder.setMetadata(StandardFields.URI, inputDocument.getMetadataValue(StandardFields.URI));
		docBuilder.setMetadata(StandardFields.DATA_SOURCE_UUID,
				inputDocument.getMetadataValue(StandardFields.DATA_SOURCE_UUID));
		docBuilder.setMetadata(StandardFields.DISPLAY_NAME,
				StringDocumentFieldValue.builder().setString(
						inputDocument.getStringMetadataValue(StandardFields.DISPLAY_NAME) + "/" + Long.toString(count))
						.build());
		docBuilder.setMetadata(StandardFields.VERSION,
				LongDocumentFieldValue.builder().setLong(System.currentTimeMillis()).build());

		docBuilder.setMetadata(StandardFields.CONTENT_TYPE,
				StringDocumentFieldValue.builder().setString("text/plain").build());

		for (Map.Entry<String, Object> entry : fields.entrySet()) {
			docBuilder.setMetadata(entry.getKey(),
					StringDocumentFieldValue.builder().setString(entry.getValue().toString()).build());
		}
		
		if (eventMatch.matches()) {
			
			docBuilder.setMetadata(IPAddress,
					StringDocumentFieldValue.builder().setString(eventMatch.group(1)).build());
			
			docBuilder.setMetadata(UserAgent,
					StringDocumentFieldValue.builder().setString(eventMatch.group(2)).build());
			
			docBuilder.setMetadata(UserName,
					StringDocumentFieldValue.builder().setString(eventMatch.group(3)).build());
			
			docBuilder.setMetadata(DateTimestamp,
					StringDocumentFieldValue.builder().setString(eventMatch.group(4)).build());
			
			docBuilder.setMetadata(Action,
					StringDocumentFieldValue.builder().setString(eventMatch.group(5)).build());
            
			docBuilder.setMetadata(FilePath,
					StringDocumentFieldValue.builder().setString(eventMatch.group(6)).build());
			
			docBuilder.setMetadata(Agent,
					StringDocumentFieldValue.builder().setString(eventMatch.group(7)).build());
			
			docBuilder.setMetadata(StatusCode,
					StringDocumentFieldValue.builder().setString(eventMatch.group(8)).build());
			
			docBuilder.setMetadata(BytesTransferred,
					StringDocumentFieldValue.builder().setString(eventMatch.group(9)).build());
			
			String namespaceInfo = eventMatch.group(10);
			
			
			
			if (namespaceInfo != null && namespaceInfo.length()>0){
			   String[] namespace = namespaceInfo.split("\\.");
			   if (namespace.length == 2){
				   String namespaceName = namespace[0];
				   String[] tenantInfo = namespace[1].split(" ");
				   String tenantProtocol = tenantInfo[0];
				   String tenant;
				   String protocol;
				   if (tenantProtocol.contains("@")){
					   tenant = tenantProtocol.substring(0, tenantProtocol.indexOf("@"));
					   protocol = "hs3";
				   }else {
					   tenant = tenantProtocol;
					   protocol = "http(s)";
				   }
				   
				   String timemillis = tenantInfo[1];
				   docBuilder.setMetadata("ElapsedRequestTimeMillis",
							StringDocumentFieldValue.builder().setString(timemillis).build());
				   docBuilder.setMetadata("Gateway",
							StringDocumentFieldValue.builder().setString(protocol).build());
				   docBuilder.setMetadata(Namespace,
							StringDocumentFieldValue.builder().setString(namespaceName).build());
				   docBuilder.setMetadata("Tenant",
							StringDocumentFieldValue.builder().setString(tenant).build());
			   }
			}
            
		}

		if (eventString.length() > 1048576) {
			try {
				docBuilder.setStream(eventField + "_Stream", Collections.emptyMap(),
						eventString).build();
			} catch (IOException e) {
				//Eat Exception
			}
		} else {

			docBuilder.setMetadata(eventField, StringDocumentFieldValue.builder()
					.setString(eventString).build());
		}

		docBuilder.setHasContent(false);

		return docBuilder.build();
	}

	private String getExtension(String name) {
		int idx = name.lastIndexOf(".");
		if (idx == -1) {
			return null;
		}
		return name.substring(idx + 1);
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
