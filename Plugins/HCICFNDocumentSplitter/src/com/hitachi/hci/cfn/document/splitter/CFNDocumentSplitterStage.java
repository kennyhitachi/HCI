package com.hitachi.hci.cfn.document.splitter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
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

public class CFNDocumentSplitterStage implements StagePlugin {
	private static final String PLUGIN_NAME = "com.hitachi.hci.cfn.document.splitter.cFNDocumentSplitterStage";
	private static final String PLUGIN_DISPLAY_NAME = "CFN Document Splitter";
	private static final String PLUGIN_DESCRIPTION = "This stage reads from a content stream and splits the stream into multiple streams based on a delimiter. Individual Stream is examined for Branch and Rep codes, streams "
			+ "with same Branch and Rep id combo will be consolidated together into a single stream with page breaks .Each stream is then wrapped in a"
			+ "seperate document for further processing in the pipeline.";
	private static final String METADATA_FIELDS_GROUP_NAME = "Include these metadata fields for all the split documents";
	private static final String METADATA_FIELD_COLUMN_NAME = "Metadata Field Name";

	private static final Pattern patternBranch = Pattern.compile("((?<=BRANCH:\\s{1,10})\\w{1,3}(?=\\s{1,10}RR2:))");
	private static final Pattern patternRR2 = Pattern.compile("((?<=RR2:\\s{1,10})\\w+)");

	//private HashMap<String, String> hMap = new HashMap<String, String>();

	private List<String> metadataCfgNames = new ArrayList<>();

	private final PluginConfig config;
	private final PluginCallback callback;

	private static String inputFieldName = "Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("HCI_content").setRequired(true).setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the stream that points to the actual file that needs to be split");
	// Text field
	public static final ConfigProperty.Builder PROPERTY_DELIMITER_NAME = new ConfigProperty.Builder()
			.setName("Delimiter").setValue("").setRequired(true).setUserVisibleName("Delimiter")
			.setUserVisibleDescription(
					"Delimiter or a regular expression based on the which the file needs to be split");

	private static List<ConfigProperty.Builder> metadataFieldGroupProperties = new ArrayList<>();

	static {
		// Default properties in a single value table may be included but can
		// be deleted by the end user.
		metadataFieldGroupProperties.add(new ConfigProperty.Builder().setName("HCI_filename").setValue("HCI_filename"));
	}

	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_TWO = new ConfigPropertyGroup.Builder(
			METADATA_FIELDS_GROUP_NAME, null).setType(PropertyGroupType.SINGLE_VALUE_TABLE)
					.setOptions(Arrays.asList(METADATA_FIELD_COLUMN_NAME))
					.setConfigProperties(metadataFieldGroupProperties);

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
		groupProperties.add(PROPERTY_DELIMITER_NAME);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties))
			.addGroup(PROPERTY_GROUP_TWO).build();

	private CFNDocumentSplitterStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public CFNDocumentSplitterStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public CFNDocumentSplitterStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new CFNDocumentSplitterStage(config, callback);
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
			throw new ConfigurationException("No configuration for found for the Document Splitter Stage");
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

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				StandardFields.CONTENT);

		ConfigPropertyGroup metadataCfgProperty = this.config.getGroup(METADATA_FIELDS_GROUP_NAME);
		metadataCfgNames = metadataCfgProperty.getProperties().stream().map(ConfigProperty::getName)
				.collect(Collectors.toList());

		InputStream inputStream = this.callback.openNamedStream(document, streamName);
		
		HashMap<String, String> hMap = new HashMap<String, String>();

		String delimiter = this.config.getPropertyValue(PROPERTY_DELIMITER_NAME.getName());
		boolean includeHeader = true;

		Scanner scan = new Scanner(inputStream);
		scan.useDelimiter(delimiter);

		while (scan.hasNext()) {
			String line = scan.next().trim();
			if (includeHeader) {
				hMap.put("HEADER", line);
				includeHeader = false;
				continue;
			}
			Matcher matchBranch = patternBranch.matcher(line);
			Matcher matchRR2 = patternRR2.matcher(line);
			String repId = "";
			if (matchBranch.find()) {
				if (!matchRR2.find()) {
					repId = "XXXX";
				} else {
					repId = matchRR2.group(0);
				}
				if (hMap.isEmpty()) {
					hMap.put(matchBranch.group(0) + "_" + repId, line);
				} else {
					String existingLine = hMap.get(matchBranch.group(0) + "_" + repId);
					if (existingLine != null) {

						StringBuilder sb = new StringBuilder();
						sb.append(existingLine);
						sb.append("\n");
						sb.append("\f");
						sb.append("\n");
						sb.append(line);
						hMap.put(matchBranch.group(0) + "_" + repId, sb.toString());

					} else {
						hMap.put(matchBranch.group(0) + "_" + repId, line);
					}
				}
			}
		}

		scan.close();

		Iterator<Map.Entry<String, String>> entrySet = hMap.entrySet().iterator();

		return new StreamingDocumentIterator() {
			boolean sentAllDocuments = false;
			private int count = 0;

			@Override
			protected Document getNextDocument() {
				if (!sentAllDocuments) {
					if (!entrySet.hasNext()) {
						sentAllDocuments = true;
						return document;
					}
					return buildEventDocument(document, "Split_Stream", entrySet.next(), ++count);
				}
				closeStream();
				return endOfDocuments();
			}

			private void closeStream() {
				try {
					scan.close();
					inputStream.close();
				} catch (IOException e) {
					// Do Nothing. Eat Exception.
				}
			}

		};

	}

	private Document buildEventDocument(Document inputDocument, String eventField,
			Map.Entry<String, String> splitStream, int count) {

		if (splitStream == null) {
			return inputDocument;
		}

		DocumentBuilder docBuilder = callback.documentBuilder();

		// Standard required fields
		docBuilder.setMetadata(StandardFields.ID, StringDocumentFieldValue.builder()
				.setString(inputDocument.getUniqueId() + "-" + Long.toString(count)).build());
		docBuilder.setMetadata(StandardFields.URI, inputDocument.getMetadataValue(StandardFields.URI));
		docBuilder.setMetadata(StandardFields.DATA_SOURCE_UUID,
				inputDocument.getMetadataValue(StandardFields.DATA_SOURCE_UUID));
		docBuilder.setMetadata(StandardFields.DISPLAY_NAME,
				StringDocumentFieldValue.builder().setString(
						inputDocument.getStringMetadataValue(StandardFields.DISPLAY_NAME) + "-" + Long.toString(count))
						.build());
		docBuilder.setMetadata(StandardFields.VERSION,
				LongDocumentFieldValue.builder().setLong(System.currentTimeMillis()).build());

		docBuilder.setMetadata(StandardFields.CONTENT_TYPE,
				StringDocumentFieldValue.builder().setString("text/plain;charset=utf-8").build());
		docBuilder.setMetadata(StandardFields.PARENT_DISPLAY, inputDocument.getMetadataValue(StandardFields.FILENAME));
		docBuilder.setMetadata("CFN_fileSuffix",
				StringDocumentFieldValue.builder().setString(splitStream.getKey()).build());
		
		String[] tokens = splitStream.getKey().split("_");
		
		if (tokens != null && tokens.length == 2){
			docBuilder.setMetadata("CFN_BRANCH",
					StringDocumentFieldValue.builder().setString(tokens[0]).build());
			docBuilder.setMetadata("CFN_REP_ID",
					StringDocumentFieldValue.builder().setString(tokens[1]).build());
		}

		if (!metadataCfgNames.isEmpty()) {
			for (String metadataField : metadataCfgNames) {
				docBuilder.setMetadata(metadataField, inputDocument.getMetadataValue(metadataField));
			}
		}

		try {
			docBuilder.setStream(eventField, Collections.emptyMap(), new String(splitStream.getValue().getBytes(),"UTF-8")).build();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		docBuilder.setHasContent(false);

		return docBuilder.build();
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
