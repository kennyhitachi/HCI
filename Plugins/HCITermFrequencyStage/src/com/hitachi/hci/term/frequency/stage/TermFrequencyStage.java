package com.hitachi.hci.term.frequency.stage;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;

import com.hds.ensemble.sdk.model.StandardFields;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * TermFrequency divides text into a sequence of tokens and counts frequencies
 * of each token. The class has a pre-processing layer that get rid of
 * stop-words in English language. The class tags a metadata to the object
 * containing the key-value pair list of tokens and their frequencies.
 * 
 * TermFrequency utilizes resources in CoreNLP and integrates them into HCI.
 */
public class TermFrequencyStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.term.frequency.stage.TermFrequencyStage";
	private static final String NAME = "Term Frequency Stage";
	private static final String DESCRIPTION = "Annotates term frequencies in document";

	private final PluginConfig config;
	private final PluginCallback callback;

	private static String inputStreamName = StandardFields.CONTENT;

	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName(inputStreamName).setValue("HCI_text").setRequired(true).setUserVisibleName("Stream")
			.setUserVisibleDescription("Name of the source stream to read text from (default: HCI_text)");

	public static final ConfigProperty.Builder PROPERTY_INPUT_AREA_NAME = new ConfigProperty.Builder()
			.setName("TextArea").setValue("").setRequired(false).setType(PropertyType.TEXT_AREA)
			.setUserVisibleName("TextArea").setUserVisibleDescription("Stopwords list. Update the list as needed");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_STREAM_NAME);
		groupProperties.add(PROPERTY_INPUT_AREA_NAME);
	}

	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_ONE = new ConfigPropertyGroup.Builder("Options",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(groupProperties);

	// Default config
	// This default configuration will be returned to callers of
	// getDefaultConfig().
	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(PROPERTY_GROUP_ONE).build();

	// Default constructor for new unconfigured plugin instances (can obtain
	// default
	// config)
	public TermFrequencyStage() {
		config = this.getDefaultConfig();
		callback = null;
	}

	// Constructor for configured plugin instances to be used in workflows
	private TermFrequencyStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);
		TermFrequencyStage.inputStreamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(),
				"HCI_text");
	}

	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		// This method is used to ensure that the specified configuration is
		// valid for
		// this
		// connector, i.e. that required properties are present in the
		// configuration and
		// no invalid
		// values are set.

		// This method handles checking for non-empty and existing required
		// properties.
		// It should typically always be called here.
		Config.validateConfig(getDefaultConfig(), config);

	}

	@Override
	public TermFrequencyStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		// This method is used as a factory to create a configured instance of
		// this
		// stage
		return new TermFrequencyStage(config, callback);
	}

	@Override
	public String getHost() {
		// If this plugin utilizes SSL to talk to remote servers, the hostname
		// of the
		// server being
		// connected to should be returned here. Plugins which do not use SSL
		// can safely
		// return
		// null.
		return null;
	}

	@Override
	public Integer getPort() {
		// If this plugin utilizes SSL to talk to remote servers, the port of
		// the server
		// being
		// connected to should be returned here. Plugins which do not use SSL
		// can safely
		// return
		// null.
		return null;
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		// If this plugin maintains state to communicate with remote servers, a
		// plugin
		// defined
		// PluginSession should be returned here.
		// Plugins which do not use SSL can safely return
		// PluginSession.NOOP_INSTANCE.
		return PluginSession.NOOP_INSTANCE;
	}

	@Override
	public String getName() {
		// The fully qualified class name of this plugin
		return PLUGIN_NAME;
	}

	@Override
	public String getDisplayName() {
		// The name of this plugin to display to end users
		return NAME;
	}

	@Override
	public String getDescription() {
		// A user-visible description string describing this plugin
		return DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		// This method is used to specify a default plugin configuration.
		// Configuration properties are defined here, including their type, how
		// they are
		// presented, and whether or not they require valid user input before
		// the
		// connector
		// can be used.
		// This default configuration is also what will appear in the UI when a
		// new
		// instance of your
		// plugin is created.
		return DEFAULT_CONFIG;
	}

	@Override
	public Iterator<Document> process(PluginSession session, Document inputDocument)
			throws ConfigurationException, PluginOperationFailedException {

		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(inputDocument);

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(inputDocument, streamName);

		String textArea = this.config.getPropertyValue(PROPERTY_INPUT_AREA_NAME.getName());

		HashSet<String> stopWordsSet = new HashSet<>();
		for (String line : textArea.split("\\n")) {
			stopWordsSet.add(line);
		}

		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
			// Term Frequency
			HashMap<String, Integer> termFrequencyMapUnsorted = new HashMap<>();

			// Read lines
			String line;
			while ((line = br.readLine()) != null) {
				// Remove all punctuations
				line = line.replaceAll("\\p{IsPunctuation}", "");

				// Split line by whitespace
				String[] terms = line.split("\\s+");

				// Check if term is a stopword
				for (int i = 0; i < terms.length; i++) {
					if (stopWordsSet.contains(terms[i].toLowerCase())) {
						continue;
					} else if (terms[i].contains(System.getProperty("line.separator")) || terms[i].matches("^\\s*$")) {
						continue;
					} else if (termFrequencyMapUnsorted.containsKey(terms[i].toLowerCase())) {
						// increment term counter
						int oldvalue = termFrequencyMapUnsorted.get(terms[i].toLowerCase());
						termFrequencyMapUnsorted.put(terms[i].toLowerCase(), oldvalue + 1);
					} else {
						termFrequencyMapUnsorted.put(terms[i].toLowerCase(), 1);
					}
				}
			}

			// Sort TF map by descending order
			LinkedHashMap<String, Integer> termFrequencyMapSorted = new LinkedHashMap<>();

			termFrequencyMapUnsorted.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
					.forEachOrdered(x -> termFrequencyMapSorted.put(x.getKey(), x.getValue()));

			// Limit TF map to top 100 terms
			// This is to not exceed 1MB ceiling of metadata tags in HCI
			String termFrequency = "Term Frequency: ";
			if (termFrequencyMapSorted.size() <= 100) {
				termFrequency = termFrequencyMapSorted.entrySet().toString();
			} else {
				Iterator<Entry<String, Integer>> it = termFrequencyMapSorted.entrySet().iterator();
				int top100 = 100;
				for (int i = 0; i < top100 && it.hasNext(); i++) {
					termFrequency = termFrequency + " " + (Map.Entry<String, Integer>) it.next();
				}
			}

			// TODO add metadata tags
			docBuilder.setMetadata("TermFrequency",
					StringDocumentFieldValue.builder().setString(termFrequency).build());

			br.close();
		} catch (IOException e) {
			throw new PluginOperationRuntimeException(
					new PluginOperationFailedException("Error processing filestream: " + e.getMessage()));

		}
		//
		// HCI Document Streams
		//
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

	@Override
	public StagePluginCategory getCategory() {
		// This method is used to determine which top level category the
		// stage plugin should be grouped in for UI display purposes.
		return StagePluginCategory.OTHER;
	}

	@Override
	public String getSubCategory() {
		// This method is used to determine which sub-category the stage
		// plugin should be grouped in for UI display purposes.
		return "Custom";
	}
}
