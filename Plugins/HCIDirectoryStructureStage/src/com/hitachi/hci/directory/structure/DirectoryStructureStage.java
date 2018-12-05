package com.hitachi.hci.directory.structure;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

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

public class DirectoryStructureStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.directory.structure.directoryStructureStage";
	private static final String PLUGIN_DISPLAY_NAME = "Directory Structure Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage creates a directory structure based on the structure type selected.\n ";
	private static final String DATE_DIR_STRUCTURE = "Date";
	private static final String GUID_DIR_STRUCTURE = "UUID";
	private final PluginConfig config;
	private final PluginCallback callback;

	// Radio options list
	private static List<String> radioOptions = new ArrayList<>();

	static {
		radioOptions.add(DATE_DIR_STRUCTURE);
		radioOptions.add(GUID_DIR_STRUCTURE);
	}
	// Override existing
	public static final ConfigProperty.Builder INCLUDE_MINUTES = new ConfigProperty.Builder().setName("minutes")
			.setType(PropertyType.CHECKBOX).setValue("false").setRequired(false).setUserVisibleName("Include Minutes")
			.setUserVisibleDescription("Check this option to include minutes in the directory structure format.");
	// Radio selection
	public static final ConfigProperty.Builder PROPERTY_DATE_GUID = new ConfigProperty.Builder()
			.setName("directoryStructureType").setType(PropertyType.RADIO).setOptions(radioOptions)
			.setValue(radioOptions.get(0)).setRequired(true).setUserVisibleName("Directory Structure Format")
			.setUserVisibleDescription("Select the directory structure format.");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_DATE_GUID);
		groupProperties.add(INCLUDE_MINUTES);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private DirectoryStructureStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public DirectoryStructureStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public DirectoryStructureStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new DirectoryStructureStage(config, callback);
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
			throw new ConfigurationException("No configuration for DirectoryStructure Stage");
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

		String structureType = this.config.getPropertyValue(PROPERTY_DATE_GUID.getName());

		if (DATE_DIR_STRUCTURE.equalsIgnoreCase(structureType)) {
			Boolean includeMinutes = Boolean.valueOf(this.config.getPropertyValue(INCLUDE_MINUTES.getName()));

			String defaultFormat = "/yyyy/MM/dd/HH/";

			if (includeMinutes) {
				defaultFormat = defaultFormat + "mm/";
			}

			SimpleDateFormat sdf = new SimpleDateFormat(defaultFormat, Locale.US);

			docBuilder.setMetadata("directoryStructure",
					StringDocumentFieldValue.builder().setString(sdf.format(new Date())).build());

		} else {
			String uuid = UUID.randomUUID().toString();

			docBuilder.setMetadata("uuid", StringDocumentFieldValue.builder().setString(uuid).build());

			String subPath = "/" + uuid.substring(0,3) + "/" + uuid.substring(3,5) + "/";

			docBuilder.setMetadata("directoryStructure", StringDocumentFieldValue.builder().setString(subPath).build());

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
