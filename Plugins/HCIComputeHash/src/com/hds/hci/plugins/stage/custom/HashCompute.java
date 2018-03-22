package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
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

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HashCompute implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.hashCompute";
	private static final String PLUGIN_DISPLAY_NAME = "Hash Compute";
	private static final String PLUGIN_DESCRIPTION = "This stage computes the hash of a given file and tags with an X_HCI_Hash metadata field.\n ";
	private final PluginConfig config;
	private final PluginCallback callback;

	private static final int HASH_READ_BUFFER_SIZE = 8 * 1024;

	private static String inputFieldName = "Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("HCI_content").setRequired(true).setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the stream whose hash has to be computed");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private HashCompute(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public HashCompute() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public HashCompute build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new HashCompute(config, callback);
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
			throw new ConfigurationException("No configuration for Encryption File Detection Stage");
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

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);

		// Compute the digest using SHA-256.
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		try {
			byte buffer[] = new byte[HASH_READ_BUFFER_SIZE];
			int numBytes;
			numBytes = inputStream.read(buffer);
			while (0 < numBytes) {
				md.update(buffer, 0, numBytes);

				numBytes = inputStream.read(buffer);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Build a string out of it.
		StringBuffer computedHashString = new StringBuffer();

		byte[] computedHash = md.digest();

		for (int i = 0; i < computedHash.length; i++) {
			/**
			 * Return a string representation of the integer argument as an
			 * unsigned integer in base 16.
			 * 
			 * The unsigned integer value is the argument plus 2^32 if the
			 * argument is negative; otherwise, it is equal to the argument.
			 * This value is converted to a string of ASCII digits in
			 * hexadecimal (base 16) with no extra leading 0s.
			 **/
			String hex = Integer.toHexString(0xff & computedHash[i]).toUpperCase();
			if (hex.length() == 1) {
				computedHashString.append('0');
			}
			computedHashString.append(hex);
		}
		docBuilder.setMetadata("X_HCI_Hash",
				StringDocumentFieldValue.builder().setString(computedHashString.toString()).build());

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
		return StagePluginCategory.ANALYZE;
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
