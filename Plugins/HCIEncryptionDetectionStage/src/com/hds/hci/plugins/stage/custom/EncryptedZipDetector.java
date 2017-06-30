package com.hds.hci.plugins.stage.custom;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.BooleanDocumentFieldValue;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentFieldValue;
import com.hds.ensemble.sdk.model.IntegerDocumentFieldValue;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.hds.hci.plugins.stage.utils.EncryptedFileRecord;
import com.hds.hci.plugins.stage.utils.StringUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

public class EncryptedZipDetector implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.encryptedZipDetector";
	private static final String PLUGIN_DISPLAY_NAME = "Encrypted Zip Detection";
	private static final String PLUGIN_DESCRIPTION = "This stage detects if a given zip file is password-protected or encrypted and tags with an Encrypted metadata field";
	private static final Set<String> SUPPORTED_MIME_TYPES = Stream.of("application/zip", "application/octet-stream")
			.collect(Collectors.toCollection(HashSet::new));
	private final PluginConfig config;
	private final PluginCallback callback;

	private static String inputFieldName = "Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("HCI_content").setRequired(true).setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the stream that needs to be checked for encryption");
	
	// Text field
		public static final ConfigProperty.Builder PROPERTY_REL_PATH_FIELD_NAME = new ConfigProperty.Builder()
				.setName("Encoded Content Relative Path").setValue("").setRequired(true).setUserVisibleName("Relative Path")
				.setUserVisibleDescription("Relative Path where the encoded content needs to be written on HCP");
		
	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
		groupProperties.add(PROPERTY_REL_PATH_FIELD_NAME);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private EncryptedZipDetector(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public EncryptedZipDetector() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public EncryptedZipDetector build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new EncryptedZipDetector(config, callback);
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
			throw new ConfigurationException("No configuration for Encryption Zip Detection Stage");
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
		String mimeType = document.getStringMetadataValue("Content_Type");
		if (mimeType == null) {
			mimeType = "application/octet-stream";
		}

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(), StandardFields.CONTENT);
		final InputStream inputStream = this.callback.openNamedStream(document, streamName);
		final ZipArchiveInputStream zipStream = new ZipArchiveInputStream(inputStream);
		
		List<String> encFileList = new ArrayList<String>();

		if (SUPPORTED_MIME_TYPES.contains(mimeType)) {
			
			try {
				ZipArchiveEntry entry = null;
				boolean isEncryptExists = false;
				int fileCount = 0;
				while ((entry = zipStream.getNextZipEntry()) != null) {
					if ((entry).getGeneralPurposeBit().usesEncryption()
							|| (entry).getGeneralPurposeBit().usesStrongEncryption()) {
						
						encFileList.add(entry.getName());
						fileCount++;
						isEncryptExists = true;
					} 
				}
				if (isEncryptExists) {
					
					docBuilder.setMetadata("$EncryptedZip",
							BooleanDocumentFieldValue.builder().setBoolean(Boolean.TRUE).build());
					docBuilder.setMetadata("$EncryptedZipFileCount",
							IntegerDocumentFieldValue.builder().setInteger(fileCount).build());
					

					List<String> values = new ArrayList<>();
					
					String fileName = document.getStringMetadataValue(StandardFields.FILENAME);
					DocumentFieldValue<?> metadataValue = document.getMetadataValue(StandardFields.PARENT_DISPLAY);
					StringJoiner joiner = new StringJoiner("-");
					if(metadataValue != null) {
	                    Set<String> rawValues = metadataValue.getAllRawValues();
	                    values.addAll(rawValues);
	                    
	                    String fileNameNoExt = StringUtil.removeExtension(fileName);
					    for (String value : values) {
						   value = StringUtil.removeExtension(value);
						   joiner.add(value.toString());
					    }
						
					    fileNameNoExt = joiner.toString()+"-"+fileNameNoExt;
													
						String encFileName = fileNameNoExt+".csv";
						docBuilder.setMetadata("$EncFileName",
								StringDocumentFieldValue.builder().setString(encFileName).build());
					} else {
						String fileNameNoExt = StringUtil.removeExtension(fileName);
						fileNameNoExt = fileNameNoExt+".csv";
						docBuilder.setMetadata("$EncFileName",
								StringDocumentFieldValue.builder().setString(fileNameNoExt).build());
					}
					
					String relPath = this.config.getPropertyValueOrDefault(PROPERTY_REL_PATH_FIELD_NAME.getName(), "encrypted/zip/content");
					
					docBuilder.setMetadata("$EncRelPath",
							StringDocumentFieldValue.builder().setString(relPath).build());
					EncryptedFileRecord fileRecord = new EncryptedFileRecord(document.getStringMetadataValue(StandardFields.URI),
							fileName, encFileList, fileCount, document.getStringMetadataValue(StandardFields.CREATED_DATE_STRING));
					InputStream stream = new ByteArrayInputStream(
							fileRecord.toString().getBytes(StandardCharsets.UTF_8));
					docBuilder.setStream("$Enc_zipcontent", Collections.emptyMap(), stream);
				}

			} catch (Exception e) {
				throw new PluginOperationRuntimeException(new PluginOperationFailedException("Failed to process document", (Throwable)e));
			} finally {
				try {
					zipStream.close();
					inputStream.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

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
