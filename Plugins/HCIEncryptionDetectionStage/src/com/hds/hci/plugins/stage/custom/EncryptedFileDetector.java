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
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;
import com.hds.hci.plugins.stage.utils.EncryptedFileException;
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

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.microsoft.OfficeParser;
import org.apache.tika.parser.microsoft.ooxml.OOXMLParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;

public class EncryptedFileDetector implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.stage.custom.encryptedFileDetector";
	private static final String PLUGIN_DISPLAY_NAME = "Encrypted File Detection";
	private static final String PLUGIN_DESCRIPTION = "This stage detects if a given file is password-protected or encrypted and tags with an Encrypted metadata field";
	private static final Set<String> PDF_TYPES = Stream.of("pdf")
			.collect(Collectors.toCollection(HashSet::new));
	private static final Set<String> OLD_OFFICE_TYPES = Stream.of("doc", "ppt", "xls")
			.collect(Collectors.toCollection(HashSet::new));
	private static final Set<String> NEW_OFFICE_TYPES = Stream.of("docx","pptx","xlsx")
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

	private EncryptedFileDetector(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public EncryptedFileDetector() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public EncryptedFileDetector build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new EncryptedFileDetector(config, callback);
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
		String mimeType = document.getStringMetadataValue("Content_Type");
		String fileType = document.getStringMetadataValue("HCI_filename");
		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(), StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);
		String[] fileNameTokens = fileType.split("\\.");
		Parser parser = null;
		if (fileNameTokens.length == 2 && PDF_TYPES.contains(fileNameTokens[1].toLowerCase().trim())) {
			parser = new PDFParser();
		} else if (fileNameTokens.length == 2 && OLD_OFFICE_TYPES.contains(fileNameTokens[1].toLowerCase().trim())) {
			parser = new OfficeParser();
		} else if (fileNameTokens.length == 2 && NEW_OFFICE_TYPES.contains(fileNameTokens[1].toLowerCase().trim())) {
			parser = new OOXMLParser();
		} else {
			parser = new AutoDetectParser();
		}
		  BodyContentHandler handler = new BodyContentHandler();
		  Metadata metadata = new Metadata();
		  ParseContext context = new ParseContext();
		  List<String> encFileList = new ArrayList<String>();
		  TikaInputStream stream = null;
			try {
				TemporaryResources resources = new TemporaryResources();
				resources.setTemporaryFileDirectory(this.callback.getTempDirectory(session).toFile());
				stream = TikaInputStream.get(inputStream, resources);
				
				try {
					if (mimeType != null) {
                        metadata.set("Content-Type", mimeType);
					}
                    metadata.set("resourceName", document.getStringMetadataValue(StandardFields.FILENAME));
                    
                    if (NEW_OFFICE_TYPES.contains(fileNameTokens[1].toLowerCase().trim()) 
                    		&& mimeType.toLowerCase().contains("protected")) {
                        throw new EncryptedFileException("document encrypted");
                    }
					parser.parse(stream, handler, metadata, context);
				} catch (Exception e) {
					String message = e.getMessage();
					Throwable cause = e.getCause();
					String causeStr = "";

					if (cause != null) {
						causeStr = cause.getMessage().toString();
					}

					if ((message.toLowerCase().contains("encrypt")) || (causeStr.toLowerCase().contains("encrypt"))) {

						docBuilder.setMetadata("$EncryptedFile",
								BooleanDocumentFieldValue.builder().setBoolean(Boolean.TRUE).build());
												
						String relPath = this.config.getPropertyValueOrDefault(PROPERTY_REL_PATH_FIELD_NAME.getName(), "encrypted/file/content");
						
						docBuilder.setMetadata("$EncRelPath",
								StringDocumentFieldValue.builder().setString(relPath).build());
						
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
						 encFileList.add(fileName);
						    
						EncryptedFileRecord fileRecord = new EncryptedFileRecord(document.getStringMetadataValue(StandardFields.ID),
									fileName, encFileList,1,document.getStringMetadataValue(StandardFields.CREATED_DATE_STRING));
						
						
						InputStream instream = new ByteArrayInputStream(
								fileRecord.toString().getBytes(StandardCharsets.UTF_8));
						
						docBuilder.setStream("$Enc_filecontent", Collections.emptyMap(), instream);
						
					}

			} 
			}catch (Exception e) {
				throw new PluginOperationRuntimeException(new PluginOperationFailedException("Failed to process document", (Throwable)e));
			} finally {
				try {
				    if (stream != null) {
					stream.close();
					}
					if (inputStream != null) {
					inputStream.close();
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
