package com.hitachi.hci.content.stage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.hwpf.usermodel.Range;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.BooleanDocumentFieldValue;
import com.hds.ensemble.sdk.model.DateDocumentFieldValue;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentFieldValue;
import com.hds.ensemble.sdk.model.DocumentUtil;
import com.hds.ensemble.sdk.model.IntegerDocumentFieldValue;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;

public class ContentAnonymizerStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.content.stage.contentAnonymizerStage";
	private static final String PLUGIN_DISPLAY_NAME = "Content Anonymizer Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage finds a given pattern and replaces the pattern with a given text in the content of a file.\n ";

	private static final int DEFAULT_FONT_SIZE = 10;

	private final PluginConfig config;
	private final PluginCallback callback;

	static final String SPACE_CONSTANT = "%SPACE";

	public static final String STREAM_TO_PROCESS_GROUP_NAME = "Stream to Process";
	private static String inputStreamName = StandardFields.CONTENT;
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName(inputStreamName).setValue(inputStreamName).setRequired(true).setUserVisibleName("Stream")
			.setUserVisibleDescription("Name of the stream whose content needs to be anonymized");

	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_STREAM_NAME);
	}

	public static final String REPLACEMENT_GROUP_NAME = "Values to Replace";
	private static final String REPLACEMENT_GROUP_DESCRIPTION = "Specify one or more source regular expressions "
			+ "supported by the Java \"Pattern\" class and the corresponding replacement text for each matching "
			+ "subsequence. For example, use expression \"[0-9]{4}-[0-9]{4}-[0-9]{4}-\" with replacement \"XXXXXX-\" "
			+ "to redact credit card numbers, or expression \"physician\" with replacement \"doctor\" for simple word or phrase replacements. "
			+ "Use \"" + SPACE_CONSTANT + "\" to specify a single space character in replacement values. "
			+ "Document field values can be included in replacement text using ${fieldName} syntax, " + "eg ${"
			+ StandardFields.DISPLAY_NAME + "}.";

	private static final String COLUMN_ONE_NAME = "Source Expression";
	private static final String COLUMN_TWO_NAME = "Replacement";

	// REPLACEMENT GROUP
	// Key-Value table for content replacement
	private static ConfigPropertyGroup.Builder REPLACEMENT_GROUP = new ConfigPropertyGroup.Builder(
			REPLACEMENT_GROUP_NAME, REPLACEMENT_GROUP_DESCRIPTION).setConfigProperties(Lists.newArrayList())
					.setType(PropertyGroupType.KEY_VALUE_TABLE)
					.setOptions(ImmutableList.of(COLUMN_ONE_NAME, COLUMN_TWO_NAME));

	// DEFAULT CONFIG
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(
			new ConfigPropertyGroup.Builder(STREAM_TO_PROCESS_GROUP_NAME, null).setConfigProperties(groupProperties))
			.addGroup(REPLACEMENT_GROUP).build();

	private Map<Pattern, String> patternToReplacementMap;

	private ContentAnonymizerStage(PluginConfig pluginConfig, PluginCallback callback) throws ConfigurationException {
		validateConfig(pluginConfig);
		this.config = pluginConfig;
		this.callback = callback;

		// Source/Replacement Properties
		ConfigPropertyGroup targetGroup = config.getGroup(REPLACEMENT_GROUP_NAME);
		List<ConfigProperty> sourceReplacementProperties = targetGroup.getProperties();
		patternToReplacementMap = Maps.newLinkedHashMap();

		try {
			// Pre-compile the Patterns for efficiency
			for (ConfigProperty property : sourceReplacementProperties) {
				Pattern pattern = Pattern.compile(property.getName());
				String replacement = property.getValue();
				if (replacement != null) {
					replacement = replacement.replaceAll(SPACE_CONSTANT, " ");
				}
				patternToReplacementMap.put(pattern, replacement);
			}
		} catch (PatternSyntaxException ex) {
			throw new ConfigurationException("Invalid syntax in replacement source configuration", ex);
		}
	}

	/**
	 * This no-argument constructor is needed for SPI, and shouldn't be used
	 * elsewhere.
	 */
	public ContentAnonymizerStage() {
		config = getDefaultConfig();
		callback = null;
	}

	@Override
	public ContentAnonymizerStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new ContentAnonymizerStage(config, callback);
	}

	@Override
	public String getHost() {
		return null;
	}

	@Override
	public Integer getPort() {
		return null;
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		return PluginSession.NOOP_INSTANCE;
	}

	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		Config.validateConfig(getDefaultConfig(), config);

		// Field name Properties
		ConfigPropertyGroup fieldNameGroup = config.getGroup(STREAM_TO_PROCESS_GROUP_NAME);
		if (fieldNameGroup == null) {
			throw new ConfigurationException(
					"Missing configuration for group \"" + STREAM_TO_PROCESS_GROUP_NAME + "\"");
		}
		List<ConfigProperty> fieldNameProperties = fieldNameGroup.getProperties();
		if (fieldNameProperties == null || fieldNameProperties.isEmpty()) {
			throw new ConfigurationException("Need to specify one or more field names in configuration group \""
					+ STREAM_TO_PROCESS_GROUP_NAME + "\"");
		}

		// Source/Replacement Properties
		ConfigPropertyGroup targetGroup = config.getGroup(REPLACEMENT_GROUP_NAME);
		if (targetGroup == null) {
			throw new ConfigurationException("Missing configuration group \"" + REPLACEMENT_GROUP_NAME + "\"");
		}
		List<ConfigProperty> sourceReplacementProperties = targetGroup.getProperties();
		if (sourceReplacementProperties == null || sourceReplacementProperties.isEmpty()) {
			throw new ConfigurationException(
					"Need to specify one or more replacement mappings in configuration group \""
							+ REPLACEMENT_GROUP_NAME + "\"");
		}
		for (ConfigProperty property : sourceReplacementProperties) {
			if (Strings.isNullOrEmpty(property.getName())) {
				throw new ConfigurationException("Missing configuration for replacement source expression");
			}
			try {
				Pattern.compile(property.getName());
			} catch (PatternSyntaxException ex) {
				throw new ConfigurationException("Invalid syntax in replacement source configuration", ex);
			}
		}
	}

	@Override
	public String getDisplayName() {
		return PLUGIN_DISPLAY_NAME;
	}

	@Override
	public String getName() {
		return PLUGIN_NAME;
	}

	@Override
	public String getDescription() {
		return PLUGIN_DESCRIPTION;
	}

	@Override
	public String getLongDescription() {
		return PLUGIN_DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	public Iterator<Document> process(PluginSession session, Document document)
			throws ConfigurationException, PluginOperationFailedException {
		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(document);

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_STREAM_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);
		FileOutputStream fos = null;
		try {
			if (FileMagic.valueOf(inputStream) == FileMagic.OLE2) {
				docBuilder.setMetadata("OldDocFormat",
						BooleanDocumentFieldValue.builder().setBoolean(Boolean.TRUE).build());
				POIFSFileSystem fs = null;
				Path tempPath = this.callback.getTempDirectory(session);
				File tempOutFile = File.createTempFile("ReplaceContentStagePlugin-", ".tmp", tempPath.toFile());

				try {

					fs = new POIFSFileSystem(inputStream);

					HWPFDocument doc = new HWPFDocument(fs);
					WordExtractor we = new WordExtractor(doc);

					String[] paragraphs = we.getParagraphText();
					for (int i = 0; i < paragraphs.length; i++) {

						for (Map.Entry<Pattern, String> entry : patternToReplacementMap.entrySet()) {

							Pattern pattern = entry.getKey();
							String replacement = entry.getValue();

							// Support ${fieldName} value substitution
							// in
							// replacement strings
							if (replacement != null) {
								replacement = DocumentUtil.evaluateTemplate(document, replacement);
							}

							Matcher matcher = pattern.matcher(paragraphs[i]);

							while (matcher.find()) {
								for (int j = 0; j <= matcher.groupCount(); j++) {
									doc.getRange().replaceText(matcher.group(j), replacement);
								}
							}
						}
					}
					fos = new FileOutputStream(tempOutFile);
					doc.write(fos);

					docBuilder.setStream(streamName, new HashMap<String, String>(),
							Paths.get(tempOutFile.getAbsolutePath()));

				} catch (Exception e) {

					e.printStackTrace();
				} finally {
					if (fos != null) {
						try {
							fos.flush();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							fos.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if (FileMagic.valueOf(inputStream) == FileMagic.OOXML) {
				try {
					if (inputStream != null) {

						Path tempPath = this.callback.getTempDirectory(session);
						File tempOutFile = File.createTempFile("ReplaceContentStagePlugin-", ".tmp", tempPath.toFile());

						XWPFDocument doc = new XWPFDocument(inputStream);
						XWPFDocument newdoc = new XWPFDocument();

						List<XWPFParagraph> paras = doc.getParagraphs();

						for (XWPFParagraph para : paras) {

							XWPFParagraph newpara = newdoc.createParagraph();

							for (XWPFRun run : para.getRuns()) {
								String textInRun = run.getText(run.getTextPosition());

								int fontSize = run.getFontSize();

								XWPFRun newRun = newpara.createRun();

								for (Map.Entry<Pattern, String> entry : patternToReplacementMap.entrySet()) {

									Pattern pattern = entry.getKey();
									String replacement = entry.getValue();

									// Support ${fieldName} value substitution
									// in
									// replacement strings
									if (replacement != null) {
										replacement = DocumentUtil.evaluateTemplate(document, replacement);
									}

									Matcher matcher = pattern.matcher(textInRun);

									textInRun = matcher.replaceAll(replacement);
								}
								// Replace text
								newRun.setText(textInRun, run.getTextPosition());

								// Apply the same style
								newRun.setFontSize((fontSize == -1) ? DEFAULT_FONT_SIZE : run.getFontSize());
								newRun.setFontFamily(run.getFontFamily());
								newRun.setBold(run.isBold());
								newRun.setItalic(run.isItalic());
								newRun.setColor(run.getColor());

							}

						}
						// docBuilder.setStream(streamName,
						// document.getAllStreamMetadata().get(streamName),
						// inputStream);

						fos = new FileOutputStream(tempOutFile);
						newdoc.write(fos);

						docBuilder.setStream(streamName, new HashMap<String, String>(),
								Paths.get(tempOutFile.getAbsolutePath()));
					}

				} catch (Exception e) {

				} finally {
					if (fos != null) {
						try {
							fos.flush();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							fos.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else {
				throw new PluginOperationRuntimeException(
						new PluginOperationFailedException("Current Supported Formats are .doc and docx"));
			}
		} catch (Exception e) {

		}

		return Iterators.singletonIterator(docBuilder.build());

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
