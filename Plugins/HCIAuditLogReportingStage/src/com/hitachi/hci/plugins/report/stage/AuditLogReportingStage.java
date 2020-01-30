/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2018. All rights reserved.
 *
 * ========================================================================
 */

package com.hitachi.hci.plugins.report.stage;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
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

public class AuditLogReportingStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.report.stage.auditLogReportingStage";
	private static final String PLUGIN_DISPLAY_NAME = "Audit Log Reporting";
	private static final String PLUGIN_DESCRIPTION = "This stage generates three streams that contain the Summary, Processed Items and Failure Items information.\n ";

	private static final String BASE_QUERY = "q=*:*&fq=runDate:[START TO END]&rows=0&wt=json";
	private static final String TEST_QUERY = "q=*:*&rows=0&wt=json";
	private static final String FACET_QUERY = "json.facet={moved:{type : terms,mincount : 1,limit : -1,numBuckets : true,field : MovedToCountry},"
			+ "numread:{type : terms,mincount : 1,limit : -1,numBuckets : true,field : numberRead}}";
	private static final String PROCESSED_QUERY = "q=*:*&fq=runDate:[START TO END]&fq=MovedDateTime:[* TO *]&fl=FileName:HCI_filename,OriginalLocation:HCI_URI,NewLocation:targetURI,UserName:combinedFields,Date_Moved_To_Country_Namespace:MovedDateTime,Date_of_ICEChat_Email:Creation_Date,Date_Ingested_To_HCP:HCI_modifiedDateString&rows=ITEMS&wt=csv";
	private static final String FAILURE_QUERY = "q=*:*&fq=runDate:[START TO END]&fq=-MovedDateTime:[* TO *]&fl=Date_of_ICEChat_Email:Creation_Date,UserName:account,OriginalLocation:HCI_URI&rows=ITEMS&wt=csv";

	private static final String START_DATE_SUFFIX = "T00:00:00Z";
	private static final String END_DATE_SUFFIX = "T23:59:59Z";

	private static final String DEFAULT_MAX_ROWS = "10000";
	
	private static final String DEFAULT_WORKFLOW_NAME = "ICEChatIngestWF_v1.0";

	private static final String M_AUDIT_LOG_NAME = "auditLogFilename";
	private static final String M_RELATIVE_PATH = "relativePath";
	private static final String M_EMAIL_METADATA = "emailData";
	private static final String M_WRITE_AUDIT_LOG = "writeAuditLog";
	private static final String PATH_SEPERATOR = "/";

	private static final String AUDIT_LOG_NAME = "_ICEchatProcessLog.txt";

	private static final String FACET_STREAM_NAME = "Facet_Stream";
	private static final String PROCESSED_STREAM_NAME = "Processed_Stream";
	private static final String FAILURE_STREAM_NAME = "Failure_Stream";
	private static final String PROCESSED_HEADER_STREAM_NAME = "Processed_Header";
	private static final String FAILURE_HEADER_STREAM_NAME = "Failure_Header";

	private static final String SOLR_SCHEME = "http://";

	private static final String EMPTY = "";

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat sdf_time = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	private final PluginConfig config;
	private final PluginCallback callback;

	// Host Name Text Field
	public static final ConfigProperty.Builder HOST_NAME = new ConfigProperty.Builder()

			.setName("hci.host").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Solr Host")
			.setUserVisibleDescription("Host Name of the instance running the Solr Service");

	// PORT Text Field
	public static final ConfigProperty.Builder PORT = new ConfigProperty.Builder()

			.setName("hci.port").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Solr Port").setUserVisibleDescription("Solr Port");

	// Index Text Field
	public static final ConfigProperty.Builder INDEX_NAME = new ConfigProperty.Builder().setName("hci.index.name")
			.setValue("").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Index Name")
			.setUserVisibleDescription(
					"Specify the name of the index from which the Summary Report information needs to be captured");

	// Start Date Text Field
	public static final ConfigProperty.Builder START_DATE = new ConfigProperty.Builder().setName("Start Date")
			.setValue(null).setType(PropertyType.TEXT).setRequired(false).setUserVisibleName("Start Date")
			.setUserVisibleDescription(
					"Specify the start date for the report timeframe in the format (yyyy-mm-dd). If no start date is specified, HCI will use the current Date as Start Date by default");

	// End Date Text Field
	public static final ConfigProperty.Builder END_DATE = new ConfigProperty.Builder().setName("End Date")
			.setValue(null).setType(PropertyType.TEXT).setRequired(false).setUserVisibleName("End Date")
			.setUserVisibleDescription(
					"Specify the end date for the report timeframe in the format (yyyy-mm-dd). If no end date is specified, HCI will use the current Date as End Date by default");
	// Max Rows Text Field
	public static final ConfigProperty.Builder MAX_ROWS = new ConfigProperty.Builder()

			.setName("Max Items").setValue("10000").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Max Items")
			.setUserVisibleDescription("Max items to be retrieved for Processed/Failure Items.Default is 10000");
	
	// Workflow Nmae Text Field
		public static final ConfigProperty.Builder WORKFLOW_NAME = new ConfigProperty.Builder()

				.setName("Workflow Name").setValue("ICEChatIngestWF_v1.0").setType(PropertyType.TEXT).setRequired(true)
				.setUserVisibleName("Workflow Name")
				.setUserVisibleDescription("Name of the workflow for which provides the information for the audit log. Default: ICEChatIngestWF_v1.0");

	// Check Summary
	public static final ConfigProperty.Builder CHECK_SUMMARY = new ConfigProperty.Builder().setName("hci.check.summary")
			.setType(PropertyType.CHECKBOX).setValue("true").setRequired(false).setUserVisibleName("Include Statistics")
			.setUserVisibleDescription(
					"Check this option to capture the summary of email objects moved to their corresponding namespaces");

	// Check Processed Items
	public static final ConfigProperty.Builder CHECK_PROCESSED = new ConfigProperty.Builder()
			.setName("hci.check.processed").setType(PropertyType.CHECKBOX).setValue("true").setRequired(false)
			.setUserVisibleName("Include Processed Items").setUserVisibleDescription(
					"Check this option to capture the processed items list of email objects moved to their corresponding namespaces");

	// Check Failure Items
	public static final ConfigProperty.Builder CHECK_FAILURES = new ConfigProperty.Builder()
			.setName("hci.check.failures").setType(PropertyType.CHECKBOX).setValue("true").setRequired(false)
			.setUserVisibleName("Include Failure Items")
			.setUserVisibleDescription("Check this option to capture the failure list of email objects.");

	// Write Audit Log
	public static final ConfigProperty.Builder WRITE_AUDIT_LOG = new ConfigProperty.Builder()
			.setName("hci.write.auditlog").setType(PropertyType.CHECKBOX).setValue("true").setRequired(false)
			.setUserVisibleName("Write AuditLog").setUserVisibleDescription(
					"Check this option to write the statistics and summary to a Audit log on a HCP namespace.");

	// Admin Host Name Text Field
	public static final ConfigProperty.Builder ADMIN_HOST = new ConfigProperty.Builder()
			.setName("hci.adminHost").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Admin Host")
			.setUserVisibleDescription("Full URL of the Admin HCP namespace that store the Audit log");
	
	// Enable Debug
	public static final ConfigProperty.Builder DEBUG = new ConfigProperty.Builder().setName("hci.debug")
			.setType(PropertyType.CHECKBOX).setValue("false").setRequired(false).setUserVisibleName("Debug")
			.setUserVisibleDescription(
					"Check this option to debug raw response values.Disable write Audit Log when using this option.");

	// Enable Export Mode
	public static final ConfigProperty.Builder EXPORT_MODE = new ConfigProperty.Builder().setName("hci.export.mode")
			.setType(PropertyType.CHECKBOX).setValue("false").setRequired(false).setUserVisibleName("ExportMode")
			.setUserVisibleDescription("Check this option to before exporting the Workflow.");

	private static List<ConfigProperty.Builder> solrGroupProperties = new ArrayList<>();

	static {
		solrGroupProperties.add(HOST_NAME);
		solrGroupProperties.add(PORT);
		solrGroupProperties.add(INDEX_NAME);
	}

	private static List<ConfigProperty.Builder> reportGroupProperties = new ArrayList<>();

	static {
		reportGroupProperties.add(START_DATE);
		reportGroupProperties.add(END_DATE);
		reportGroupProperties.add(MAX_ROWS);
		reportGroupProperties.add(WORKFLOW_NAME);
		reportGroupProperties.add(CHECK_SUMMARY);
		reportGroupProperties.add(CHECK_PROCESSED);
		reportGroupProperties.add(CHECK_FAILURES);
		reportGroupProperties.add(WRITE_AUDIT_LOG);
		reportGroupProperties.add(ADMIN_HOST);
		reportGroupProperties.add(DEBUG);
		reportGroupProperties.add(EXPORT_MODE);
	}

	// Solr Group Settings
	public static final ConfigPropertyGroup.Builder SOLR_SETTINGS = new ConfigPropertyGroup.Builder("Solr Settings",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(solrGroupProperties);

	// Report Group Settings
	public static final ConfigPropertyGroup.Builder REPORT_SETTINGS = new ConfigPropertyGroup.Builder("Report Settings",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(reportGroupProperties);

	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(SOLR_SETTINGS)
			.addGroup(REPORT_SETTINGS).build();

	private AuditLogReportingStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public AuditLogReportingStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public AuditLogReportingStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new AuditLogReportingStage(config, callback);
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
		String startDate;
		String endDate;
		String val;
		Date start = null;
		Date end = null;

		if (config == null) {
			throw new ConfigurationException("No configuration for AuditLogReporting Stage");
		}
		Boolean exportEnabled = Boolean.valueOf(config.getPropertyValue(EXPORT_MODE.getName()));
		if (!exportEnabled) {
			try {
				HttpClient mHttpClient = HttpClientBuilder.create().build();
				HttpPost testPostRequest = new HttpPost(getRequestURL(config.getPropertyValue(HOST_NAME.getName()),
						config.getPropertyValue(PORT.getName()), config.getPropertyValue(INDEX_NAME.getName())));
				StringEntity queryEntity = new StringEntity(TEST_QUERY);

				testPostRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
				testPostRequest.setEntity(queryEntity);

				HttpResponse httpResponse = mHttpClient.execute(testPostRequest);
				if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
					throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
							"Unexpected status returned from " + testPostRequest.getMethod() + " ("
									+ httpResponse.getStatusLine().getStatusCode() + ": "
									+ httpResponse.getStatusLine().getReasonPhrase() + ")");

				}
				EntityUtils.consume(httpResponse.getEntity());
			} catch (Exception e) {
				throw new ConfigurationException("Unable to connect to the solr index specified: " + e.getMessage());
			}
		}
		try {
			startDate = config.getPropertyValue(START_DATE.getName());
			if (startDate != null && !EMPTY.equals(startDate)) {
				start = sdf.parse(startDate);
			}
		} catch (Exception e) {
			throw new ConfigurationException("Invalid Start Date specified for property: " + START_DATE.getName());
		}
		try {
			endDate = config.getPropertyValue(END_DATE.getName());
			if (endDate != null && !EMPTY.equals(endDate)) {
				end = sdf.parse(endDate);
			}
		} catch (Exception e) {
			throw new ConfigurationException("Invalid End Date specified for property: " + END_DATE.getName());
		}

		if (end != null && end.compareTo(start) < 0) {
			throw new ConfigurationException("Start Date cannot be greater than End Date.");
		}

		try {
			val = config.getPropertyValue(MAX_ROWS.getName());
			if (val != null) {
				Integer.valueOf(val);
			}
		} catch (Exception e) {
			throw new ConfigurationException("Invalid integer value for property: " + MAX_ROWS.getName());
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

		String hostname = this.config.getPropertyValue(HOST_NAME.getName());
		String port = this.config.getPropertyValue(PORT.getName());
		String indexName = this.config.getPropertyValue(INDEX_NAME.getName());

		String startDate = this.config.getPropertyValue(START_DATE.getName());
		if (startDate == null || EMPTY.equalsIgnoreCase(startDate)) {
			startDate = sdf.format(new Date());
		}
		String endDate = this.config.getPropertyValue(END_DATE.getName());
		if (endDate == null || EMPTY.equalsIgnoreCase(endDate)) {
			endDate = sdf.format(new Date());
		}
		String maxRows = this.config.getPropertyValueOrDefault(MAX_ROWS.getName(), DEFAULT_MAX_ROWS);
		
		String workflowName = this.config.getPropertyValueOrDefault(WORKFLOW_NAME.getName(), DEFAULT_WORKFLOW_NAME);

		Boolean includeSummary = Boolean.valueOf(this.config.getPropertyValue(CHECK_SUMMARY.getName()));
		Boolean includeProcessedItems = Boolean.valueOf(this.config.getPropertyValue(CHECK_PROCESSED.getName()));
		Boolean includeFailureItems = Boolean.valueOf(this.config.getPropertyValue(CHECK_FAILURES.getName()));
		Boolean shouldWriteAuditLog = Boolean.valueOf(this.config.getPropertyValue(WRITE_AUDIT_LOG.getName()));
		Boolean debugEnabled = Boolean.valueOf(this.config.getPropertyValue(DEBUG.getName()));

		JSONParser parser = new JSONParser();
		HttpClient mHttpClient = HttpClientBuilder.create().build();
		Boolean failuresExist = false;
		StringBuilder sb = new StringBuilder();
		StringBuilder formattedFailures = new StringBuilder();
		
		StringBuilder reportHeader = new StringBuilder();
		reportHeader.append("ICE Chat Audit Log Report:");
		reportHeader.append("\n");
		reportHeader.append("==========================");
		reportHeader.append("\n");
		reportHeader.append("HCI Workflow/Job Name : ");
		reportHeader.append(workflowName);
		reportHeader.append("\n");
		reportHeader.append("Report Time Range : ");
		reportHeader.append(startDate + START_DATE_SUFFIX+" TO "+endDate + END_DATE_SUFFIX);
		reportHeader.append("\n");
		reportHeader.append("\n");
		
		docBuilder.setMetadata("reportHeader",
				StringDocumentFieldValue.builder().setString(reportHeader.toString()).build());
		

		try {
			if (includeSummary) {
				HttpPost facetPostRequest = new HttpPost(getRequestURL(hostname, port, indexName));

				String baseQueryTemplate = BASE_QUERY;
				String baseQuery = baseQueryTemplate.replace("START", startDate + START_DATE_SUFFIX).replace("END",
						endDate + END_DATE_SUFFIX);
				// Add the body of the POST request.
				if (debugEnabled) {
					docBuilder.setMetadata("baseQuery",
							StringDocumentFieldValue.builder().setString(baseQuery).build());
				}
				StringEntity queryEntity = new StringEntity(baseQuery + "&" + FACET_QUERY);

				facetPostRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
				facetPostRequest.setEntity(queryEntity);

				HttpResponse httpResponse = mHttpClient.execute(facetPostRequest);
				String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(),
						StandardCharsets.UTF_8.toString());
				if (debugEnabled) {
					docBuilder.setMetadata("jsonResponse",
							StringDocumentFieldValue.builder().setString(jsonResponseString).build());
				}
				EntityUtils.consume(httpResponse.getEntity());

				Object obj = parser.parse(jsonResponseString);
				JSONObject obj2 = (JSONObject) obj;
				JSONObject obj3 = (JSONObject) obj2.get("facets");
				if (obj3 != null) {
					JSONObject obj4 = (JSONObject) obj3.get("numread");
					if (obj4 != null) {
						JSONArray array = (JSONArray) obj4.get("buckets");
						sb.append("Statistics :");
						sb.append("\n");
						sb.append("============");
						sb.append("\n");
						for (int i = 0; i < array.size(); i++) {
							JSONObject newobj = (JSONObject) array.get(i);
							sb.append("Landing zone namespace - Total number of emails read : ");
							sb.append(newobj.get("count").toString());
							sb.append("\n");
						}

						JSONObject obj5 = (JSONObject) obj3.get("moved");
						JSONArray array2 = (JSONArray) obj5.get("buckets");
                        
						
						for (int i = 0; i < array2.size(); i++) {
							JSONObject newobj = (JSONObject) array2.get(i);
							if ("Unknown".equalsIgnoreCase(newobj.get("val").toString())) {
								sb.append("Landing zone namespace - Number of emails remaining (i.e. not mapped) : ");
								sb.append(newobj.get("count").toString());
								sb.append("\n");
								failuresExist = true;
							} 
							if ("GB".equalsIgnoreCase(newobj.get("val").toString())) {
								sb.append("GB namespace - Number of emails written : ");
								sb.append(newobj.get("count").toString());
								sb.append("\n");
							}
							if ("US".equalsIgnoreCase(newobj.get("val").toString())) {
								sb.append("US namespace - Number of emails written : ");
								sb.append(newobj.get("count").toString());
								sb.append("\n");
							}
							if ("NL".equalsIgnoreCase(newobj.get("val").toString())) {
								sb.append("NL namespace - Number of emails written : ");
								sb.append(newobj.get("count").toString());
								sb.append("\n");
							}
						}
						if (!failuresExist) {
							sb.append("Landing zone namespace - Number of emails remaining (i.e. not mapped) : ");
							sb.append("0");
							sb.append("\n");
						} 
						sb.append("\n");
					}
				}

				docBuilder.setStream(FACET_STREAM_NAME, Collections.emptyMap(), IOUtils.toInputStream(sb.toString()));
			}
			if (includeProcessedItems) {
				HttpPost processedPostRequest = new HttpPost(getRequestURL(hostname, port, indexName));

				String processedQueryTemplate = PROCESSED_QUERY;
				String processedQuery = processedQueryTemplate.replace("START", startDate + START_DATE_SUFFIX)
						.replace("END", endDate + END_DATE_SUFFIX).replace("ITEMS", maxRows);

				// Add the body of the POST request.
				StringEntity processedQueryEntity = new StringEntity(processedQuery);

				processedPostRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
				processedPostRequest.setEntity(processedQueryEntity);

				HttpResponse httpProcessedResponse = mHttpClient.execute(processedPostRequest);
				docBuilder.setStream(PROCESSED_STREAM_NAME, Collections.emptyMap(),
						httpProcessedResponse.getEntity().getContent());

				EntityUtils.consume(httpProcessedResponse.getEntity());
			}

			if (includeFailureItems && failuresExist) {
				HttpPost failurePostRequest = new HttpPost(getRequestURL(hostname, port, indexName));

				String failureQueryTemplate = FAILURE_QUERY;
				String failureQuery = failureQueryTemplate.replace("START", startDate + START_DATE_SUFFIX)
						.replace("END", endDate + END_DATE_SUFFIX).replace("ITEMS", maxRows);

				// Add the body of the POST request.
				StringEntity failureQueryEntity = new StringEntity(failureQuery);

				failurePostRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
				failurePostRequest.setEntity(failureQueryEntity);

				HttpResponse httpFailureResponse = mHttpClient.execute(failurePostRequest);
				String csvResponseString = IOUtils.toString(httpFailureResponse.getEntity().getContent(),
						StandardCharsets.UTF_8.toString());
				docBuilder.setStream(FAILURE_STREAM_NAME, Collections.emptyMap(),
						IOUtils.toInputStream(csvResponseString));

				EntityUtils.consume(httpFailureResponse.getEntity());

				String[] lines = csvResponseString.split("\n");
				for (String line : lines) {
					String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					formattedFailures.append(String.format("%-25s%-50s%s\n", cols[0], cols[1], cols[2]));
				}
			}
			
			

			if (!sb.toString().isEmpty() || !formattedFailures.toString().isEmpty()) {
				sb.append("\n");
				sb.append("Failure Details :");
				sb.append("\n");
				sb.append("=================");
				sb.append("\n");
				if (formattedFailures.toString().isEmpty()){
					sb.append("No Failure Items Found.");
					sb.append("\n");
				}				
			}
			

			String currentDateTime = sdf_time.format(new Date());
			String[] dateTokens = currentDateTime.split("-");
			String relPath = PATH_SEPERATOR + "AuditTrail" + PATH_SEPERATOR + dateTokens[0] + PATH_SEPERATOR;
			String auditLogFileName = currentDateTime + AUDIT_LOG_NAME;

			docBuilder.setMetadata(M_RELATIVE_PATH, StringDocumentFieldValue.builder().setString(relPath).build());
			docBuilder.setMetadata(M_AUDIT_LOG_NAME,
					StringDocumentFieldValue.builder().setString(auditLogFileName).build());
			if (shouldWriteAuditLog) {
				docBuilder.setMetadata(M_WRITE_AUDIT_LOG, StringDocumentFieldValue.builder().setString("true").build());
			}
			
			if (!sb.toString().isEmpty() || !formattedFailures.toString().isEmpty()) {
				if (formattedFailures.toString().length() + sb.length() > (900*1024)) {
					// If more than 0.9 MB, then provide a link because metadata cannot have more than 1 MB
					sb.append("Link to the Audit Log: " + config.getPropertyValue(ADMIN_HOST.getName()) + relPath + auditLogFileName + "\n");
				} else {
					// If less than 0.9 MB, then provide the list of failures
					sb.append(formattedFailures).toString();
				}
				docBuilder.setMetadata(M_EMAIL_METADATA,
				StringDocumentFieldValue.builder().setString(sb.toString()).build());
			}
			
			StringBuilder processedHeader = new StringBuilder();
			processedHeader.append("\n");
			processedHeader.append("Successfully Processed : ");
			processedHeader.append("\n");
			processedHeader.append("=========================");
			processedHeader.append("\n");

			docBuilder.setStream(PROCESSED_HEADER_STREAM_NAME, Collections.emptyMap(),
					IOUtils.toInputStream(processedHeader.toString()));

			StringBuilder failureHeader = new StringBuilder();
			failureHeader.append("\n");
			failureHeader.append("Failures : ");
			failureHeader.append("\n");
			failureHeader.append("=========================");
			failureHeader.append("\n");
			if (!failuresExist) {
				failureHeader.append("No Failure Items Found");
				failureHeader.append("\n");
			}

			docBuilder.setStream(FAILURE_HEADER_STREAM_NAME, Collections.emptyMap(),
					IOUtils.toInputStream(failureHeader.toString()));

		} catch (Exception e) {
			throw new PluginOperationFailedException("Encountered an Unexpected Error : ", (Throwable) e);
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

	public String getRequestURL(String host, String port, String index) {
		StringBuilder sb = new StringBuilder();
		sb.append(SOLR_SCHEME);
		sb.append(host);
		sb.append(":");
		sb.append(port);
		sb.append(PATH_SEPERATOR);
		sb.append("solr");
		sb.append(PATH_SEPERATOR);
		sb.append(index);
		sb.append(PATH_SEPERATOR);
		sb.append("select");

		return sb.toString(); 
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
