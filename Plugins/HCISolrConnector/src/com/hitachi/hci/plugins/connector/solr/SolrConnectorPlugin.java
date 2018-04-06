/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hitachi.hci.plugins.connector.solr;

import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyGroupType;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.connector.ConnectorMode;
import com.hds.ensemble.sdk.connector.ConnectorOptionalMethod;
import com.hds.ensemble.sdk.connector.ConnectorPlugin;
import com.hds.ensemble.sdk.connector.ConnectorPluginCategory;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class SolrConnectorPlugin implements ConnectorPlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.plugins.connector.solr.solrConnectorPlugin";
	private static final String DISPLAY_NAME = "Solr Connector";
	private static final String DESCRIPTION = "A Solr connector plugin that captures all the facets as a stream in a single document that can be used for reporting purposes";

	private static final String SUBCATEGORY_EXAMPLE = "Custom";

	private final PluginCallback callback;
	private final PluginConfig config;

	// Host Name Text Field
	public static final ConfigProperty.Builder HOST_NAME = new ConfigProperty.Builder()

			.setName("hci.host").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Solr Host").setUserVisibleDescription("Host Name running the Solr Service");

	// PORT Text Field
	public static final ConfigProperty.Builder PORT = new ConfigProperty.Builder()

			.setName("hci.port").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Solr Port").setUserVisibleDescription("Solr Port");

	// Root Directory Text Field
	public static final ConfigProperty.Builder INDEX_NAME = new ConfigProperty.Builder().setName("hci.index.name")
			.setValue("/").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Index Name")
			.setUserVisibleDescription(
					"Specify the name of the index from which the facet information needs to be captured");

	private static List<ConfigProperty.Builder> solrGroupProperties = new ArrayList<>();

	static {
		solrGroupProperties.add(HOST_NAME);
		solrGroupProperties.add(PORT);
		solrGroupProperties.add(INDEX_NAME);
	}

	// Solr Group Settings
	public static final ConfigPropertyGroup.Builder SOLR_SETTINGS = new ConfigPropertyGroup.Builder("Solr Settings",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(solrGroupProperties);

	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(SOLR_SETTINGS).build();

	public SolrConnectorPlugin() {
		this.callback = null;
		this.config = null;
	}

	private SolrConnectorPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.callback = callback;
		this.config = config;
	}

	// Basic Validation of fields and additional validation for PORT and ROOT
	// DIR
	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {

		if (config.getPropertyValue(HOST_NAME.getName()) == null) {
			throw new ConfigurationException("Missing Property HOST NAME");
		}
		if (config.getPropertyValue(PORT.getName()) == null) {
			throw new ConfigurationException("Missing Property PORT");
		}
		try {
			Integer.parseInt(config.getPropertyValue(PORT.getName()));
		} catch (Exception e) {
			throw new ConfigurationException("Invalid PORT Specified.");
		}
	}

	@Override
	public SolrConnectorPlugin build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		validateConfig(config);
		return new SolrConnectorPlugin(config, callback);
	}

	@Override
	public String getName() {
		return PLUGIN_NAME;
	}

	@Override
	public String getDisplayName() {
		return DISPLAY_NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	// Get an instance of a solr Session
	private SolrPluginSession getSolrPluginSession(PluginSession session) {
		if (!(session instanceof SolrPluginSession)) {
			throw new PluginOperationRuntimeException("PluginSession is not an instance of SolrPluginSession", null);
		}
		return (SolrPluginSession) session;
	}

	@Override
	public PluginSession startSession() throws PluginOperationFailedException, ConfigurationException {
		return new SolrPluginSession(callback, config);
	}

	@Override
	public String getHost() {
		return null;
	}

	@Override
	public Integer getPort() {
		return null;
	}

	// Get Root Document.
	@Override
	public Document root(PluginSession session) throws PluginOperationFailedException {
		SolrPluginSession solrSession = this.getSolrPluginSession(session);
		String indexName = solrSession.config.getPropertyValue(INDEX_NAME.getName());
		return callback.documentBuilder().setIsContainer(true).setHasContent(false)
				.addMetadata(StandardFields.ID,
						StringDocumentFieldValue.builder().setString("solr%2f" + indexName).build())
				.addMetadata(StandardFields.URI,
						StringDocumentFieldValue.builder().setString("solr%2f" + indexName).build())
				.addMetadata(StandardFields.DISPLAY_NAME,
						StringDocumentFieldValue.builder().setString("solr%2f" + indexName).build())
				.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build()).build();

	}

	/***
	 * List documents for directories only.
	 * 
	 * E.g if /Folder is the root directory specified and the directory
	 * structure is as follows:
	 * 
	 * /Folder/SubFolder1 /Folder/Subfolder2 /file.txt
	 * 
	 * This method should return only documents for SubFolder1 and SubFolder2.
	 * 
	 * For additional information refer to the sdk documentation for
	 * ConnectorPlugin interface.
	 */
	@Override
	public Iterator<Document> listContainers(PluginSession session, Document startDocument)
			throws PluginOperationFailedException {
		// No containers
		return Collections.emptyIterator();

	}

	/***
	 * List documents for directories and files.
	 * 
	 * E.g if /Folder is the root directory specified and the directory
	 * structure is as follows:
	 * 
	 * /Folder/SubFolder1 /Folder/Subfolder2 /file.txt
	 * 
	 * This method should return documents for SubFolder1 , SubFolder2 and
	 * file.txt
	 * 
	 * For additional information refer to the sdk documentation for
	 * ConnectorPlugin interface.
	 */
	@Override
	public Iterator<Document> list(PluginSession session, Document startDocument)
			throws PluginOperationFailedException {
		LinkedList<Document> list = new LinkedList<Document>();

		list.add(getDocument(session));

		return list.iterator();
	}

	public Document getDocument(PluginSession session) {
		SolrPluginSession solrSession = this.getSolrPluginSession(session);
		String indexName = solrSession.config.getPropertyValue(INDEX_NAME.getName());

		DocumentBuilder builder = this.callback.documentBuilder();

		builder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap());
		builder.addMetadata(StandardFields.ID,
				StringDocumentFieldValue.builder().setString("solr%2f" + indexName).build());
		builder.addMetadata(StandardFields.URI,
				StringDocumentFieldValue.builder().setString("solr%2f" + indexName).build());
		builder.addMetadata(StandardFields.DISPLAY_NAME,
				StringDocumentFieldValue.builder().setString("facetResult").build());
		builder.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());
		return builder.build();
	}

	/***
	 * Get Metadata for a single entry. The entry could be a directory or a
	 * file.
	 * 
	 * E.g Get the metadata for a root directory.
	 * 
	 */

	@Override
	public Document getMetadata(PluginSession session, URI uri) throws PluginOperationFailedException {

		return getDocument(session);
	}

	// Get the content stream for a given document.
	@Override
	public InputStream get(PluginSession session, URI uri) throws PluginOperationFailedException {
		SolrPluginSession myPluginSession = getSolrPluginSession(session);
		HttpPost httpRequest = myPluginSession.postRequest;
		JSONParser parser = new JSONParser();
		HttpClient mHttpClient = HttpClientBuilder.create().build();

		/*
		 * Now execute the POST request.
		 */
		try {
			HttpResponse httpResponse = mHttpClient.execute(httpRequest);
			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());

			EntityUtils.consume(httpResponse.getEntity());

			Object obj = parser.parse(jsonResponseString);
			JSONObject obj2 = (JSONObject) obj;
			JSONObject obj3 = (JSONObject) obj2.get("facets");
			JSONObject obj4 = (JSONObject) obj3.get("files");
			
			JSONArray array = (JSONArray) obj4.get("buckets");
			// System.out.println("Facet Size : " + array.size());
			// System.out.println("Filename,FileSize(inBytes),FileCount,FileAvgSize(inBytes)");
			StringBuilder sb = new StringBuilder();
			//sb.append(jsonResponseString+"\n");
			sb.append("FileName");
			sb.append(",");
			sb.append("TotalSize");
			sb.append(",");
			sb.append("Count");
			sb.append(",");
			sb.append("AvgFileSize");
			sb.append("\n");
			for (int i = 0; i < array.size(); i++) {
				JSONObject newobj = (JSONObject) array.get(i);

				sb.append(newobj.get("val"));
				sb.append(",");
				sb.append(Double.valueOf(newobj.get("totalsize").toString()).longValue());
				sb.append(",");
				sb.append(newobj.get("count"));
				sb.append(",");
				sb.append(Double.valueOf(newobj.get("avgsize").toString()).longValue());
				sb.append("\n");
				// System.out.println(newobj.get("val") + "," +
				// Double.valueOf(newobj.get("totalsize").toString()).longValue()
				// + "," + newobj.get("count") + ","
				// +
				// Double.valueOf(newobj.get("avgsize").toString()).longValue());
			}

			InputStream inputStream = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
			return inputStream;
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException, PluginOperationFailedException {
		// No streams
		try {
			return get(session, new URI(doc.getUri()));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public ConnectorPluginCategory getCategory() {
		return ConnectorPluginCategory.CUSTOM;
	}

	@Override
	public String getSubCategory() {
		return SUBCATEGORY_EXAMPLE;
	}

	@Override
	public ConnectorMode getMode() {

		return ConnectorMode.CRAWL_LIST;
	}

	// Not implemented as this is a CRAWL Based connector.
	@Override
	public DocumentPagedResults getChanges(PluginSession pluginSession, String eventToken)
			throws ConfigurationException, PluginOperationFailedException {
		throw new PluginOperationFailedException("Operation not supported");
	}

	/***
	 * Perform additional validations like: - check if the user specified a root
	 * directory that actually exists on the Filesystem - validate username and
	 * password combo to obtain an access token. - validate if the user
	 * specified an invalid hostname.
	 */
	@Override
	public void test(PluginSession pluginSession) throws ConfigurationException, PluginOperationFailedException {

		try {
			SolrPluginSession solrSession = getSolrPluginSession(pluginSession);
			HttpPost httpRequest = solrSession.postRequest;
			HttpClient mHttpClient = HttpClientBuilder.create().build();

			HttpResponse httpResponse = mHttpClient.execute(httpRequest);
			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}

			EntityUtils.consume(httpResponse.getEntity());

		} catch (Exception ex) {
			throw new ConfigurationException("Failed To Connect to the datasource : "+ex.getMessage(), (Throwable) ex);
		}
	}

	@Override
	public boolean supports(ConnectorOptionalMethod connectorOptionalMethod) {

		boolean supports = false;
		switch (connectorOptionalMethod) {
		case ROOT:
		case LIST_CONTAINERS:
		case LIST:
			supports = true;
		default:
			break;
		}
		return supports;
	}

	private class SolrPluginSession implements PluginSession {
		PluginConfig config;
		HttpPost postRequest;

		// Create a Solr Plugin session.
		SolrPluginSession(PluginCallback callback, PluginConfig config)
				throws PluginOperationFailedException, ConfigurationException {

			this.config = config;
			String hostname = this.config.getPropertyValue(HOST_NAME.getName());
			String port = this.config.getPropertyValue(PORT.getName());
			String indexName = this.config.getPropertyValue(INDEX_NAME.getName());

			try {
				InetAddress.getByName(hostname);
			} catch (UnknownHostException ex) {
				throw new ConfigurationException("Unable to resolve hostname: " + hostname, (Throwable) ex);
			}
			// Initialize the POST Request to be used in the list method.
			try {
				this.postRequest = new HttpPost("http://" + hostname + ":" + port + "/solr/" + indexName + "/select");

				// Add the body of the POST request.
				StringEntity queryEntity = new StringEntity("q=*:*&rows=0&wt=json&json.facet={files:{type "
						+ ": terms,mincount : 1,limit : -1,numBuckets : true,field : "
						+ "HCI_displayName,sort : \"totalsize desc\",facet:{totalsize : "
						+ "\"sum(HCP_GW_Num2)\",avgsize : \"avg(HCP_GW_Num2)\"}}}");

				this.postRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
				this.postRequest.setEntity(queryEntity);

			} catch (Exception e) {
				throw new PluginOperationFailedException(
						"Unable to connect to Solr. " + "Please verify the credentials", (Throwable) e);
			}

		}

		@Override
		public void close() {

		}
	}
}
