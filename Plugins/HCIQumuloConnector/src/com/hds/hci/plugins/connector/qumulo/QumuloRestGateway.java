/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.qumulo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.IntegerDocumentFieldValue;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.hci.plugins.connector.utils.QumuloCredentials;
import com.hds.hci.plugins.connector.utils.QumuloFile;
import com.hds.hci.plugins.connector.utils.QumuloFilesResultMapper;
import com.hds.hci.plugins.connector.utils.QumuloStandardFields;
import com.hds.hci.plugins.connector.utils.QumuloToken;
import com.hds.hci.plugins.connector.utils.QumuloUtils;

public class QumuloRestGateway {

	private String sHost;
	private int iPort;
	private String sUserName;
	private String sPassword;
	private String sSsl;
	private boolean bIsInitialized;
	private String sAccessToken;
	private PluginCallback callback;

	private static HttpClient mHttpClient = null;

	// Qumulo Directory and File Identifiers.
	private static final String QUMULO_DIRECTORY = "FS_FILE_TYPE_DIRECTORY";
	private static final String QUMULO_FILE = "FS_FILE_TYPE_FILE";

	public QumuloRestGateway(String host, int port, String userName, String userPass, String ssl,
			PluginCallback callback) throws Exception {
		this.setUserName(userName);
		this.setPassword(userPass);
		this.setSsl(ssl);
		this.setPort(port);
		this.setHost(host);
		this.callback = callback;
		QumuloUtils.setHTTPSPort(port);
		init();
	}

	// Initialize the httpClient and accesstoken.
	private void init() throws Exception {
		if (!bIsInitialized) {
			if (null == mHttpClient) {
				mHttpClient = QumuloUtils.initHttpClient();
			}
			if (null == sAccessToken) {
				getAccessToken();
			}
			bIsInitialized = true;
		}
	}

	// Perform a REST GET Operation on the passed URL and return a httpResponse
	// object.
	public HttpResponse qumuloGetOperation(String containerUri) throws ClientProtocolException, IOException {

		HttpResponse httpResponse = null;
		HttpGet httpRequest = new HttpGet(containerUri);
		httpRequest.setHeader(QumuloUtils.AUTH_HEADER, "Bearer " + this.sAccessToken);
		httpResponse = mHttpClient.execute(httpRequest);

		if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
			EntityUtils.consume(httpResponse.getEntity());

			throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
					"Unexpected status returned from " + httpRequest.getMethod() + " ("
							+ httpResponse.getStatusLine().getStatusCode() + ": "
							+ httpResponse.getStatusLine().getReasonPhrase() + ")");
		}

		return httpResponse;

	}

	// Get QumuloFile Object to extract metadata.
	private QumuloFile getQumuloMetadata(String containerUri) throws PluginOperationFailedException {

		ObjectMapper responseMapper = new ObjectMapper();
		responseMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		QumuloFile qFile = null;
		try {
			HttpResponse response = this.qumuloGetOperation(containerUri);
			String jsonResponseString = IOUtils.toString(response.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());
			qFile = responseMapper.readValue(jsonResponseString, QumuloFile.class);
			EntityUtils.consume(response.getEntity());

		} catch (Exception e) {
			throw new PluginOperationFailedException("Failed to getMetadata for: " + containerUri, (Throwable) e);
		}
		return qFile;
	}

	// Get a listing of QumuloFile Objects in side a container.
	private QumuloFilesResultMapper getQumuloListing(String containerUri) throws PluginOperationFailedException {

		ObjectMapper responseMapper = new ObjectMapper();
		responseMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		QumuloFilesResultMapper resultMapper = null;
		try {
			HttpResponse response = this.qumuloGetOperation(containerUri);
			String jsonResponseString = IOUtils.toString(response.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());
			resultMapper = responseMapper.readValue(jsonResponseString, QumuloFilesResultMapper.class);
			EntityUtils.consume(response.getEntity());

		} catch (Exception e) {
			throw new PluginOperationFailedException("Failed to get Listing for: " + containerUri, (Throwable) e);
		}
		return resultMapper;
	}

	// Returns the Qumulo Files Base URL with the reference encoded.
	private String getFilesBaseURI() throws UnsupportedEncodingException {

		StringBuilder listingUriBuilder = new StringBuilder();
		listingUriBuilder.append(this.getBaseUri());
		listingUriBuilder.append(QumuloUtils.HTTP_SEPERATOR);
		listingUriBuilder.append(QumuloUtils.FILES_ENDPOINT);
		listingUriBuilder.append(QumuloUtils.HTTP_SEPERATOR);
		listingUriBuilder.append(URLEncoder.encode(QumuloUtils.HTTP_SEPERATOR, StandardCharsets.UTF_8.toString()));

		return listingUriBuilder.toString();
	}

	public String getQumuloEndpoint(String url, String endpointUri) {
		StringBuilder endpoint = new StringBuilder();
		endpoint.append(url);
		endpoint.append(endpointUri);

		return endpoint.toString();

	}

	// Get the accesstoken using the Qumulo REST session management login
	// endpoint.
	public String getAccessToken() throws PluginOperationFailedException {

		ObjectMapper responseMapper = new ObjectMapper();
		ObjectMapper requestMapper = new ObjectMapper();
		QumuloToken qumuloToken = null;

		QumuloCredentials credentials = new QumuloCredentials(this.getUserName(), this.getPassword());

		try {

			HttpPost httpRequest = new HttpPost(this.getLoginUri());

			String jsonInString = requestMapper.writeValueAsString(credentials);
			StringEntity params = new StringEntity(jsonInString);
			httpRequest.addHeader("Content-Type", "application/json");
			httpRequest.setEntity(params);

			HttpResponse httpResponse = mHttpClient.execute(httpRequest);

			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}

			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());

			qumuloToken = responseMapper.readValue(jsonResponseString, QumuloToken.class);
			EntityUtils.consume(httpResponse.getEntity());
			sAccessToken = qumuloToken.getBearer_token();
			return sAccessToken;
		} catch (Exception e) {

			throw new PluginOperationFailedException("Failed to get an AccessToken ", (Throwable) e);
		}

	}

	// Construct a Qumulo REST login Endpoint
	public String getLoginUri() {
		StringBuilder loginUri = new StringBuilder();
		loginUri.append(this.getBaseUri());
		loginUri.append(QumuloUtils.HTTP_SEPERATOR);
		loginUri.append(QumuloUtils.LOGIN_ENDPOINT);
		return loginUri.toString();
	}

	// Construct a Base REST url. The Qumulo REST url is different from a
	// standard REST URL.
	private String getBaseRestUri() {
		return this.getQumuloEndpoint(this.getBaseUri(), QumuloUtils.HTTP_REST);
	}

	public String getBaseUri() {

		StringBuilder baseUriBuilder = new StringBuilder();
		boolean useSSL = Boolean.parseBoolean(this.getSsl());
		if (useSSL) {
			baseUriBuilder.append(QumuloUtils.SCHEME_SSL);
		} else {
			baseUriBuilder.append(QumuloUtils.SCHEME);
		}
		baseUriBuilder.append("://");
		baseUriBuilder.append(this.getHost());
		baseUriBuilder.append(":");
		baseUriBuilder.append(this.getPort());

		return baseUriBuilder.toString();
	}

	/**
	 * @param url
	 * @return Document
	 * 
	 *         This method returns the HCI document metadata for a given url
	 * @throws IOException
	 * @throws PluginOperationFailedException
	 * 
	 */
	public Document getDocumentMetadata(String inUrl) throws IOException, PluginOperationFailedException {

		String infoUrl = this.getQumuloEndpoint(this.getQumuloRestURL(inUrl), QumuloUtils.INFO_ENDPOINT);

		QumuloFile qFile = this.getQumuloMetadata(infoUrl);
		if (null == qFile) {
			throw new PluginOperationFailedException("Failed to get Document Metadata for " + inUrl);
		}

		if (qFile.getPath().equals(QumuloUtils.HTTP_SEPERATOR)) {
			qFile.setName(QumuloUtils.HTTP_REST.substring(1, QumuloUtils.HTTP_REST.length()));
		}
		return getDocument(this.getBaseRestUri() + qFile.getPath(), qFile.getName(),
				QUMULO_DIRECTORY.equals(qFile.getType()), qFile);

	}

	/**
	 * @param url,container
	 *            flag
	 * @return Document Iterator
	 * 
	 *         This method returns the Document Iterator for a given container
	 *         url.
	 * 
	 * @throws PluginOperationFailedException
	 * 
	 */
	public Iterator<Document> getDocumentList(String inUri, boolean listContainers)
			throws PluginOperationFailedException {
		LinkedList<Document> documentList = new LinkedList<Document>();
		try {
			String listUri = this.getQumuloEndpoint(this.getQumuloRestURL(inUri), QumuloUtils.LISTING_ENDPOINT);
			QumuloFilesResultMapper resultMapper = this.getQumuloListing(listUri);
			if (resultMapper != null) {

				for (QumuloFile entry : resultMapper.getFiles()) {
					String path = entry.getPath();
					if (listContainers && entry.getType().equalsIgnoreCase(QUMULO_DIRECTORY)) {
						documentList
								.add(getDocument(this.getBaseRestUri() + path, entry.getName(), listContainers, entry));
					} else if (!listContainers && entry.getType().equalsIgnoreCase(QUMULO_FILE)) {
						documentList
								.add(getDocument(this.getBaseRestUri() + path, entry.getName(), listContainers, entry));
					} else if (!listContainers && entry.getType().equalsIgnoreCase(QUMULO_DIRECTORY)) {
						documentList.add(
								getDocument(this.getBaseRestUri() + path, entry.getName(), !listContainers, entry));
					}

				}
			}
			return documentList.iterator();
		} catch (Exception e) {
			throw new PluginOperationFailedException("Failed to crawl " + inUri, (Throwable) e);
		}

	}

	// Extract Qumulo File Metadata from a QumuloFile object to construct a
	// Document.
	private DocumentBuilder getMetadataFromFile(QumuloFile entry, DocumentBuilder builder) {

		builder.addMetadata(QumuloStandardFields.FILE_CHANGE_TIME,
				StringDocumentFieldValue.builder().setString(entry.getChange_time()).build());
		builder.addMetadata(QumuloStandardFields.FILE_CHILD_COUNT,
				IntegerDocumentFieldValue.builder().setInteger(entry.getChild_count()).build());
		builder.addMetadata(QumuloStandardFields.FILE_CREATE_TIME,
				StringDocumentFieldValue.builder().setString(entry.getCreation_time()).build());
		builder.addMetadata(QumuloStandardFields.FILE_GROUP,
				StringDocumentFieldValue.builder().setString(entry.getGroup()).build());
		builder.addMetadata(QumuloStandardFields.FILE_ID,
				StringDocumentFieldValue.builder().setString(entry.getId()).build());
		builder.addMetadata(QumuloStandardFields.FILE_MOD_TIME,
				StringDocumentFieldValue.builder().setString(entry.getModification_time()).build());
		builder.addMetadata(QumuloStandardFields.FILE_NAME,
				StringDocumentFieldValue.builder().setString(entry.getName()).build());
		builder.addMetadata(QumuloStandardFields.FILE_NUMBER,
				StringDocumentFieldValue.builder().setString(entry.getFile_number()).build());
		builder.addMetadata(QumuloStandardFields.FILE_OWNER,
				StringDocumentFieldValue.builder().setString(entry.getOwner()).build());
		builder.addMetadata(QumuloStandardFields.FILE_PERMISIONS,
				StringDocumentFieldValue.builder().setString(entry.getMode()).build());
		builder.addMetadata(QumuloStandardFields.FILE_REL_PATH,
				StringDocumentFieldValue.builder().setString(entry.getPath()).build());
		builder.addMetadata(QumuloStandardFields.FILE_SIZE,
				StringDocumentFieldValue.builder().setString(entry.getSize()).build());
		builder.addMetadata(QumuloStandardFields.FILE_TYPE,
				StringDocumentFieldValue.builder().setString(entry.getType()).build());
		return builder;
	}

	// Convert a URl into Qumulo Standard Rest URL.
	public String getQumuloRestURL(String url) throws UnsupportedEncodingException {

		StringBuilder restUrl = new StringBuilder();
		String relUri = url.replaceAll(this.getBaseRestUri(), "");
		if (relUri.trim().isEmpty() || QumuloUtils.HTTP_SEPERATOR.equals(relUri.trim())) {
			// encountered root
			restUrl.append(this.getFilesBaseURI());
			return restUrl.toString();
		}
		if (relUri.endsWith(QumuloUtils.HTTP_SEPERATOR)) {
			relUri = relUri.substring(0, relUri.length() - 1);
		}
		if (relUri.startsWith(QumuloUtils.HTTP_SEPERATOR)) {
			relUri = relUri.substring(1, relUri.length());
			restUrl.append(this.getFilesBaseURI());
			String encodedURI = URLEncoder.encode(relUri, StandardCharsets.UTF_8.toString());
			restUrl.append(encodedURI);
		}
		return restUrl.toString();
	}

	// Create a document with basic HCI metadata.
	private Document getDocument(String url, String name, Boolean isContainer, QumuloFile entry) throws IOException {

		DocumentBuilder builder = this.callback.documentBuilder();

		if (isContainer) {
			builder.setIsContainer(true).setHasContent(false);
		} else {
			builder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap());
		}
		builder.addMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(url).build());
		builder.addMetadata(StandardFields.URI, StringDocumentFieldValue.builder().setString(url).build());
		builder.addMetadata(StandardFields.DISPLAY_NAME, StringDocumentFieldValue.builder().setString(name).build());
		builder.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());
		builder.addMetadata(QumuloStandardFields.URL,
				StringDocumentFieldValue.builder().setString(this.getQumuloRestURL(url)).build());
		if (entry != null) {
			builder = getMetadataFromFile(entry, builder);
		}
		return builder.build();

	}

	public Document getRootDocument(String uri, boolean isContainer)
			throws IOException, PluginOperationFailedException {
		if (!uri.endsWith(QumuloUtils.HTTP_SEPERATOR)) {
			uri = this.getQumuloEndpoint(uri, QumuloUtils.HTTP_SEPERATOR);
		}
		return getDocumentMetadata(this.getQumuloEndpoint(this.getBaseRestUri(), uri));
	}

	// Get the Content of a qumulo file as a Stream , which is later set as
	// HCI_content.
	public InputStream getContentStream(String uri)
			throws IllegalStateException, IOException, PluginOperationFailedException {
		String datauri = this.getQumuloEndpoint(this.getQumuloRestURL(uri), QumuloUtils.DATA_ENDPOINT);
		HttpResponse response = this.qumuloGetOperation(datauri);
		InputStream stream = response.getEntity().getContent();
		if (stream == null) {
			// Return an empty stream
			stream = new ByteArrayInputStream(QumuloUtils.EMPTY_STRING.getBytes(StandardCharsets.UTF_8));
		}
		return stream;
	}

	private int getPort() {

		return this.iPort;
	}

	/**
	 * @return the mHost
	 */
	public String getHost() {
		return sHost;
	}

	/**
	 * @param mHost
	 *            the mHost to set
	 */
	public void setHost(String sHost) {
		this.sHost = sHost;
	}

	/**
	 * @param mPort
	 *            the mPort to set
	 */
	public void setPort(int iPort) {
		this.iPort = iPort;
	}

	/**
	 * @return the mUserName
	 */
	public String getUserName() {
		return sUserName;
	}

	/**
	 * @param mUserName
	 *            the mUserName to set
	 */
	public void setUserName(String sUserName) {
		this.sUserName = sUserName;
	}

	/**
	 * @return the mPassword
	 */
	public String getPassword() {
		return sPassword;
	}

	/**
	 * @param mPassword
	 *            the mPassword to set
	 */
	public void setPassword(String sPassword) {
		this.sPassword = sPassword;
	}

	/**
	 * @return the mSsl
	 */
	public String getSsl() {
		return sSsl;
	}

	/**
	 * @param mSsl
	 *            the mSsl to set
	 */
	public void setSsl(String sSsl) {
		this.sSsl = sSsl;
	}

	/**
	 * @return the callback
	 */
	public PluginCallback getCallback() {
		return callback;
	}

	/**
	 * @param callback
	 *            the callback to set
	 */
	public void setCallback(PluginCallback callback) {
		this.callback = callback;
	}

	/**
	 * @param mAccessToken
	 *            the mAccessToken to set
	 */
	public void setAccessToken(String mAccessToken) {
		this.sAccessToken = mAccessToken;
	}
}
