/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.qumulo;

import java.io.IOException;
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
	private String mHost;
	private int mPort;
	private String mUserName;
	private String mPassword;
	private String mSsl;
	private boolean bIsInitialized;
	private String mAccessToken;
	private PluginCallback callback;

	private static final String LOGIN_ENDPOINT = "v1/session/login";
	private static final String LISTING_ENDPOINT = "/entries/";
	private static final String FILES_ENDPOINT = "v1/files";
	private static final String DATA_ENDPOINT = "/data";
	private static final String INFO_ENDPOINT = "/info/attributes";
	private static final String SCHEME_SSL = "https";
	private static final String SCHEME = "http";
	private static final String HTTP_SEPERATOR = "/";
	private static final String HTTP_REST = "/rest";

	private static HttpClient mHttpClient = null;
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

	private void init() throws Exception {
		if (!bIsInitialized) {
				if (null == mHttpClient) {
					mHttpClient = QumuloUtils.initHttpClient();
				}
				if (null == mAccessToken) {
					getAccessToken();
				}
			bIsInitialized = true;	
		}
	}

	private HttpResponse qumuloGetOperation(String containerUri) throws ClientProtocolException, IOException {

		HttpResponse httpResponse = null;
		HttpGet httpRequest = new HttpGet(containerUri);
		httpRequest.setHeader(QumuloUtils.AUTH_HEADER, "Bearer " + this.mAccessToken);
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

	private QumuloFile getQumuloMetadata(String containerUri) throws PluginOperationFailedException {

		ObjectMapper responseMapper = new ObjectMapper();
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
	
	private QumuloFilesResultMapper getQumuloListing(String containerUri) throws PluginOperationFailedException {

		ObjectMapper responseMapper = new ObjectMapper();
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

	private String getFilesBaseURI() throws UnsupportedEncodingException {

		StringBuilder listingUriBuilder = new StringBuilder();
		listingUriBuilder.append(this.getBaseUri());
		listingUriBuilder.append(HTTP_SEPERATOR);
		listingUriBuilder.append(FILES_ENDPOINT);
		listingUriBuilder.append(HTTP_SEPERATOR);
		listingUriBuilder.append(URLEncoder.encode(HTTP_SEPERATOR, StandardCharsets.UTF_8.toString()));

		return listingUriBuilder.toString();
	}

	private String getQumuloEndpoint(String url, String endpointUri) {
		StringBuilder endpoint = new StringBuilder();
		endpoint.append(url);
		endpoint.append(endpointUri);
		
		return endpoint.toString();
		
	}

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
			mAccessToken = qumuloToken.getBearer_token();
			return mAccessToken;
		} catch (Exception e) {
			
			throw new PluginOperationFailedException("Failed to get an AccessToken ", (Throwable) e);
		} 
		
	}

	public String getLoginUri() {
		StringBuilder loginUri = new StringBuilder();
		loginUri.append(this.getBaseUri());
		loginUri.append(HTTP_SEPERATOR);
		loginUri.append(LOGIN_ENDPOINT);
		return loginUri.toString();
	}

	private String getBaseRestUri() {
		return this.getQumuloEndpoint(this.getBaseUri(),HTTP_REST);
	}

	public String getBaseUri() {

		StringBuilder baseUriBuilder = new StringBuilder();
		boolean useSSL = Boolean.parseBoolean(this.getSsl());
		if (useSSL) {
			baseUriBuilder.append(SCHEME_SSL);
		} else {
			baseUriBuilder.append(SCHEME);
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
		
		
		String infoUrl = this.getQumuloEndpoint(this.getQumuloRestURL(inUrl),INFO_ENDPOINT);
		
		QumuloFile qFile = this.getQumuloMetadata(infoUrl);
		if (null == qFile) {
			throw new PluginOperationFailedException("Failed to get Document Metadata for "+inUrl);
		}
		
		if (qFile.getPath().equals(HTTP_SEPERATOR)) {
			qFile.setName(HTTP_REST.substring(1,HTTP_REST.length()));
		}
		return getDocument(this.getBaseRestUri() + qFile.getPath(), qFile.getName(),
				QUMULO_DIRECTORY.equals(qFile.getType()), qFile);

	}

	public Iterator<Document> getDocumentList(String inUri, boolean listContainers)
			throws PluginOperationFailedException {
		LinkedList<Document> documentList = new LinkedList<Document>();
		try {
			String listUri = this.getQumuloEndpoint(this.getQumuloRestURL(inUri),LISTING_ENDPOINT);
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

	private String getQumuloRestURL(String url) throws UnsupportedEncodingException {

		StringBuilder restUrl = new StringBuilder();
		String relUri = url.replaceAll(this.getBaseRestUri(), "");
		if (relUri.trim().isEmpty() || HTTP_SEPERATOR.equals(relUri.trim())) {
			// encountered root
			restUrl.append(this.getFilesBaseURI());
			return restUrl.toString();
		}
		if (relUri.endsWith(HTTP_SEPERATOR)) {
			relUri = relUri.substring(0, relUri.length() - 1);
		}
		if (relUri.startsWith(HTTP_SEPERATOR)) {
			relUri = relUri.substring(1, relUri.length());
			restUrl.append(this.getFilesBaseURI());
			String encodedURI = URLEncoder.encode(relUri, StandardCharsets.UTF_8.toString());
			restUrl.append(encodedURI);
		}
		return restUrl.toString();
	}

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
		builder.addMetadata(QumuloStandardFields.URL, StringDocumentFieldValue.builder().setString(this.getQumuloRestURL(url)).build());
		if (entry != null) {
			builder = getMetadataFromFile(entry, builder);
		}
		return builder.build();

	}

	public Document getRootDocument(String uri, boolean isContainer) throws IOException, PluginOperationFailedException {
		if (!uri.endsWith(HTTP_SEPERATOR)) {
			uri = this.getQumuloEndpoint(uri,HTTP_SEPERATOR);
		}
		return getDocumentMetadata(this.getQumuloEndpoint(this.getBaseRestUri(), uri));
	}

	public String getContentAsString(String uri) throws IllegalStateException, IOException, PluginOperationFailedException {
		String datauri = this.getQumuloEndpoint(this.getQumuloRestURL(uri),DATA_ENDPOINT);
		HttpResponse response = this.qumuloGetOperation(datauri);
		String data = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
		EntityUtils.consume(response.getEntity());
		if (null == data) {
			throw new PluginOperationFailedException("Failed to get content for the document " + uri);
		}
		return data;
	}

	private int getPort() {
		
		return this.mPort;
	}

	/**
	 * @return the mHost
	 */
	public String getHost() {
		return mHost;
	}

	/**
	 * @param mHost
	 *            the mHost to set
	 */
	public void setHost(String mHost) {
		this.mHost = mHost;
	}

	/**
	 * @param mPort
	 *            the mPort to set
	 */
	public void setPort(int mPort) {
		this.mPort = mPort;
	}

	/**
	 * @return the mUserName
	 */
	public String getUserName() {
		return mUserName;
	}

	/**
	 * @param mUserName
	 *            the mUserName to set
	 */
	public void setUserName(String mUserName) {
		this.mUserName = mUserName;
	}

	/**
	 * @return the mPassword
	 */
	public String getPassword() {
		return mPassword;
	}

	/**
	 * @param mPassword
	 *            the mPassword to set
	 */
	public void setPassword(String mPassword) {
		this.mPassword = mPassword;
	}

	/**
	 * @return the mSsl
	 */
	public String getSsl() {
		return mSsl;
	}

	/**
	 * @param mSsl
	 *            the mSsl to set
	 */
	public void setSsl(String mSsl) {
		this.mSsl = mSsl;
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
		this.mAccessToken = mAccessToken;
	}
}
