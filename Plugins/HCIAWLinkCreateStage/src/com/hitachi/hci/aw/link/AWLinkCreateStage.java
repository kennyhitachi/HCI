package com.hitachi.hci.aw.link;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyType;
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
import com.hitachi.hci.aw.utils.AWCreateLinkRequest;
import com.hitachi.hci.aw.utils.AWCreateLinkResponse;
import com.hitachi.hci.aw.utils.AWFileUploadResponse;
import com.hitachi.hci.aw.utils.AWToken;
import com.hitachi.hci.aw.utils.AWUtils;

public class AWLinkCreateStage implements StagePlugin{
	private static final String PLUGIN_NAME = "com.hitachi.hci.aw.link.aWLinkCreateStage";
	private static final String PLUGIN_DISPLAY_NAME = "HCP AW Create Link Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage uploads a file to HCP Anywhere and creates a link and access code for the parent folder and the file itself.\n ";
	private final PluginConfig config;
	private final PluginCallback callback;
	
	private static HttpClient mHttpClient = null;
	private boolean bIsInitialized;
	
	private String sAccessToken;

	private static String inputFieldName = "Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_FIELD_NAME = new ConfigProperty.Builder()
			.setName(inputFieldName).setValue("HCI_content").setRequired(true).setUserVisibleName(inputFieldName)
			.setUserVisibleDescription("Name of the stream which needs to be written to HCP Anywhere");
	
	// Text field
		public static final ConfigProperty.Builder PROPERTY_INPUT_AW_HOST = new ConfigProperty.Builder()
				.setName("Host").setValue("hcpanywhere.hds.com").setRequired(true).setUserVisibleName("HostName")
				.setUserVisibleDescription("HCP Anywhere Host Name, defaults to hcpanywhere.hds.com");

		// Text field
		public static final ConfigProperty.Builder PROPERTY_INPUT_AW_USER = new ConfigProperty.Builder()
				.setName("User").setValue("").setRequired(true).setUserVisibleName("UserName")
				.setUserVisibleDescription("HCP Anywhere User Name");

		// Text field
		public static final ConfigProperty.Builder PROPERTY_INPUT_AW_PASSWORD = new ConfigProperty.Builder()
				.setName("Password").setValue("letmein").setRequired(true).setUserVisibleName("Password")
				.setUserVisibleDescription("HCP Anywhere User Password").setType(PropertyType.PASSWORD);

		// Text field
		public static final ConfigProperty.Builder PROPERTY_INPUT_AW_REL_PATH = new ConfigProperty.Builder()
				.setName("RelPath").setValue("/hci").setRequired(true).setUserVisibleName("RelativePath")
				.setUserVisibleDescription("Relative Path on the HCP Anywhere to upload the file(s). Defaults to: /hci");


	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	static {
		groupProperties.add(PROPERTY_INPUT_FIELD_NAME);
		groupProperties.add(PROPERTY_INPUT_AW_HOST);
		groupProperties.add(PROPERTY_INPUT_AW_USER);
		groupProperties.add(PROPERTY_INPUT_AW_PASSWORD);
		groupProperties.add(PROPERTY_INPUT_AW_REL_PATH);
	}
	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private AWLinkCreateStage(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
		this.validateConfig(this.config);

	}

	public AWLinkCreateStage() {
		this.config = this.getDefaultConfig();
		this.callback = null;
	}

	public AWLinkCreateStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new AWLinkCreateStage(config, callback);
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
	
	// Initialize the httpClient and accesstoken.
		private void init() throws Exception {
			if (!bIsInitialized) {
				if (null == mHttpClient) {
					mHttpClient = AWUtils.initHttpClient();
				}
				bIsInitialized = true;
			}
		}

	public Iterator<Document> process(PluginSession session, Document document)
			throws ConfigurationException, PluginOperationFailedException {
		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(document);

		String streamName = this.config.getPropertyValueOrDefault(PROPERTY_INPUT_FIELD_NAME.getName(),
				StandardFields.CONTENT);
		InputStream inputStream = this.callback.openNamedStream(document, streamName);
		
		
		String relPath = this.config.getPropertyValue(PROPERTY_INPUT_AW_REL_PATH.getName());
		String fileName = document.getStringMetadataValue(StandardFields.FILENAME);
		
		try {
			init();
		} catch (Exception e) {
			throw new PluginOperationFailedException("Failed to initialize the RestClient"+e.getMessage());
		}
		
		if (sAccessToken == null){
			setAccessToken();
		}
		
		AWFileUploadResponse awFileResponse = uploadFile(inputStream,relPath, fileName);
		
		if (awFileResponse != null) {
			String parent = awFileResponse.getParent();
			String name = awFileResponse.getName();
			
			AWCreateLinkResponse awCreateFolderLinkResponse = createLink(parent, AWUtils.EMPTY_STRING);
			AWCreateLinkResponse awCreateFileLinkResponse = createLink(parent, name);
			
			docBuilder.setMetadata("AW_Folder_Link",
					StringDocumentFieldValue.builder().setString(awCreateFolderLinkResponse.getUrl()).build());
			docBuilder.setMetadata("AW_Folder_Access_Code",
					StringDocumentFieldValue.builder().setString(awCreateFolderLinkResponse.getAccessCode()).build());
			docBuilder.setMetadata("AW_File_Link",
					StringDocumentFieldValue.builder().setString(awCreateFileLinkResponse.getUrl()).build());
			docBuilder.setMetadata("AW_File_Access_Code",
					StringDocumentFieldValue.builder().setString(awCreateFileLinkResponse.getAccessCode()).build());
			
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

	private AWCreateLinkResponse createLink(String parent, String name) throws PluginOperationFailedException{
		ObjectMapper responseMapper = new ObjectMapper();
		ObjectMapper requestMapper = new ObjectMapper();
		responseMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		requestMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		AWCreateLinkResponse awLinkResponse = null;
		
		String path;
		if (!name.isEmpty())
		    path = parent+AWUtils.HTTP_SEPERATOR+name;
		else
			path = parent;
		
		List<String> permissions = new ArrayList<>();
		permissions.add("READ");

		AWCreateLinkRequest requestBody = new AWCreateLinkRequest(path, true, permissions);

		try {

			HttpPost httpRequest = new HttpPost(getCreateLinkURI());
			
            String authHeader = "Bearer " + sAccessToken;
			
			httpRequest.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
			httpRequest.addHeader(HttpHeaders.ACCEPT, "application/json");
			httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");

			httpRequest.addHeader("X-HCPAW-FSS-API-VERSION", "3.1.0");

			String jsonInString = requestMapper.writeValueAsString(requestBody);
			StringEntity params = new StringEntity(jsonInString);
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

			awLinkResponse = responseMapper.readValue(jsonResponseString, AWCreateLinkResponse.class);
			EntityUtils.consume(httpResponse.getEntity());
			return awLinkResponse;
		} catch (Exception e) {

			throw new PluginOperationFailedException("Failed to create a link for the path "+path);
		}

	}

	private String getCreateLinkURI() {
		return getBaseURI()+AWUtils.AW_CREATE_LINK_URI;
	}

	private AWFileUploadResponse uploadFile(InputStream inputStream, String relPath, String fileName) throws PluginOperationFailedException{
        
		AWFileUploadResponse awResponse = null;
		ObjectMapper responseMapper = new ObjectMapper();
		

		try {

			HttpPost httpRequest = new HttpPost(getFileUploadUri(relPath, fileName));
			
			String authHeader = "Bearer " + sAccessToken;
			
			httpRequest.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
			httpRequest.addHeader(HttpHeaders.ACCEPT, "application/json");
			httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream");

			httpRequest.addHeader("X-HCPAW-FSS-API-VERSION", "3.1.0");

			httpRequest.setEntity(new InputStreamEntity(inputStream, -1));

			HttpResponse httpResponse = mHttpClient.execute(httpRequest);

			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}

			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());

			awResponse = responseMapper.readValue(jsonResponseString, AWFileUploadResponse.class);
			EntityUtils.consume(httpResponse.getEntity());
			return awResponse;
		} catch (Exception e) {

			throw new PluginOperationFailedException("Failed to upload file: "+ fileName+". Error: "+e.getMessage());
		}
	}

	private String getFileUploadUri(String relPath, String fileName) {
		
		String queryParams = "?path="+relPath+AWUtils.HTTP_SEPERATOR+fileName+"&createParents=true";
		StringBuilder sb = new StringBuilder();
		sb.append(getBaseURI());
		sb.append(AWUtils.AW_FILE_UPLOAD_URI);
		sb.append(queryParams);
		
		return sb.toString();
	}

	private void setAccessToken() throws PluginOperationFailedException{
		
		ObjectMapper responseMapper = new ObjectMapper();
		
		ArrayList<NameValuePair> postParameters;
		AWToken awToken = null;
		
		String user = this.config.getPropertyValue(PROPERTY_INPUT_AW_USER.getName());
		String password = this.config.getPropertyValue(PROPERTY_INPUT_AW_PASSWORD.getName());
		
		try {

			HttpPost httpRequest = new HttpPost(getLoginUri());
			
			String encoding = Base64.getEncoder().encodeToString((user+":"+password).getBytes("UTF-8"));
			String authHeader = "Basic " + new String(encoding);
			httpRequest.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
			
			httpRequest.addHeader("Accept", "application/json");
			httpRequest.addHeader("X-HCPAW-FSS-API-VERSION", "3.1.0");
			
			postParameters = new ArrayList<NameValuePair>();
			postParameters.add(new BasicNameValuePair("grant_type", "urn:hds:oauth:negotiate-client"));

			httpRequest.setEntity(new UrlEncodedFormEntity(postParameters));

			HttpResponse httpResponse = mHttpClient.execute(httpRequest);

			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}

			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(),
					StandardCharsets.UTF_8.toString());

			awToken = responseMapper.readValue(jsonResponseString, AWToken.class);
			EntityUtils.consume(httpResponse.getEntity());
			sAccessToken = awToken.getAccess_token();
		} catch (Exception e) {

			throw new PluginOperationFailedException("Failed to get an AccessToken ", (Throwable) e);
		}
	}

	private String getLoginUri() {
		return getBaseURI()+AWUtils.AW_LOGIN_URI;
	}
	
	private String getBaseURI() {
		String hostname = this.config.getPropertyValue(PROPERTY_INPUT_AW_HOST.getName());
		StringBuilder sb = new StringBuilder();
		sb.append(AWUtils.SCHEME_SSL);
		sb.append(":");
		sb.append(AWUtils.HTTP_SEPERATOR);
		sb.append(AWUtils.HTTP_SEPERATOR);
		sb.append(hostname);
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
