package com.hitachi.hci.rest.api.stage;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

import com.hds.ensemble.sdk.config.Config;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.IntegerDocumentFieldValue;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.ensemble.sdk.stage.StagePlugin;
import com.hds.ensemble.sdk.stage.StagePluginCategory;

public class RestAPIStage implements StagePlugin {

	private static final String PLUGIN_NAME = "com.hitachi.hci.rest.api.stage.restAPIStage";
	private static final String PLUGIN_DISPLAY_NAME = "REST Stage";
	private static final String PLUGIN_DESCRIPTION = "This stage performs a RESTful call based on the action selected.\n "
			+ "If the API yields a response, the response is output as a stream named Response_Stream along with http headers as metadata fields.";

	private final PluginConfig config;
	private final PluginCallback callback;

	private static final String POST = "POST";
	private static final String GET = "GET";
	private static final String PUT = "PUT";

	private static final String SCHEME_SSL = "https";
	private static final String SCHEME = "http";

	// Radio options list
	private static List<String> radioOptions = new ArrayList<>();

	static {
		radioOptions.add(POST);
	}

	static final String SPACE_CONSTANT = "%SPACE";

	public static final String STREAM_TO_PROCESS_GROUP_NAME = "Stream to Process";
	private static String inputStreamName = "Request_Stream";
	// Text field
	public static final ConfigProperty.Builder PROPERTY_INPUT_STREAM_NAME = new ConfigProperty.Builder()
			.setName(inputStreamName).setValue(inputStreamName).setRequired(false).setUserVisibleName("Stream")
			.setUserVisibleDescription(
					"Name of the json/xml stream for the REST Request. Not required for GET Requests.");

	// Radio selection
	public static final ConfigProperty.Builder PROPERTY_ACTION = new ConfigProperty.Builder().setName("Action")
			.setType(PropertyType.RADIO).setOptions(radioOptions).setValue(radioOptions.get(0)).setRequired(true)
			.setUserVisibleName("Action to perform").setUserVisibleDescription("Select the action to perform.");
	private static List<ConfigProperty.Builder> groupProperties = new ArrayList<>();

	// Host Name Text Field
	public static final ConfigProperty.Builder HOST_NAME = new ConfigProperty.Builder()

			.setName("api.host").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("API Host Name").setUserVisibleDescription("API Host DNS Name");

	// PORT Text Field
	public static final ConfigProperty.Builder PORT = new ConfigProperty.Builder()

			.setName("api.port").setValue("").setType(PropertyType.TEXT).setRequired(false)
			.setUserVisibleName("API Port").setUserVisibleDescription("API Port");

	// SSL Checkbox Field
	public static final ConfigProperty.Builder SSL = new ConfigProperty.Builder().setName("api.ssl").setValue("true")
			.setType(PropertyType.CHECKBOX).setRequired(false).setUserVisibleName("Use SSL")
			.setUserVisibleDescription("Whether to use SSL to talk to API");

	// Root Directory Text Field
	public static final ConfigProperty.Builder API_URI = new ConfigProperty.Builder().setName("api.uri").setValue("/")
			.setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Endpoint URI")
			.setUserVisibleDescription("Specify the URI starting from /");

	// UserName Text Field
	public static final ConfigProperty.Builder USER_NAME = new ConfigProperty.Builder().setName("api.user").setValue("")
			.setType(PropertyType.TEXT).setRequired(false).setUserVisibleName("User Name")
			.setUserVisibleDescription("User Name");

	// Password Text Field
	public static final ConfigProperty.Builder PASSWORD = new ConfigProperty.Builder().setName("api.password")
			.setType(PropertyType.PASSWORD).setValue("").setRequired(false).setUserVisibleName("Password")
			.setUserVisibleDescription("Password");

	static {
		groupProperties.add(PROPERTY_INPUT_STREAM_NAME);
		groupProperties.add(HOST_NAME);
		groupProperties.add(PORT);
		groupProperties.add(SSL);
		groupProperties.add(API_URI);
		groupProperties.add(USER_NAME);
		groupProperties.add(PASSWORD);
		groupProperties.add(PROPERTY_ACTION);
	}

	// Property Group
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Options", null).setConfigProperties(groupProperties)).build();

	private RestAPIStage(PluginConfig pluginConfig, PluginCallback callback) throws ConfigurationException {
		validateConfig(pluginConfig);
		this.config = pluginConfig;
		this.callback = callback;
	}

	/**
	 * This no-argument constructor is needed for SPI, and shouldn't be used
	 * elsewhere.
	 */
	public RestAPIStage() {
		config = getDefaultConfig();
		callback = null;
	}

	@Override
	public RestAPIStage build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		return new RestAPIStage(config, callback);
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
		if (config.getPropertyValue(HOST_NAME.getName()) == null) {
			throw new ConfigurationException("Missing Property HOST NAME");
		}
		if (config.getPropertyValue(PORT.getName()) != null) {
			try {
				Integer.parseInt(config.getPropertyValue(PORT.getName()));
			} catch (Exception e) {
				throw new ConfigurationException("Invalid PORT Specified.");
			}
		}
		if (config.getPropertyValue(API_URI.getName()) == null) {
			throw new ConfigurationException("Missing Property Endpoint Uri");
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

	@SuppressWarnings("unused")
	public Iterator<Document> process(PluginSession session, Document document)
			throws ConfigurationException, PluginOperationFailedException {
		DocumentBuilder docBuilder = this.callback.documentBuilder().copy(document);

		StringBuilder requestUrl = new StringBuilder();
		String streamName = this.config.getPropertyValue(PROPERTY_INPUT_STREAM_NAME.getName());
		InputStream inputStream = this.callback.openNamedStream(document, streamName);
		// BufferedInputStream bs = new BufferedInputStream(inputStream);

		String hostName = this.config.getPropertyValue(HOST_NAME.getName());
		String port = this.config.getPropertyValue(PORT.getName());
		String isSsl = this.config.getPropertyValue(SSL.getName());
		String apiUri = this.config.getPropertyValue(API_URI.getName());
		String userName = this.config.getPropertyValue(USER_NAME.getName());
		String password = this.config.getPropertyValue(PASSWORD.getName());

		boolean useSSL = Boolean.parseBoolean(isSsl);
		if (useSSL) {
			requestUrl.append(SCHEME_SSL);
		} else {
			requestUrl.append(SCHEME);
		}

		requestUrl.append("://");
		requestUrl.append(hostName);

		if (!port.isEmpty()) {
			requestUrl.append(":" + port);
		}

		// requestUrl.append(HTTP_SEPERATOR);
		requestUrl.append(apiUri);

		try {
			String requestEntity = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			HttpClient mHttpClient = HttpClientBuilder.create().build();
			// postRequest = new
			// HttpPost("https://appservertest.commonwealth.com/api/CFN.App.WebSvc.DocumentStore/api/Process/HCIDocStoreInitiate");
			HttpPost postRequest = new HttpPost(requestUrl.toString());
			StringEntity queryEntity = new StringEntity(requestEntity);

			postRequest.setHeader("Content-Type", "application/json");
			postRequest.setEntity(queryEntity);

			HttpResponse httpResponse = mHttpClient.execute(postRequest);
			docBuilder.setMetadata("responseCode", IntegerDocumentFieldValue.builder()
					.setInteger(httpResponse.getStatusLine().getStatusCode()).build());
			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {
				docBuilder.setMetadata("responseErrorPhrase", StringDocumentFieldValue.builder()
						.setString(httpResponse.getStatusLine().getReasonPhrase()).build());
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + postRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}
			EntityUtils.consume(httpResponse.getEntity());
		} catch (Exception e) {
			// throw new PluginOperationFailedException("Encountered an
			// Unexpected Error : ", (Throwable) e);
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
