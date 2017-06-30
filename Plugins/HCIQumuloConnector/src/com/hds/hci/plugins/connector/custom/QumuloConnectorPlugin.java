/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.custom;

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
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.hci.plugins.connector.qumulo.QumuloRestGateway;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QumuloConnectorPlugin implements ConnectorPlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.connector.custom.qumuloConnector";
	private static final String DISPLAY_NAME = "Qumulo";
	private static final String DESCRIPTION = "A Qumulo connector plugin to crawl files on Qumulo";

	private static final String SUBCATEGORY_EXAMPLE = "Custom";

	private final PluginCallback callback;
	private final PluginConfig config;

	public static final ConfigProperty.Builder HOST_NAME = new ConfigProperty.Builder()

			.setName("hci.host").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Qumulo Host Name").setUserVisibleDescription("Qumulo Host DNS Name");

	public static final ConfigProperty.Builder PORT = new ConfigProperty.Builder()

			.setName("hci.port").setValue("").setType(PropertyType.TEXT).setRequired(true)
			.setUserVisibleName("Qumulo Port").setUserVisibleDescription("Qumulo Port");

	public static final ConfigProperty.Builder SSL = new ConfigProperty.Builder().setName("hci.ssl").setValue("true")
			.setType(PropertyType.CHECKBOX).setRequired(false).setUserVisibleName("Use SSL")
			.setUserVisibleDescription("Whether to use SSL to talk to Qumulo");

	public static final ConfigProperty.Builder ROOT_DIR = new ConfigProperty.Builder().setName("hci.root.dir")
			.setValue("/").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Root Directory")
			.setUserVisibleDescription("Specify Root Directory to start from. Ex: ('/dir1/dir2') where '/' is the root diectory");

	public static final ConfigProperty.Builder USER_NAME = new ConfigProperty.Builder().setName("hci.user").setValue("")
			.setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("User Name")
			.setUserVisibleDescription("User Name");

	public static final ConfigProperty.Builder PASSWORD = new ConfigProperty.Builder().setName("hci.password")
			.setType(PropertyType.PASSWORD).setValue("letmein").setRequired(true).setUserVisibleName("Password")
			.setUserVisibleDescription("Password");

	private static List<ConfigProperty.Builder> qumuloGroupProperties = new ArrayList<>();

	static {
		qumuloGroupProperties.add(HOST_NAME);
		qumuloGroupProperties.add(PORT);
		qumuloGroupProperties.add(SSL);
		qumuloGroupProperties.add(ROOT_DIR);
		qumuloGroupProperties.add(USER_NAME);
		qumuloGroupProperties.add(PASSWORD);
	}

	public static final ConfigPropertyGroup.Builder QUMULO_SETTINGS = new ConfigPropertyGroup.Builder("Qumulo Settings",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(qumuloGroupProperties);

	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(QUMULO_SETTINGS).build();

	public QumuloConnectorPlugin() {
		this.callback = null;
		this.config = null;
	}

	private QumuloConnectorPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.callback = callback;
		this.config = config;
	}

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
		if (config.getPropertyValue(ROOT_DIR.getName()) == null) {
			throw new ConfigurationException("Missing Property ROOT Directory");
		}
		if (!config.getPropertyValue(ROOT_DIR.getName()).startsWith("/")) {
			throw new ConfigurationException("Invalid ROOT directory specified. Must Start from the base root dir ('/').");
		}
		if (config.getPropertyValue(USER_NAME.getName()) == null) {
			throw new ConfigurationException("Missing Property User Name");
		}
		if (config.getPropertyValue(PASSWORD.getName()) == null) {
			throw new ConfigurationException("Missing Property Password");
		}
	}

	@Override
	public QumuloConnectorPlugin build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		validateConfig(config);
		return new QumuloConnectorPlugin(config, callback);
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

	private QumuloPluginSession getQumuloPluginSession(PluginSession session) {
		if (!(session instanceof QumuloPluginSession)) {
			throw new PluginOperationRuntimeException("PluginSession is not an instance of QumuloPluginSession", null);
		}
		return (QumuloPluginSession) session;
	}

	@Override
	public PluginSession startSession() throws PluginOperationFailedException {
		return new QumuloPluginSession(callback, config);
	}

	@Override
	public String getHost() {
		return this.config.getPropertyValue(HOST_NAME.getName());
	}

	@Override
	public Integer getPort() {
		return Integer.parseInt(this.config.getPropertyValue(PORT.getName()));
	}

	@Override
	public Document root(PluginSession session) throws PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.getRootDocument();
	}

	@Override
	public Iterator<Document> listContainers(PluginSession session, Document startDocument)
			throws PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.listContainers(startDocument);
	}

	@Override
	public Iterator<Document> list(PluginSession session, Document startDocument)
			throws PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.list(startDocument);
	}

	@Override
	public Document getMetadata(PluginSession session, URI uri) throws PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.getMetadata(uri);
	}

	@Override
	public InputStream get(PluginSession session, URI uri) throws PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.get(uri);
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException, PluginOperationFailedException {
		QumuloPluginSession myPluginSession = getQumuloPluginSession(session);
		return myPluginSession.openNamedStream(doc, streamName);
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

	@Override
	public DocumentPagedResults getChanges(PluginSession pluginSession, String eventToken)
			throws ConfigurationException, PluginOperationFailedException {
		throw new PluginOperationFailedException("Operation not supported");
	}

	@Override
	public void test(PluginSession pluginSession) throws ConfigurationException, PluginOperationFailedException {
		String uri = null;
		URL url;
		try {
			QumuloPluginSession qumuloSession = getQumuloPluginSession(pluginSession);
			uri = qumuloSession.getRestGateway().getBaseUri();
			url = new URL(uri);
			String hostname = url.getHost();
			try {
				InetAddress.getByName(hostname);
			} catch (UnknownHostException ex) {
				throw new ConfigurationException("Unable to resolve hostname: "+ hostname,
						(Throwable) ex);
			}

			uri = qumuloSession.getRestGateway().getLoginUri();
			url = new URL(uri);
			try {
				qumuloSession.getRestGateway().getAccessToken();
			} catch (Exception e) {
				throw new ConfigurationException("Unable to get an AccessToken" , (Throwable) e);
			}
		} catch (Exception ex) {
			throw new ConfigurationException(ex.getMessage()+" "+ uri, (Throwable) ex);
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

	private class QumuloPluginSession implements PluginSession {
		PluginCallback callback;
		QumuloRestGateway restGateway;
		PluginConfig config;

		QumuloPluginSession(PluginCallback callback, PluginConfig config) throws PluginOperationFailedException {

			this.config = config;
			this.callback = callback;
			try {
				this.restGateway = new QumuloRestGateway(this.config.getPropertyValue(HOST_NAME.getName()),
						Integer.parseInt(this.config.getPropertyValue(PORT.getName())),
						this.config.getPropertyValue(USER_NAME.getName()),
						this.config.getPropertyValue(PASSWORD.getName()), this.config.getPropertyValue(SSL.getName()),
						this.callback);
			} catch (Exception e) {
				throw new PluginOperationFailedException("Unable to establish a session with Qumulo. "
						+ "Please verify the credentials.", (Throwable) e);
			}

		}

		@Override
		public void close() {

		}

		public Document getRootDocument() throws PluginOperationFailedException {
			String rootUri = this.config.getPropertyValueOrDefault(ROOT_DIR.getName(), "/");
			try {

				return this.getRestGateway().getRootDocument(rootUri, true);
			} catch (Exception e) {
				throw new PluginOperationFailedException("Unable to browse root Directory: "+rootUri, (Throwable) e);
			}
		}

		public Iterator<Document> listContainers(Document startDocument) throws PluginOperationFailedException {
			String uri = startDocument.getUri();
			return this.getRestGateway().getDocumentList(uri, true);
		}

		public Document getMetadata(URI uri) throws PluginOperationFailedException {
			try {
				return this.getRestGateway().getDocumentMetadata(uri.toString());
			} catch (Exception e) {
				throw new PluginOperationFailedException("Unable to get metadata for: "+uri,
						(Throwable) e);
			}
		}

		public InputStream get(URI uri) throws PluginOperationFailedException {
			try {
				return new ByteArrayInputStream(this.getRestGateway().getContentAsString(uri.toString()).getBytes(StandardCharsets.UTF_8));
			} catch (IllegalStateException | IOException e) {
				throw new PluginOperationFailedException("Unable to get content stream for: "+uri,
						(Throwable) e);
			}
		}

		public InputStream openNamedStream(Document document, String streamName)
				throws ConfigurationException, PluginOperationFailedException {
			String uriString = document.getUri();
			try {

				URI uri = new URI(uriString);
				if (!document.isContainer() && streamName.equals(StandardFields.CONTENT)) {
					return this.get(uri);
				}
				return null;
			} catch (URISyntaxException ex) {
				throw new ConfigurationException("Failed to Failed to Open Content Stream for "+uriString, (Throwable) ex);
			}
		}

		public Iterator<Document> list(Document startDocument) throws PluginOperationFailedException {
			String uri = startDocument.getUri();
			return this.getRestGateway().getDocumentList(uri, false);

		}

		public QumuloRestGateway getRestGateway() {
			return restGateway;
		}
	}
}
