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
import com.hds.hci.plugins.connector.cmis.CMISGateway;
import com.hds.hci.plugins.connector.utils.CMISUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.chemistry.opencmis.client.api.Session;

public class CMISConnectorPlugin implements ConnectorPlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.connector.custom.cmisConnectorPlugin";
	private static final String DISPLAY_NAME = "CMIS Compatible";
	private static final String DESCRIPTION = "A CMIS connector plugin to crawl repositories on CMIS compliant Applications";

	private static final String SUBCATEGORY_EXAMPLE = "Custom";

	private final PluginCallback callback;
	private final PluginConfig config;

	private static List<String> selectOptions = new ArrayList<>();

	static {
		selectOptions.add(CMISUtils.ATOMPUB);
		selectOptions.add(CMISUtils.BROWSER);
	}

	public static final ConfigProperty.Builder SELECT_BINDING = new ConfigProperty.Builder().setName("hci.binding")
			.setType(PropertyType.SELECT).setOptions(selectOptions).setValue(selectOptions.get(1)).setRequired(true)
			.setUserVisibleName("Binding Type")
			.setUserVisibleDescription("Select the appropriate binding type to connect to the CMIS repository");

	// ATOM PUB URL text field
	public static final ConfigProperty.Builder BINDING_URL = new ConfigProperty.Builder().setName("hci.url")
			.setValue("").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Binding Url")
			.setUserVisibleDescription("AtomPub or Browser Url to connect to the repository");

	// Root Directory Text Field
	public static final ConfigProperty.Builder ROOT_DIR = new ConfigProperty.Builder().setName("hci.root.dir")
			.setValue("/").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Root Directory")
			.setUserVisibleDescription(
					"Specify Root Directory to start from. Ex: ('/dir1/dir2') where '/' is the root diectory");

	// UserName Text Field
	public static final ConfigProperty.Builder USER_NAME = new ConfigProperty.Builder().setName("hci.user").setValue("")
			.setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("User Name")
			.setUserVisibleDescription("User Name");

	// Password Text Field
	public static final ConfigProperty.Builder PASSWORD = new ConfigProperty.Builder().setName("hci.password")
			.setType(PropertyType.PASSWORD).setValue("letmein").setRequired(true).setUserVisibleName("Password")
			.setUserVisibleDescription("Password");

	private static List<ConfigProperty.Builder> cmisGroupProperties = new ArrayList<>();

	static {
		cmisGroupProperties.add(SELECT_BINDING);
		cmisGroupProperties.add(BINDING_URL);
		cmisGroupProperties.add(ROOT_DIR);
		cmisGroupProperties.add(USER_NAME);
		cmisGroupProperties.add(PASSWORD);
	}

	// CMIS Group Settings
	public static final ConfigPropertyGroup.Builder CMIS_SETTINGS = new ConfigPropertyGroup.Builder("CMIS Settings",
			null).setType(PropertyGroupType.DEFAULT).setConfigProperties(cmisGroupProperties);

	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(CMIS_SETTINGS).build();

	public CMISConnectorPlugin() {
		this.callback = null;
		this.config = null;
	}

	private CMISConnectorPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.callback = callback;
		this.config = config;
	}

	// Basic Validation of fields and additional validation for PORT and ROOT
	// DIR
	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {

		if (config.getPropertyValue(ROOT_DIR.getName()) == null) {
			throw new ConfigurationException("Missing Property ROOT Directory");
		}
		if (config.getPropertyValue(SELECT_BINDING.getName()) == null
				|| config.getPropertyValue(SELECT_BINDING.getName()).isEmpty()) {
			throw new ConfigurationException("Select a Binding Property");
		}

		if (config.getPropertyValue(BINDING_URL.getName()) == null) {
			throw new ConfigurationException("Missing Binding Url.");
		}

		if (!config.getPropertyValue(ROOT_DIR.getName()).startsWith("/")) {
			throw new ConfigurationException(
					"Invalid ROOT directory specified. Must Start from the base root dir ('/').");
		}
		if (config.getPropertyValue(USER_NAME.getName()) == null) {
			throw new ConfigurationException("Missing Property User Name");
		}
		if (config.getPropertyValue(PASSWORD.getName()) == null) {
			throw new ConfigurationException("Missing Property Password");
		}
	}

	@Override
	public CMISConnectorPlugin build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		validateConfig(config);
		return new CMISConnectorPlugin(config, callback);
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

	// Get an instance of a cmis Session
	private CMISPluginSession getCMISPluginSession(PluginSession session) {
		if (!(session instanceof CMISPluginSession)) {
			throw new PluginOperationRuntimeException("PluginSession is not an instance of CMISPluginSession", null);
		}
		return (CMISPluginSession) session;
	}

	@Override
	public PluginSession startSession() throws PluginOperationFailedException, ConfigurationException {
		return new CMISPluginSession(callback, config);
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
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
		return myPluginSession.getRootDocument();
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
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
		return myPluginSession.listContainers(startDocument);
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
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
		return myPluginSession.list(startDocument);
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
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
		return myPluginSession.getMetadata(uri);
	}

	// Get the content stream for a given document.
	@Override
	public InputStream get(PluginSession session, URI uri) throws PluginOperationFailedException {
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
		return myPluginSession.get(uri);
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException, PluginOperationFailedException {
		CMISPluginSession myPluginSession = getCMISPluginSession(session);
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
			CMISPluginSession cmisSession = getCMISPluginSession(pluginSession);
			try {
				cmisSession.getRootDocument();
			} catch (Exception e) {
				throw new ConfigurationException("Invalid Root Directory Specified");
			}
		} catch (Exception ex) {
			throw new ConfigurationException(ex.getMessage(), (Throwable) ex);
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

	private class CMISPluginSession implements PluginSession {
		PluginCallback callback;
		CMISGateway cmisGateway;
		PluginConfig config;

		// Create a CMIS Plugin session.
		CMISPluginSession(PluginCallback callback, PluginConfig config)
				throws PluginOperationFailedException, ConfigurationException {

			this.config = config;
			this.callback = callback;

			String url = this.config.getPropertyValue(BINDING_URL.getName());
			String hostname = "";
			try {
				URI uri = new URI(url);
				hostname = uri.getHost();
				if (hostname != null) {
					hostname = hostname.startsWith("www.") ? hostname.substring(4) : hostname;
				}

				InetAddress.getByName(hostname);
			} catch (Exception ex) {
				throw new ConfigurationException("Unable to resolve hostname: " + hostname, (Throwable) ex);
			}
			// Initialize the CMIS Gateway.
			try {
				this.cmisGateway = new CMISGateway(url, this.config.getPropertyValue(USER_NAME.getName()),
						this.config.getPropertyValue(PASSWORD.getName()),
						this.config.getPropertyValue(SELECT_BINDING.getName()), this.callback);
				Session cmisSession = this.cmisGateway.getCMISSession();
				if (cmisSession == null) {
					throw new ConfigurationException("Unable to establish a session with the given parameters");
				}
			} catch (Exception e) {
				throw new PluginOperationFailedException(
						"Unable to connect to CMIS Repository. " + "Please verify the Binding Url", (Throwable) e);
			}

		}

		@Override
		public void close() {

		}

		// Get a Document for the root directory.
		public Document getRootDocument() throws PluginOperationFailedException {
			String rootUri = this.config.getPropertyValueOrDefault(ROOT_DIR.getName(), "/");
			try {
				Document rootDocument = this.getCMISGateway().getRootDocument(rootUri);
				if (!rootDocument.isContainer()) {
					throw new ConfigurationException("Root must be a container");
				}
				return rootDocument;
			} catch (Exception e) {
				throw new PluginOperationFailedException("Unable to browse root Directory: " + rootUri, (Throwable) e);
			}
		}

		// Implement listContainers in the REST Gateway.
		public Iterator<Document> listContainers(Document startDocument) throws PluginOperationFailedException {
			String url = startDocument.getUri();
			return this.getCMISGateway().getDocumentList(url, true);
		}

		// Implement getMetaData in the REST Gateway.
		public Document getMetadata(URI uri) throws PluginOperationFailedException {
			try {
				return this.getCMISGateway().getDocumentMetadata(uri.toString());
			} catch (Exception e) {
				throw new PluginOperationFailedException("Unable to get metadata for: " + uri, (Throwable) e);
			}
		}

		public InputStream get(URI uri) throws PluginOperationFailedException {
			try {
				return this.getCMISGateway().getContentStream(uri.toString());
			} catch (IllegalStateException | IOException e) {
				throw new PluginOperationFailedException("Unable to get content stream for: " + uri, (Throwable) e);
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
				throw new ConfigurationException("Failed to Open Content Stream for " + uriString, (Throwable) ex);
			}
		}

		// Implement list in the CMIS Gateway.
		public Iterator<Document> list(Document startDocument) throws PluginOperationFailedException {
			String url = startDocument.getUri();
			return this.getCMISGateway().getDocumentList(url, false);

		}

		public CMISGateway getCMISGateway() {
			return this.cmisGateway;
		}
	}
}
