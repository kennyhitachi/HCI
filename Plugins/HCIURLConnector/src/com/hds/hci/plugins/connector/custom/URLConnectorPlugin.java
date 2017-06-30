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
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hds.hci.plugins.connector.crawler.WebCrawler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Example implementation of a ConnectorPlugin.
 *
 * This connector plugin provides examples of various configuration options for
 * new plugin development. It will generate a single example document through
 * the pipeline each time it is crawled.
 */
public class URLConnectorPlugin implements ConnectorPlugin {

	private static final String PLUGIN_NAME = "com.hds.hci.plugins.connector.custom.urlconnector";
	private static final String DISPLAY_NAME = "URL connector";
	private static final String DESCRIPTION = "An example URL connector plugin intended as a starting point for development";

	private static final String SUBCATEGORY_EXAMPLE = "Example";

	private final PluginConfig config;
	private final PluginCallback callback;

	public static final ConfigProperty.Builder PROPERTY_ONE = new ConfigProperty.Builder().setName("hci.url")
			.setValue("").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Url")
			.setUserVisibleDescription("This field is used to get the url for the connector.");

	public static final ConfigProperty.Builder PROPERTY_TWO = new ConfigProperty.Builder().setName("hci.depth")
			.setValue("").setType(PropertyType.TEXT).setRequired(true).setUserVisibleName("Depth")
			.setUserVisibleDescription("This field specifies the depth the crawler needs to crawl.");

	// Configuration Group 1
	// "Default"-type property groups allow each property to be displayed
	// as its own control in the UI
	private static List<ConfigProperty.Builder> group1Properties = new ArrayList<>();

	static {
		group1Properties.add(PROPERTY_ONE);
		group1Properties.add(PROPERTY_TWO);
	}

	public static final ConfigPropertyGroup.Builder PROPERTY_GROUP_ONE = new ConfigPropertyGroup.Builder("Options",
			null).setConfigProperties(group1Properties);

	// Default config
	// This default configuration will be returned to callers of
	// getDefaultConfig().
	public static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder().addGroup(PROPERTY_GROUP_ONE).build();

	// Default constructor for new unconfigured plugin instances (can obtain
	// default config)
	public URLConnectorPlugin() {
		this.config = null;
		this.callback = null;
	}

	// Constructor for configured plugin instances to be used in workflows
	private URLConnectorPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		this.config = config;
		this.callback = callback;
	}

	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		// This method is used to ensure that the specified configuration is
		// valid for this
		// connector, i.e. that required properties are present in the
		// configuration and no invalid
		// values are set.

		// This method handles checking for non-empty and existing required
		// properties.
		// It should typically always be called here.
		// Config.validateConfig(getDefaultConfig(), config);

		// Individual property values may be read and their values checked for
		// validity
		if (config.getPropertyValue(PROPERTY_ONE.getName()) == null) {
			throw new ConfigurationException("Missing Property Url");
		}
		if (config.getPropertyValue(PROPERTY_TWO.getName()) == null) {
			throw new ConfigurationException("Missing Property Depth");
		}
	}

	@Override
	public URLConnectorPlugin build(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		validateConfig(config);

		// This method is used as a factory to create a configured instance of
		// this connector
		return new URLConnectorPlugin(config, callback);
	}

	@Override
	public String getName() {
		// The fully qualified class name of this plugin
		return PLUGIN_NAME;
	}

	@Override
	public String getDisplayName() {
		// The name of this plugin to display to end users
		return DISPLAY_NAME;
	}

	@Override
	public String getDescription() {
		// A user-visible description string describing this plugin
		return DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		// This method is used to specify a default plugin configuration.
		// Configuration properties are defined here, including their type, how
		// they are
		// presented, and whether or not they require valid user input before
		// the connector
		// can be used.
		// This default configuration is also what will appear in the UI when a
		// new instance of your
		// plugin is created.
		return DEFAULT_CONFIG;
	}

	private MyPluginSession getMyPluginSession(PluginSession session) {
		if (!(session instanceof MyPluginSession)) {
			throw new PluginOperationRuntimeException("PluginSession is not an instance of MyPluginSession", null);
		}
		return (MyPluginSession) session;
	}

	@Override
	public PluginSession startSession() {
		return new MyPluginSession(callback, config);
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
	public Document root(PluginSession session) throws PluginOperationFailedException {
		MyPluginSession myPluginSession = getMyPluginSession(session);
		return myPluginSession.getRootDocument();
	}

	@Override
	public Iterator<Document> listContainers(PluginSession session, Document startDocument) {
		MyPluginSession myPluginSession = getMyPluginSession(session);
		return myPluginSession.listContainers(); // Only 1 container to list
	}

	@Override
	public Iterator<Document> list(PluginSession session, Document startDocument) {
		MyPluginSession myPluginSession = getMyPluginSession(session);
		return myPluginSession.list(); // Only 1 container to list
	}

	@Override
	public Document getMetadata(PluginSession session, URI uri) throws PluginOperationFailedException {
		MyPluginSession myPluginSession = getMyPluginSession(session);
		return myPluginSession.getMetadata(uri);
	}

	@Override
	public InputStream get(PluginSession session, URI uri) {
		MyPluginSession myPluginSession = getMyPluginSession(session);
		return myPluginSession.get(uri);
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException {
		MyPluginSession myPluginSession = getMyPluginSession(session);
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
		// This method defines the operating mode of the connector.
		// You may utilize the PluginConfig to have end users configure the
		// operating mode
		// of this connector. CRAWL_LIST mode, for example, indicates that the
		// crawler
		// should utilize the "list" API to obtain documents. It could
		// alternatively be
		// configured to use the "getChanges" API by specifying
		// CRAWL_GET_CHANGES here.
		return ConnectorMode.CRAWL_LIST;
	}

	@Override
	public DocumentPagedResults getChanges(PluginSession pluginSession, String eventToken)
			throws ConfigurationException, PluginOperationFailedException {
		// This plugin does not support this API, and declares so in the
		// supports() method below.
		throw new PluginOperationFailedException("Operation not supported");
	}

	@Override
	public void test(PluginSession pluginSession) throws ConfigurationException, PluginOperationFailedException {
		// Implement test operations for this plugin here and
		// throw PluginOperationFailedException if the test did not succeed.
		this.validateConfig(this.config);
		String baseUrl = this.config.getPropertyValue(PROPERTY_ONE.getName());
		try {
			URI url = new URI(baseUrl);
		} catch (Exception ex) {
			throw new ConfigurationException("Failed to connect to Base Url", (Throwable) ex);
		}
	}

	@Override
	public boolean supports(ConnectorOptionalMethod connectorOptionalMethod) {
		// Returns that methods that this plugin supports, and are allowed
		// be called by the system using this data connection.
		boolean supports = false;
		switch (connectorOptionalMethod) {
		case ROOT:
		case LIST_CONTAINERS:
		case LIST:
			// Leave out these APIs, since we throw not supported exceptions!
			// case GET_CHANGES:
			// case PUT:
			// case PUT_METADATA:
			// case DELETE:
			supports = true;
		default:
			break;
		}
		return supports;
	}

	/**
	 * @return the config
	 */
	public PluginConfig getConfig() {
		return config;
	}

	// An example plugin session object
	private class MyPluginSession implements PluginSession {
		PluginCallback callback;
		PluginConfig config;
		WebCrawler crawler;

		MyPluginSession(PluginCallback callback, PluginConfig config) {
			// Can initiate a connection to a remote server here, and utilize
			// this
			// session across API calls.
			this.callback = callback;
			this.config = config;
			this.crawler = new WebCrawler(this.config.getPropertyValue(PROPERTY_ONE.getName()),
					Integer.parseInt(this.config.getPropertyValue(PROPERTY_TWO.getName())), this.callback);

		}

		@Override
		public void close() {
			// Can shutdown the connection to remote servers and free resources
			// here.
		}

		// For example purposes, return a root Document
		public Document getRootDocument() throws PluginOperationFailedException {
			try {
				return this.crawler.getRootDocument(this.config.getPropertyValue(PROPERTY_ONE.getName()), "root");
			} catch (IOException e) {
				throw new PluginOperationFailedException("Unable to get Root Document");
			}
		}

		// For example purposes, return a container Document
		public Iterator<Document> listContainers() {

			return new StreamingDocumentIterator() {

				// Computes the next Document to return to the processing
				// pipeline.
				// When there are no more documents to return, returns
				// endOfDocuments().

				@Override
				protected Document getNextDocument() {

					// No containers other than the root() in this example
					return endOfDocuments();
				}
			};
		}

		// For example purposes, get metadata for an example Document
		public Document getMetadata(URI uri) throws PluginOperationFailedException {
			return this.crawler.getDocumentMetadata(uri.toString());
		}

		// For example purposes, open an example Document stream
		public InputStream get(URI uri) {
			// Open this documents content stream from the data source.
			// For example purposes, read a fake stream here.
			return new ByteArrayInputStream(
					this.crawler.getContentAsString(uri.toString()).getBytes(StandardCharsets.UTF_8));
		}

		// For example purposes, open an example Document stream
		public InputStream openNamedStream(Document document, String streamName) throws ConfigurationException {
			// Open the stream from the data source.
			// For example purposes, read a fake stream here.
			String uriString = document.getUri();
			try {

				URI uri = new URI(uriString);
				if (streamName.equals(StandardFields.CONTENT)) {
					return this.get(uri);
				}
				return null;
			} catch (URISyntaxException ex) {
				throw new ConfigurationException("Failed to parse document URI", (Throwable) ex);
			}
		}

		// For example purposes, list all example documents
		public Iterator<Document> list() {
			this.crawler.crawl();
			final List<Document> documents = this.crawler.getDocumentList();

			return new StreamingDocumentIterator() {
				final Iterator<Document> docIter = documents.iterator();

				// Computes the next Document to return.
				// When there are no more documents to return, returns
				// endOfDocuments().
				@Override
				protected Document getNextDocument() {
					if (docIter.hasNext()) {
						return docIter.next();
					}
					return endOfDocuments();
				}
			};
		}
	}
}