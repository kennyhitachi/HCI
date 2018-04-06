/*
 * ========================================================================
 * 
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 * 
 * ========================================================================
 */
package com.hitachi.hci.custom.database;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.google.common.collect.ImmutableList;
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
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentFieldParser;
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;

/**
 * Base class for JDBC connectors
 * 
 * Implemented generic JDBC crawl functionality, without dependencies on any
 * particular JDBC driver.
 * 
 * Extend this class to provide specific JDBC connectors for real databases.
 */
public class DatabaseConnector implements ConnectorPlugin {

	private static final String WHERE_CLAUSE_MARKER = "clause";
	private static final String DEFAULT_VERSION = "1";
	private static final String DEFAULT_BATCH_SIZE = "1000";

	private static final String PLUGIN_NAME = "OracleDatabase";
	private static final String DISPLAY_NAME = "OracleDatabase";
	private static final String LONG_DESCRIPTION = "This connector uses the Java Database Connectivity (JDBC) ojdbc7 driver to connect to Oracle databases.\n\nYou use SQL query syntax when configuring a Oracle Database data connection to specify:\n* Which database table to read from\n* Filtering criteria for limiting which database rows are extracted\n* Which columns to include with extracted database rows\n\n";

	public static final ConfigProperty PROPERTY_CONNECTION = new ConfigProperty.Builder().setName("connection")
			.setValue("").setRequired(true).setUserVisibleName("JDBC Connection")
			.setUserVisibleDescription("Connection string for connecting to JDBC data source").build();

	public static final ConfigProperty PROPERTY_BATCH_SIZE = new ConfigProperty.Builder().setName("batchSize")
			.setValue(DEFAULT_BATCH_SIZE).setRequired(false).setUserVisibleName("Query batch size")
			.setUserVisibleDescription(
					"Number of rows to request from the JDBC data source in a single SELECT statement. Specify -1 or leave empty if not batching is to be used.")
			.build();

	public static final ConfigProperty PROPERTY_USERNAME = new ConfigProperty.Builder().setName("username").setValue("")
			.setRequired(false).setUserVisibleName("User Name").setUserVisibleDescription("User name to use").build();

	public static final ConfigProperty PROPERTY_PASSWORD = new ConfigProperty.Builder().setName("password").setValue("")
			.setRequired(false).setType(PropertyType.PASSWORD).setUserVisibleName("Password")
			.setUserVisibleDescription("Password for the user").build();

	public static final ConfigProperty PROPERTY_SELECT_COLUMNS = new ConfigProperty.Builder().setName("columns")
			.setValue("").setRequired(true).setUserVisibleName("SELECT columns")
			.setUserVisibleDescription("Comma-separated list of columns to select").build();

	public static final ConfigProperty PROPERTY_SELECT_FROM = new ConfigProperty.Builder().setName("from").setValue("")
			.setRequired(true).setUserVisibleName("FROM").setUserVisibleDescription("Table to select columns from")
			.build();

	public static final ConfigProperty PROPERTY_SELECT_WHERE = new ConfigProperty.Builder().setName("where")
			.setValue("").setRequired(false).setUserVisibleName("WHERE")
			.setUserVisibleDescription("Condition to apply to the SELECT statement").build();

	public static final ConfigProperty PROPERTY_RESULT_ID = new ConfigProperty.Builder().setName("id").setValue("")
			.setRequired(true).setUserVisibleName("Primary Key")
			.setUserVisibleDescription(
					"Comma-separated list of columns representing the primary key, that uniquely identifies the row. "
							+ "The value will be used for " + StandardFields.ID + " field.")
			.build();

	public static final ConfigProperty PROPERTY_RESULT_DISPLAY_NAME = new ConfigProperty.Builder()
			.setName("displayName").setValue("").setRequired(false).setUserVisibleName("Display Name")
			.setUserVisibleDescription(
					"Comma-separated list of columns that should be used as display name for the row. "
							+ "The value will be used for " + StandardFields.DISPLAY_NAME
							+ " field. If not specified, Primary Key will be used as display name.")
			.build();

	public static final ConfigProperty PROPERTY_RESULT_VERSION = new ConfigProperty.Builder().setName("version")
			.setValue("").setRequired(false).setUserVisibleName("Version")
			.setUserVisibleDescription(
					"Comma-separated list of columns that can be used to detect when a row has changed. "
							+ "The value will be used for " + StandardFields.VERSION
							+ " field. Leave blank if no such column exists (changes to the rows will not be detected unless re-crawled)")
			.build();

	@SuppressWarnings("deprecation")
	private static final PluginConfig DEFAULT_CONFIG = ((PluginConfig.Builder) ((PluginConfig.Builder) ((PluginConfig.Builder) PluginConfig
			.builder()
			.addGroup(new ConfigPropertyGroup.Builder("JDBC Settings", null).setConfigProperties(ImmutableList.of(
					new ConfigProperty.Builder(PROPERTY_CONNECTION), new ConfigProperty.Builder(PROPERTY_USERNAME),
					new ConfigProperty.Builder(PROPERTY_PASSWORD), new ConfigProperty.Builder(PROPERTY_BATCH_SIZE)))))
							.addGroup(new ConfigPropertyGroup.Builder("Query Settings", null).setConfigProperties(
									ImmutableList.of(new ConfigProperty.Builder(PROPERTY_SELECT_COLUMNS),
											new ConfigProperty.Builder(PROPERTY_SELECT_FROM),
											new ConfigProperty.Builder(PROPERTY_SELECT_WHERE)))))
													.addGroup(new ConfigPropertyGroup.Builder("Results Settings", null)
															.setConfigProperties(ImmutableList.of(
																	new ConfigProperty.Builder(PROPERTY_RESULT_ID),
																	new ConfigProperty.Builder(
																			PROPERTY_RESULT_DISPLAY_NAME),
																	new ConfigProperty.Builder(
																			PROPERTY_RESULT_VERSION))))).build();

	protected final PluginConfig configuration;
	protected final PluginCallback callback;
	protected String connectionString;
	protected String encodedConnectionString;
	protected String columnsString;
	protected String fromString;
	protected String whereString;
	protected Set<String> idColumns;
	protected Set<String> displayNameColumns;
	protected Set<String> versionColumns;

	public DatabaseConnector() {
		configuration = null;
		callback = null;
		connectionString = null;
		encodedConnectionString = null;
		whereString = null;
		idColumns = null;
		displayNameColumns = null;
		versionColumns = null;
	}

	@SuppressWarnings("deprecation")
	protected DatabaseConnector(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		configuration = config;
		this.callback = callback;
		validateConfig(config);

		columnsString = configuration.getPropertyValue(PROPERTY_SELECT_COLUMNS);
		fromString = configuration.getPropertyValue(PROPERTY_SELECT_FROM);
		whereString = configuration.getPropertyValue(PROPERTY_SELECT_WHERE);

		connectionString = configuration.getPropertyValue(PROPERTY_CONNECTION);
		if (connectionString != null) {
			try {
				encodedConnectionString = URLEncoder.encode(connectionString, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// Ignore. Encoding is valid.
			}
		}

	}

	@Override
	public ConnectorPluginCategory getCategory() {
		return ConnectorPluginCategory.DATABASE;
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
	public boolean supports(ConnectorOptionalMethod connectorOptionalMethod) {
		switch (connectorOptionalMethod) {
		case ROOT:
		case LIST_CONTAINERS:
		case LIST:
			return true;
		default:
			return false;
		}
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
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		PluginConfig.validateConfig(getDefaultConfig(), config);

		try {
			@SuppressWarnings("deprecation")
			String batchSize = config.getPropertyValue(PROPERTY_BATCH_SIZE);

			Integer.parseInt(batchSize);

		} catch (NumberFormatException e) {
			throw new ConfigurationException("Batch size must be an integer");
		}
	}

	@Override
	public Document getMetadata(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {
		return getDocumentFromJdbcResult().build();
	}

	@Override
	public InputStream get(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {
		// No streams
		return getContentAsStream(session);
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException, PluginOperationFailedException {
		try {
			return get(session, new URI(doc.getUri()));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Document root(PluginSession session) throws ConfigurationException, PluginOperationFailedException {

		return callback.documentBuilder().setIsContainer(true).setHasContent(false)
				.addMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(connectionString).build())
				.addMetadata(StandardFields.URI,
						StringDocumentFieldValue.builder().setString(encodedConnectionString).build())
				.addMetadata(StandardFields.DISPLAY_NAME,
						StringDocumentFieldValue.builder().setString(connectionString).build())
				.addMetadata(StandardFields.VERSION,
						StringDocumentFieldValue.builder().setString(DEFAULT_VERSION).build())
				.build();
	}

	@Override
	public Iterator<Document> listContainers(PluginSession session, Document startContainer)
			throws ConfigurationException, PluginOperationFailedException {
		// No containers
		return Collections.emptyIterator();
	}

	@Override
	public Iterator<Document> list(PluginSession session, Document startContainer)
			throws ConfigurationException, PluginOperationFailedException {

		LinkedList<Document> docList = new LinkedList<Document>();

		try {

			docList.add(getDocumentFromJdbcResult().build());
			return docList.iterator();
		} catch (Exception e) {

			throw new PluginOperationRuntimeException(e.getMessage(),
					new PluginOperationFailedException(e.getMessage(), e));
		}
	}

	private ResultSet getBatch(JdbcSession jdbcSession) {
		try {
			// Construct the query using configured WHERE clause and
			// the batch WHERE predicate
			// If batching, also add sorting and limit
			StringBuilder queryBuilder = new StringBuilder(jdbcSession.getQuerySQL());

			String query = queryBuilder.toString();

			ResultSet batchResults = jdbcSession.getQueryStatement().executeQuery(query);

			return batchResults;
		} catch (SQLException e) {

			throw new PluginOperationRuntimeException(e.getMessage(),
					new PluginOperationFailedException(e.getMessage(), e));
		}
	}

	@Override
	public void test(PluginSession pluginSession) throws ConfigurationException, PluginOperationFailedException {
		validateConfig(configuration);

		JdbcSession jdbcSession = (JdbcSession) pluginSession;

		// Execute SQL query to SELECT all requested rows, and see if it
		// succeeds
		ResultSet queryResultSet = null;
		try {
			StringBuilder queryBuilder = new StringBuilder(jdbcSession.getQuerySQL());
			if (whereString != null && !whereString.isEmpty()) {
				queryBuilder.append(" WHERE ").append(whereString);
			}
			String query = queryBuilder.toString();

			queryResultSet = jdbcSession.getQueryStatement().executeQuery(query);

		} catch (SQLException e) {
			throw new ConfigurationException("Failed to connect to JDBC data source: " + e.getMessage(), e);
		} finally {
			closeJdbcResource(queryResultSet);
		}
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		return new JdbcSession();
	}

	public InputStream getContentAsStream(PluginSession session) {
		JdbcSession jdbcSession = (JdbcSession) session;
		ResultSet result = getBatch(jdbcSession);

		new StringBuilder();

		StringBuilder header = new StringBuilder();
		StringBuilder resultSet = new StringBuilder();

		try {

			// Iterate over all columns returned in JDBC result
			// and either add them as metadata (if they are not special HCI
			// fields),
			// or create special fields out of them
			// Special fields being ID, DISPLAY_NAME and VERSION. These
			// are exposed in the connector config, so that the user
			// specifies which column(s) to use for ID, DISPLAY_NAME and VERSION
			// fields
			ResultSetMetaData resultMetadata = result.getMetaData();
			for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
				header.append(resultMetadata.getColumnLabel(i));
				if (i < resultMetadata.getColumnCount())
					header.append(",");
			}
            header.append("\n");
            
			while (result.next()) {
				for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
					if (result.getObject(i) != null) {
						resultSet.append(result.getObject(i).toString());
						if (i < resultMetadata.getColumnCount())
							resultSet.append(",");
					} else {
						resultSet.append("null");
						if (i < resultMetadata.getColumnCount())
							resultSet.append(",");
					}
				}
				resultSet.append("\n");
			}
			closeJdbcResource(result);
			return new ByteArrayInputStream(
					header.append(resultSet.toString()).toString().getBytes(StandardCharsets.UTF_8));
		} catch (SQLException e) {
			closeJdbcResource(result);
		}
		return null;
	}

	/**
	 * Converts JDBC result into a Document
	 * 
	 * @param result
	 *            JDBC result (database row)
	 * @return DocumentBuilder
	 * @throws SQLException
	 */
	protected DocumentBuilder getDocumentFromJdbcResult() {
		DocumentBuilder documentBuilder = callback.documentBuilder();

		documentBuilder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap())
				.setMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(connectionString).build())
				.setMetadata(StandardFields.URI,
						StringDocumentFieldValue.builder().setString(encodedConnectionString).build())
				.setMetadata(StandardFields.DISPLAY_NAME,
						StringDocumentFieldValue.builder().setString("resultSet").build())
				.setMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());

		return documentBuilder;
	}

	/**
	 * Returns a value that can be used in a WHERE-clause.
	 * 
	 * Depending on the columnType, can quote the value, convert it, etc
	 * 
	 * @param columnValue
	 * @param columnType
	 * @return value suitable for a WHERE clause
	 */
	protected String getColumnValueForWherePredicate(String columnValue, int columnType) {

		switch (columnType) {
		case java.sql.Types.CHAR:
		case java.sql.Types.NCHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.NVARCHAR:
		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.DATE:
		case java.sql.Types.TIME:
		case java.sql.Types.TIME_WITH_TIMEZONE:
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
		case java.sql.Types.OTHER:
			return (new StringBuilder()).append("'").append(columnValue).append("'").toString();
		default:
			return columnValue;
		}
	}

	/**
	 * Adds a value from JDBC result to the Document metadata
	 * 
	 * @param docBuilder
	 *            Document to add metadata to
	 * @param result
	 *            JDBC result (row)
	 * @param resultMetadata
	 *            metadata about JDBC result
	 * @param columnIndex
	 *            column of the result to process (1-based)
	 * @throws SQLException
	 */
	protected void addJdbcMetadata(DocumentBuilder docBuilder, ResultSet result, int columnIndex) throws SQLException {
		docBuilder.setMetadata(result.getMetaData().getColumnLabel(columnIndex),
				DocumentFieldParser.recommendSafeType(result.getString(columnIndex)));
	}

	/**
	 * Returns the number of rows to request from the database in one query,
	 * while crawling.
	 * 
	 * A value of 0 or less indicates no batching.
	 * 
	 * @return query batch size
	 */
	protected int getBatchSize() {
		return 1000;
	}

	/**
	 * Returns a SQL predicate that restrics number of results returned from a
	 * SELECT statement to a batch size.
	 * 
	 * The method is used in batching mode, to get a batch of rows. Returned
	 * predicate is appended to the SELECT statement.
	 * 
	 * Default implementation returns "LIMIT batchSize".
	 * 
	 * If specific database or JDBC driver requires a different syntax, override
	 * this method in the subclass implementing specific database support.
	 * 
	 * @param batchSize
	 * @return
	 */
	protected String getJdbcDriver() {
		return "oracle.jdbc.driver.OracleDriver";
	}

	protected String getBatchSizeLimitPredicate(int batchSize) {
		return "OFFSET 0 ROWS FETCH NEXT " + batchSize + " ROWS ONLY";
	}

	/**
	 * Indicates whether the connector should page/batch its requests to the
	 * database while crawling
	 * 
	 * @return true if paging/batching is supported, false otherwise
	 */
	protected boolean isBatching() {
		return getBatchSize() > 0;
	}

	/**
	 * Returns a WHERE predicate that is used to identify the current row.
	 * 
	 * The method is used for generating document ID, to allow the specific row
	 * to be looked up at a later time.
	 * 
	 * Default implementation returns an "equals to primary key" condition:
	 * column1Name = column1Value [AND column2Name = column2Value ...]
	 * 
	 * If specific database or JDBC driver requires a different syntax, override
	 * this method in the subclass implementing specific database support.
	 * 
	 * @param result
	 *            current row
	 * @return WHERE clause to identify current row
	 * @throws SQLException
	 */
	protected String getJdbcResultMarker(ResultSet result) throws SQLException {
		StringBuilder marker = new StringBuilder();

		ResultSetMetaData metaData = result.getMetaData();
		for (String id : idColumns) {
			if (marker.length() > 0) {
				marker.append(" AND ");
			}
			marker.append(id).append('=');
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if (metaData.getColumnLabel(i).equals(id)) {
					marker.append(getColumnValueForWherePredicate(result.getString(id), metaData.getColumnType(i)));
					break;
				}
			}
		}

		return marker.toString();
	}

	/**
	 * Returns a WHERE predicate that is used to identify rows after the
	 * current.
	 * 
	 * The method is used for requesting next batch of rows, when rows are
	 * sorted on the primary key.
	 * 
	 * Default implementation returns a "greater than primary key" condition. If
	 * primary key is a single column, it returns columnName > columnValue,
	 * otherwise (column1Name, column2Name) > (column1Value, column2Value)
	 * 
	 * If specific database or JDBC driver requires a different syntax, override
	 * this method in the subclass implementing specific database support.
	 * 
	 * @param result
	 *            current row
	 * @return WHERE clause to identify rows after the current
	 * @throws SQLException
	 */
	protected String getJdbcResultBatchMarker(ResultSet result) throws SQLException {
		StringBuilder leftTuple = new StringBuilder();
		StringBuilder rightTuple = new StringBuilder();

		if (idColumns.size() > 1) {
			leftTuple.append('(');
			rightTuple.append('(');
		}

		ResultSetMetaData metaData = result.getMetaData();
		for (String id : idColumns) {
			if (leftTuple.length() > (idColumns.size() > 1 ? 1 : 0)) {
				leftTuple.append(',');
				rightTuple.append(',');
			}
			leftTuple.append(id);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if (metaData.getColumnLabel(i).equals(id)) {
					rightTuple.append(getColumnValueForWherePredicate(result.getString(id), metaData.getColumnType(i)));
					break;
				}
			}
		}

		if (idColumns.size() > 1) {
			leftTuple.append(')');
			rightTuple.append(')');
		}

		StringBuilder marker = new StringBuilder();
		marker.append(leftTuple).append(">").append(rightTuple);

		return marker.toString();
	}

	private class JdbcSession implements PluginSession {
		private Connection connection = null;
		private Statement statement = null;
		private String querySQL = null;
		private String metadataSQL = null;

		@SuppressWarnings("deprecation")
		public JdbcSession() throws PluginOperationFailedException {
			try {
				Class.forName(getJdbcDriver());
				String user = configuration.getPropertyValue(PROPERTY_USERNAME);
				String password = configuration.getPropertyValue(PROPERTY_PASSWORD);
				if (user == null || user.isEmpty()) {
					connection = DriverManager.getConnection(connectionString);
				} else {
					connection = DriverManager.getConnection(connectionString, user, password);
				}
				statement = connection.createStatement();
			} catch (ClassNotFoundException | SQLException e) {
				throw new PluginOperationFailedException("Failed to connect to JDBC. " + e.getMessage(), e);
			}

			// SQL statement to "crawl", ie list all rows, used by list() API
			StringBuilder queryStatementBuilder = new StringBuilder();
			queryStatementBuilder.append("SELECT ").append("*").append(" FROM ").append(fromString);
			querySQL = queryStatementBuilder.toString();

			// SQL statement to get one specific row, used by getMetadata(URI)
			// API
			StringBuilder metadataStatementBuilder = new StringBuilder();
			metadataStatementBuilder.append("SELECT ").append(columnsString).append(" FROM ").append(fromString)
					.append(" WHERE ").append(WHERE_CLAUSE_MARKER);
			setMetadataSQL(metadataStatementBuilder.toString());
		}

		/**
		 * Returns a SQL statement to run query for all rows
		 * 
		 * @return SQL Statement to run a query for all rows
		 */
		public String getQuerySQL() {
			return querySQL;
		}

		/**
		 * Returns a JDBC Statement to run query for all rows
		 * 
		 * The statement will be closed when the session is closed.
		 * 
		 * @return JDBC Statement to run a query for all rows
		 */
		public Statement getQueryStatement() {
			return statement;
		}

		@Override
		public void close() {
			closeJdbcResource(statement);
			closeJdbcResource(connection);
		}

		/**
		 * @return the metadataSQL
		 */
		@SuppressWarnings("unused")
		public String getMetadataSQL() {
			return metadataSQL;
		}

		/**
		 * @param metadataSQL the metadataSQL to set
		 */
		public void setMetadataSQL(String metadataSQL) {
			this.metadataSQL = metadataSQL;
		}
	}

	/**
	 * Best effort attempt to close any JDBC resource (such as ResultSet,
	 * Statement or Connection)
	 * 
	 * @param resource
	 *            to close
	 */
	private void closeJdbcResource(AutoCloseable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (Exception e) {

			}
		}
	}

	@Override
	public PluginConfig getDefaultConfig() {

		return DEFAULT_CONFIG;
	}

	@Override
	public String getDescription() {

		return LONG_DESCRIPTION;
	}

	@Override
	public String getDisplayName() {

		return DISPLAY_NAME;
	}

	@Override
	public String getName() {

		return PLUGIN_NAME;
	}

	@Override
	public ConnectorPlugin build(PluginConfig config, PluginCallback pluginCallback) throws ConfigurationException {

		return new DatabaseConnector(config, pluginCallback);
	}

	@Override
	public String getSubCategory() {

		return "Custom";
	}

}
