package com.hitachi.hci.cfn.sql.connector;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
//import org.slf4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.hds.commons.strings.Splitters;
//import com.hds.ensemble.logging.HdsLogger;
//import com.hds.ensemble.plugins.SystemCategories;
import com.hds.ensemble.sdk.action.Action;
import com.hds.ensemble.sdk.action.ActionType;
import com.hds.ensemble.sdk.action.DocumentActionProcessor;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.connector.ConnectorMode;
import com.hds.ensemble.sdk.connector.ConnectorOptionalMethod;
import com.hds.ensemble.sdk.connector.ConnectorPlugin;
import com.hds.ensemble.sdk.connector.ConnectorPluginCategory;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.DocumentNotFoundException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentFieldParser;
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.model.DocumentUtil;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;

public class CFNSqlServerConnector implements ConnectorPlugin, DocumentActionProcessor {

	//private static final Logger log = HdsLogger.getLogger();
	private static final String DEFAULT_BATCH_SIZE = "1000";
	protected static final String DEFAULT_VERSION = "1";
	protected static final String ID_MARKER = "#" + StandardFields.ID + "=";
	private static final String PLUGIN_NAME = "CFNSQLServerConnector";
	private static final String DISPLAY_NAME = "CFNSQLServerConnector";
	private static final String DESCRIPTION = "Connector for accessing SQLServer through JDBC.";
	private static final String LONG_DESCRIPTION = "This connector uses the Java Database Connectivity (JDBC) 6.2.1 version driver to "
			+ "connect to SQL Server databases.\n" + "\nYou use SQL query syntax when configuring a " + DISPLAY_NAME
			+ " data connection to specify:" + "\n* Which database table to read from"
			+ "\n* Filtering criteria for limiting which database rows are extracted"
			+ "\n* Which columns to include with extracted database rows" + "\n" + "\n";
	private static final String INSERT_ACTION_NAME = "Insert";

	public static final ConfigProperty PROPERTY_CONNECTION = new ConfigProperty.Builder().setName("connection")
			.setValue("").setRequired(true).setUserVisibleName("JDBC Connection")
			.setUserVisibleDescription("Connection string for connecting to JDBC data source").build();

	public static final ConfigProperty PROPERTY_BATCH_SIZE = new ConfigProperty.Builder().setName("batchSize")
			.setValue(DEFAULT_BATCH_SIZE).setRequired(false).setUserVisibleName("Query batch size")
			.setUserVisibleDescription(
					"Number of rows to request from the JDBC data source in a single SELECT statement. Specify -1 or leave empty if not batching is to be used.")
			.build();
	
	public static final ConfigProperty PROPERTY_ATTACHMENT_FIELD_NAME = new ConfigProperty.Builder().setName("attachmentField")
			.setValue("").setRequired(false).setUserVisibleName("Attachment field name")
			.setUserVisibleDescription("Filed name for attachment").build();

	public static final ConfigProperty PROPERTY_PARTITION = new ConfigProperty.Builder().setName("partition")
			.setValue("").setRequired(false).setUserVisibleName("Stored Procedure Partition")
			.setUserVisibleDescription("Partition Number for stored procedure").build();

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

	public static final ConfigProperty PROPERTY_RESULT_ID_QUALIFIER = new ConfigProperty.Builder()
			.setName("idQualifier").setValue("").setRequired(false).setUserVisibleName("Primary Key Qualifier")
			.setUserVisibleDescription(
					"Qualifier for primary key.  Use when doing joins to specify which table the key needs to be pulled from.")
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

	public static final ConfigProperty PROPERTY_STORED_PROCEDURE_NAME = new ConfigProperty.Builder()
			.setName("procedureName").setValue("").setRequired(false).setUserVisibleName("Prodcedure Name")
			.setUserVisibleDescription("Name of stored procedure to call").build();

	public static final ConfigProperty PROPERTY_INSERT_PROCEDURE = new ConfigProperty.Builder()
			.setName("insertProcedure").setValue("").setRequired(true).setUserVisibleName("Insert procedure")
			.setUserVisibleDescription("Insert procedure name").build();

	public static final ConfigProperty PROPERTY_INSERT_PROCEDURE_VALUES = new ConfigProperty.Builder()
			.setName("insertProcedureValues").setValue("").setRequired(true)
			.setUserVisibleName("Insert procedure values")
			.setUserVisibleDescription("Comma seperated list of fields to pass into insert procedure").build();

	@SuppressWarnings("deprecation")
	private static final PluginConfig INSERT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Insert Settings", null)
					.setConfigProperties(ImmutableList.of(new ConfigProperty.Builder(PROPERTY_INSERT_PROCEDURE),
							new ConfigProperty.Builder(PROPERTY_INSERT_PROCEDURE_VALUES))))
			.build();

	private static final Action INSERT_ACTION = Action.builder().name(INSERT_ACTION_NAME)
			.description("Performs an update on the SQL Database based on the query specified by the user")
			.config(INSERT_CONFIG).available(true).types(EnumSet.of(ActionType.OUTPUT, ActionType.STAGE)).build();

	@SuppressWarnings("deprecation")
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("JDBC Settings", null).setConfigProperties(ImmutableList.of(
					new ConfigProperty.Builder(PROPERTY_CONNECTION), new ConfigProperty.Builder(PROPERTY_USERNAME),
					new ConfigProperty.Builder(PROPERTY_PASSWORD), new ConfigProperty.Builder(PROPERTY_BATCH_SIZE))))
			.addGroup(new ConfigPropertyGroup.Builder("Query Settings", null)
					.setConfigProperties(ImmutableList.of(new ConfigProperty.Builder(PROPERTY_SELECT_COLUMNS),
							new ConfigProperty.Builder(PROPERTY_SELECT_FROM),
							new ConfigProperty.Builder(PROPERTY_SELECT_WHERE),
							new ConfigProperty.Builder(PROPERTY_ATTACHMENT_FIELD_NAME))))
			.addGroup(new ConfigPropertyGroup.Builder("Stored Procedure Settings", null)
					.setConfigProperties(ImmutableList.of(new ConfigProperty.Builder(PROPERTY_STORED_PROCEDURE_NAME),
							new ConfigProperty.Builder(PROPERTY_PARTITION))))
			.addGroup(new ConfigPropertyGroup.Builder("Results Settings", null)
					.setConfigProperties(ImmutableList.of(new ConfigProperty.Builder(PROPERTY_RESULT_ID),
							new ConfigProperty.Builder(PROPERTY_RESULT_ID_QUALIFIER),
							new ConfigProperty.Builder(PROPERTY_RESULT_DISPLAY_NAME),
							new ConfigProperty.Builder(PROPERTY_RESULT_VERSION))))
			.build();

	protected final PluginConfig configuration;
	protected final PluginCallback callback;
	protected String connectionString;
	protected String encodedConnectionString;
	protected String columnsString;
	protected String attachmentFieldString;
	protected String fromString;
	protected String whereString;
	protected String procedureName;
	protected Set<String> idColumns;
	protected Set<String> displayNameColumns;
	protected Set<String> versionColumns;

	public CFNSqlServerConnector() {
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
	private CFNSqlServerConnector(PluginConfig config, PluginCallback callback)
			throws ConfigurationException {
		configuration = config;
		this.callback = callback;
		validateConfig(config);

		columnsString = configuration.getPropertyValue(PROPERTY_SELECT_COLUMNS);
		attachmentFieldString = configuration.getPropertyValue(PROPERTY_ATTACHMENT_FIELD_NAME);
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
		String idString = configuration.getPropertyValue(PROPERTY_RESULT_ID);
		String displayNameString = configuration.getPropertyValue(PROPERTY_RESULT_DISPLAY_NAME);
		String versionString = configuration.getPropertyValue(PROPERTY_RESULT_VERSION);
		procedureName = configuration.getPropertyValue(PROPERTY_STORED_PROCEDURE_NAME);

		idColumns = Sets.newHashSet(Splitters.csv().split(Strings.nullToEmpty(idString)));
		displayNameColumns = Strings.isNullOrEmpty(displayNameString) ? Sets.newHashSet(idColumns)
				: Sets.newHashSet(Splitters.csv().split(displayNameString));
		versionColumns = Sets.newHashSet(Splitters.csv().split(Strings.nullToEmpty(versionString)));
	}

	private String getJdbcDriver() {
		return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	}

	@Override
	public ConnectorPlugin build(PluginConfig config, PluginCallback pluginCallback) throws ConfigurationException {
		return new CFNSqlServerConnector(config, pluginCallback);
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

	public String getLongDescription() {
		return LONG_DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	@Override
	public ConnectorPluginCategory getCategory() {
		return ConnectorPluginCategory.DATABASE;
	}

	@Override
	public String getSubCategory() {
		return ConnectorPluginCategory.CUSTOM.name();
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
			if (!Strings.isNullOrEmpty(batchSize)) {
				Integer.parseInt(batchSize);
			}
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Batch size must be an integer");
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public Document getMetadata(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {

		// If this is the root container, return root and don't bother with jdbc queries
		if (uri.toString().equals(encodedConnectionString)) {
			return root(session);
		}

		JdbcSession jdbcSession = (JdbcSession) session;

		// Parse the WHERE clause to get the specific object out of URI
		String uriString = uri.toString();
		String idClause = uriString.substring(uriString.indexOf(ID_MARKER) + ID_MARKER.length());
		try {
			idClause = URLDecoder.decode(idClause, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// Ignore. Encoding is valid.
		}

		// Inject the WHERE clause identifying the object into the SQL query
		// String statement = jdbcSession.getMetadataSQL().replace(WHERE_CLAUSE_MARKER,
		// idClause);
		String statement = jdbcSession.getMetadataSQL();
		if (configuration.getPropertyValue(PROPERTY_RESULT_ID_QUALIFIER).isEmpty()) {
			statement = statement + " and " + idClause;
		} else {
			statement = statement + " and " + configuration.getPropertyValue(PROPERTY_RESULT_ID_QUALIFIER) + "."
					+ idClause;
		}

		ResultSet results = null;
		Statement metadataJdbcStatement = null;
		try {
			// Execute SQL query to get the requested row and create a Document out of it
			metadataJdbcStatement = jdbcSession.getMetadataStatement();
			results = metadataJdbcStatement.executeQuery(statement);
			if (!results.next()) {
				//log.debug("JDBC query for row {} returned no results" + idClause);
				throw new DocumentNotFoundException("Could not find a document with ID " + idClause);
			}
			return getDocumentFromJdbcResult(results).build();
		} catch (SQLException e) {
			//log.warn("JDBC query failed for row {}" + idClause + " " + e);
			throw new PluginOperationFailedException(e.getMessage(), e);
		} finally {
			closeJdbcResource(results);
			closeJdbcResource(metadataJdbcStatement);
		}
	}

	@Override
	public InputStream get(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {
		return getContentAsStream(session, uri);
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName) {
		try {
			return get(session, new URI(doc.getUri()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("deprecation")
	public InputStream getContentAsStream(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {

		JdbcSession jdbcSession = (JdbcSession) session;

		// Parse the WHERE clause to get the specific object out of URI
		String uriString = uri.toString();
		String idClause = uriString.substring(uriString.indexOf(ID_MARKER) + ID_MARKER.length());
		try {
			idClause = URLDecoder.decode(idClause, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// Ignore. Encoding is valid.
		}

		// Inject the WHERE clause identifying the object into the SQL query
		// String statement = jdbcSession.getMetadataSQL().replace(WHERE_CLAUSE_MARKER,
		// idClause);
		// SQL statement to get one specific row, used by getMetadata(URI) API
		StringBuilder metadataStatementBuilder = new StringBuilder();
		metadataStatementBuilder.append("SELECT ").append(attachmentFieldString).append(" FROM ").append(fromString);
		if (configuration.getPropertyValue(PROPERTY_RESULT_ID_QUALIFIER).isEmpty()) {
			metadataStatementBuilder.append(" WHERE ").append(idClause);
		} else {
			metadataStatementBuilder.append(" WHERE ").append(configuration.getPropertyValue(PROPERTY_RESULT_ID_QUALIFIER)+"."+idClause);
		}
				
		String statement = metadataStatementBuilder.toString();
//		statement = statement + " and " + idClause;

		ResultSet results = null;
		Statement metadataJdbcStatement = null;
		try {
			// Execute SQL query to get the requested row and create a Document out of it
			metadataJdbcStatement = jdbcSession.getMetadataStatement();
			results = metadataJdbcStatement.executeQuery(statement);
			if (!results.next()) {
				//log.debug("JDBC query for row {} returned no results " + idClause);
				throw new DocumentNotFoundException("Could not find a document with ID " + idClause);
			}
			ResultSetMetaData resultMetadata = results.getMetaData();
			for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
				// get the column type and if varbinary get bytes.
				// convert bytes to inputstream and set stream in the documentBuilder
				// implement get and openNamedStream
				int columnType = resultMetadata.getColumnType(i);
				if (columnType == Types.VARBINARY) {
					// byte [] decodedBytes = Base64.getMimeDecoder().decode(results.getBytes(i));
					byte[] decodedBytes = results.getBytes(i);
					if (Base64.isBase64(decodedBytes)) {
						decodedBytes = Base64.decodeBase64(decodedBytes);
					}
					return new ByteArrayInputStream(decodedBytes);
				} else if (columnType == Types.NVARCHAR) {
					return new ByteArrayInputStream(results.getString(i).getBytes());
				}
			}
			return null;
		} catch (SQLException e) {
			//log.warn("JDBC query failed for row {}" + idClause + " " + e);
			throw new PluginOperationFailedException(e.getMessage(), e);
		} finally {
			closeJdbcResource(results);
			closeJdbcResource(metadataJdbcStatement);
		}
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

		JdbcSession jdbcSession = (JdbcSession) session;
		

		return new StreamingDocumentIterator() {
			ResultSet results;
			int offSet = 0;

			@SuppressWarnings("deprecation")
			@Override
			protected Document getNextDocument() {
				try {
					if (results == null) {
						//log.debug("Requesting initial batch");
						//offSet = offSet + Integer.valueOf(configuration.getPropertyValue(PROPERTY_BATCH_SIZE));
						results = getBatch(offSet);
					}

					if (!results.next()) {
						closeJdbcResource(results);
						//log.debug("Exhausted all results in a batch, requesting next batch");
						offSet = offSet + Integer.valueOf(configuration.getPropertyValue(PROPERTY_BATCH_SIZE));
						results = getBatch(offSet);
						if (!results.next()) {
							//log.debug("Next batch is empty, we are done");
							closeJdbcResource(results);
							return endOfDocuments();
						}
					}
					return getDocumentFromJdbcResult(results).build();
				} catch (SQLException | PluginOperationFailedException e) {
					//log.warn("Failed to convert JDBC result into a Document", e);
					closeJdbcResource(results);
					throw new PluginOperationRuntimeException(e.getMessage(),
							new PluginOperationFailedException(e.getMessage(), e));
				}
			}

			@SuppressWarnings("deprecation")
			private ResultSet getBatch(int offSet) throws PluginOperationFailedException {
				CallableStatement cstmt = null;
				try {
					// Call stored procedure to get batch
					String callSyntax = "{call " + procedureName + "(?,?,?)}";
					// for (int i=0; i < procedureValues.size(); i++) {
					// callSyntax = callSyntax + "?, ";
					// }
					// callSyntax = callSyntax + ")}";
					cstmt = jdbcSession.getCallableStatement(callSyntax);
					// int valueIndex = 1;
					// for (String value: procedureValues) {
					// cstmt.setInt(valueIndex, Integer.valueOf(value));
					// valueIndex ++;
					// }
					cstmt.setInt(1, Integer.valueOf(configuration.getPropertyValue(PROPERTY_BATCH_SIZE)));
					cstmt.setInt(2, Integer.valueOf(configuration.getPropertyValue(PROPERTY_PARTITION)));
					cstmt.setInt(3, offSet);

					return cstmt.executeQuery();
				} catch (SQLException e) {
					//log.warn("JDBC query failed", e);
					throw new PluginOperationFailedException(e.getMessage(), e);
					// } finally {
					// closeJdbcResource(cstmt);
				}
			}
		};
	}

	@Override
	public void test(PluginSession pluginSession) throws ConfigurationException, PluginOperationFailedException {
		validateConfig(configuration);

		JdbcSession jdbcSession = (JdbcSession) pluginSession;

		// Execute SQL query to SELECT all requested rows, and see if it succeeds
		ResultSet queryResultSet = null;
		try {
			StringBuilder queryBuilder = new StringBuilder(jdbcSession.getQuerySQL());
			if (!Strings.isNullOrEmpty(whereString)) {
				queryBuilder.append(" WHERE ").append(whereString);
			}
			String query = queryBuilder.toString();
			//log.debug("Querying JDBC. Query: {}", query);
			queryResultSet = jdbcSession.getQueryStatement().executeQuery(query);
			//log.debug("Successfully queried JDBC");

			// Verify that all required columns (id, displayName, version)
			// are actually present in the result
			Set<String> requiredColumns = Sets.newHashSet();
			requiredColumns.addAll(idColumns);
			requiredColumns.addAll(displayNameColumns);
			requiredColumns.addAll(versionColumns);
			ResultSetMetaData resultSetMetaData = queryResultSet.getMetaData();
			for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
				requiredColumns.remove(resultSetMetaData.getColumnLabel(i));
			}
			if (!requiredColumns.isEmpty()) {
				throw new ConfigurationException(
						"JDBC query result is missing required column(s): " + requiredColumns.toString());
			}
		} catch (SQLException e) {
			//log.warn("JDBC query for all rows failed", e);
			throw new ConfigurationException("Failed to connect to JDBC data source: " + e.getMessage(), e);
		} finally {
			closeJdbcResource(queryResultSet);
		}
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		return new JdbcSession();
	}

	@Override
	public List<Action> listActions(Boolean all) {
		List<Action> actions = new ArrayList<>();
		actions.add(INSERT_ACTION);
		return actions;
	}

	@Override
	public DocumentActionProcessor getActionProcessor() {
		return this;
	}

	@Override
	public Action getAction(String name) throws ConfigurationException {
		switch (name) {
		case INSERT_ACTION_NAME:
			return INSERT_ACTION;
		default:
			throw new ConfigurationException(name + " not a valid action name");
		}
	}

	@Override
	public void flush(PluginSession session) {
	}

	@Override
	public Iterator<Document> executeAction(PluginSession session, Action action, List<Document> documents)
			throws ConfigurationException, PluginOperationFailedException {

		ImmutableList.Builder<Document> docs = ImmutableList.builder();

		for (Document d : documents) {
			//DocumentBuilder builder = callback.documentBuilder().copy(d);

			JdbcSession jdbcSession = (JdbcSession) session;

			CallableStatement cstmt = null;
			try {
				// Call stored procedure to get batch
				@SuppressWarnings("deprecation")
				String procedureInsertName = action.getConfig().getPropertyValue(PROPERTY_INSERT_PROCEDURE);

				String callSyntax = "{call " + procedureInsertName + "(?)}";
				cstmt = jdbcSession.getCallableStatement(callSyntax);

				@SuppressWarnings("deprecation")
				String procedureInputs = action.getConfig().getPropertyValue(PROPERTY_INSERT_PROCEDURE_VALUES);
				
				//throw new PluginOperationFailedException("procedureInputs: " + procedureInputs);
				
				//List<String> procedureInputList = new ArrayList<String> (Arrays.asList(procedureInputs.split(",")));
				String activityID = DocumentUtil.evaluateTemplate(d, procedureInputs);
				//String activityFileStreamID = DocumentUtil.evaluateTemplate(d, procedureInputList.get(1));
				//String fileSize = DocumentUtil.evaluateTemplate(d, procedureInputList.get(2));
				//String vchFileName = DocumentUtil.evaluateTemplate(d, procedureInputList.get(3));
				cstmt.setInt(1, Integer.valueOf(activityID));
				//cstmt.setInt(2, Integer.valueOf(activityFileStreamID));
				//cstmt.setInt(3, Integer.valueOf(fileSize));
				//cstmt.setString(4, vchFileName);

				cstmt.execute();
				
			} catch (SQLException  e) {
//			} catch (PluginOperationFailedException  e) {	
				//log.warn("JDBC query failed", e);
				throw new PluginOperationFailedException(e.getMessage(), e);
				// } finally {
				// closeJdbcResource(cstmt);
			}

			//docs.add(builder.build());			
		}

		return docs.build().iterator();
	}

	/*
	 * Converts JDBC result into a Document**
	 * 
	 * @param result JDBC result (database row)
	 * 
	 * @return DocumentBuilder
	 * 
	 * @throws SQLException
	 */

	protected DocumentBuilder getDocumentFromJdbcResult(ResultSet result) throws SQLException {
		DocumentBuilder documentBuilder = callback.documentBuilder();

		StringBuilder displayName = new StringBuilder();
		StringBuilder version = new StringBuilder();

		// Iterate over all columns returned in JDBC result
		// and either add them as metadata (if they are not special HCI fields),
		// or create special fields out of them
		// Special fields being ID, DISPLAY_NAME and VERSION. These
		// are exposed in the connector config, so that the user
		// specifies which column(s) to use for ID, DISPLAY_NAME and VERSION fields
		ResultSetMetaData resultMetadata = result.getMetaData();
		for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
			String columnName = resultMetadata.getColumnLabel(i).trim();
			// get the column type and if varbinary get bytes.
			// convert bytes to inputstream and set stream in the documentBuilder
			// implement get and openNamedStream
			int columnType = resultMetadata.getColumnType(i);
			if (columnType != Types.VARBINARY && columnType != Types.NVARCHAR) {

				String columnValue = result.getString(i);
				// Build displayName as a concatination of all specified columns
				if (displayNameColumns.contains(columnName)) {
					if (displayName.length() > 0) {
						displayName.append(" ");
					}
					displayName.append(columnValue);
				}

				// Build version as a concatination of all specified columns
				if (versionColumns.contains(columnName)) {
					if (version.length() > 0) {
						version.append(" ");
					}
					version.append(columnValue);
				}
			}
			// If this is not a special field, add it to the Document metadata
			if (!StandardFields.isRequired(columnName) && !StandardFields.isInternal(columnName)
					&& !columnName.equals(StandardFields.SOLR_VERSION)) {
				if (columnType != Types.VARBINARY && columnType != Types.NVARCHAR) {
					addJdbcMetadata(documentBuilder, result, i);
				}
			}
		}

		if (version.length() == 0) {
			// If version columns were not configured, default version to a constant
			version.append(DEFAULT_VERSION);
		}

		String id = getJdbcResultMarker(result);
		// Constuct URI as the JDBC connection URI plus the document ID
		// which is a WHERE-clause for the row primary key.
		// This is used by getMetadata(URI) method to lookup a specific row
		StringBuilder uri = new StringBuilder();
		try {
			uri.append(encodedConnectionString).append(ID_MARKER).append(URLEncoder.encode(id, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// Ignore. Encoding is valid.
		}

		documentBuilder.setMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(id).build())
				.setMetadata(StandardFields.URI, StringDocumentFieldValue.builder().setString(uri.toString()).build())
				.setMetadata(StandardFields.DISPLAY_NAME,
						StringDocumentFieldValue.builder().setString(displayName.toString()).build())
				.setMetadata(StandardFields.VERSION,
						StringDocumentFieldValue.builder().setString(version.toString()).build());

		documentBuilder.setStreamMetadata(StandardFields.CONTENT, new HashMap<String, String>());
		documentBuilder.setHasContent(true);

		//if (log.isDebugEnabled()) {
		//	log.debug("Created Document from JDBC result: {}", documentBuilder.toJson());
		//}

		return documentBuilder;
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
	 * Returns a WHERE predicate that is used to identify the current row.
	 * 
	 * The method is used for generating document ID, to allow the specific row to
	 * be looked up at a later time.
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

	private class JdbcSession implements PluginSession {
		private Connection connection = null;
		private Statement statement = null;
		private String querySQL = null;
		private String metadataSQL = null;

		public JdbcSession() throws PluginOperationFailedException {
			try {
				Class.forName(getJdbcDriver());
				@SuppressWarnings("deprecation")
				String user = configuration.getPropertyValue(PROPERTY_USERNAME);
				@SuppressWarnings("deprecation")
				String password = configuration.getPropertyValue(PROPERTY_PASSWORD);
				if (Strings.isNullOrEmpty(user)) {
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
			queryStatementBuilder.append("SELECT ").append(columnsString).append(" FROM ").append(fromString);
			querySQL = queryStatementBuilder.toString();

			// SQL statement to get one specific row, used by getMetadata(URI) API
			StringBuilder metadataStatementBuilder = new StringBuilder();
			metadataStatementBuilder.append("SELECT ").append(columnsString).append(" FROM ").append(fromString)
					.append(" WHERE ").append(whereString);
			metadataSQL = metadataStatementBuilder.toString();
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

		public CallableStatement getCallableStatement(String callSyntax) throws PluginOperationFailedException {
			try {
				return connection.prepareCall(callSyntax, ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
			} catch (SQLException e) {
				throw new PluginOperationFailedException("Failed to call sql stored procedure " + e.getMessage(), e);
			}

		}

		/**
		 * Returns a SQL statement to run a metadata lookup.
		 * 
		 * @return SQL Statement to run a metadata lookup.
		 */
		public String getMetadataSQL() {
			return metadataSQL;
		}

		@SuppressWarnings("deprecation")
		public Connection getConnection() throws PluginOperationFailedException {
			try {
				if (connection == null || connection.isClosed()) {
					Class.forName(getJdbcDriver());
					String user = configuration.getPropertyValue(PROPERTY_USERNAME);
					String password = configuration.getPropertyValue(PROPERTY_PASSWORD);
					if (Strings.isNullOrEmpty(user)) {
						connection = DriverManager.getConnection(connectionString);
					} else {
						connection = DriverManager.getConnection(connectionString, user, password);
					}
				}
				return connection;
			} catch (ClassNotFoundException | SQLException e) {
				throw new PluginOperationFailedException("Failed to connect to JDBC. " + e.getMessage(), e);
			}
		}

		/**
		 * Returns a JDBC Statement to run a metadata lookup.
		 * 
		 * Caller must close the statement.
		 * 
		 * @return JDBC Statement to run a metadata lookup.
		 * @throws PluginOperationFailedException
		 */
		public Statement getMetadataStatement() throws PluginOperationFailedException {
			try {
				Statement mdStatement = connection.createStatement();
				return mdStatement;
			} catch (SQLException e) {
				throw new PluginOperationFailedException("Failed to connect to JDBC. " + e.getMessage(), e);
			}
		}

		@SuppressWarnings("unused")
		public PreparedStatement getPreparedStatement(String statement) {
			try {
				connection = getConnection();
				return connection.prepareStatement(statement);
			} catch (SQLException | PluginOperationFailedException e) {
				throw new PluginOperationRuntimeException(statement + " SQL set statement invalid!");
			}
		}

		@Override
		public void close() {
			closeJdbcResource(statement);
			closeJdbcResource(connection);
		}
	}

	/**
	 * Best effort attempt to close any JDBC resource (such as ResultSet, Statement
	 * or Connection)
	 * 
	 * @param resource
	 *            to close
	 */
	private void closeJdbcResource(AutoCloseable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (Exception e) {
				//log.warn("Failed to close JDBC resource {}", resource.toString(), e);
			}
		}
	}

}
