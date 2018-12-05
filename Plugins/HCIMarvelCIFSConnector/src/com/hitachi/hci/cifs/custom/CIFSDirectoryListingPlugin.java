package com.hitachi.hci.cifs.custom;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.hds.commons.util.StringUtils;
import com.hds.ensemble.plugins.file.BaseFileSystemConnectorPlugin;
import com.hds.ensemble.sdk.action.Action;
import com.hds.ensemble.sdk.config.ConfigProperty;
import com.hds.ensemble.sdk.config.ConfigPropertyGroup;
import com.hds.ensemble.sdk.config.ConfigPropertyInfo;
import com.hds.ensemble.sdk.config.PropertyType;
import com.hds.ensemble.sdk.connector.ConnectorMode;
import com.hds.ensemble.sdk.connector.ConnectorOptionalMethod;
import com.hds.ensemble.sdk.connector.ConnectorPluginCategory;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.DocumentNotFoundException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.exception.PluginOperationRuntimeException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.msdtyp.SecurityDescriptor;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.protocol.commons.IOUtils;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.hitachi.hci.cifs.custom.utils.DirectoryMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CIFSDirectoryListingPlugin extends BaseFileSystemConnectorPlugin {
	public static final String NAME = "Cifs Directory Listing Connector";

	public static final ConfigPropertyInfo PROPERTY_HOST = new ConfigProperty.Builder().setName("host").setValue(null)
			.setRequired(true).setUserVisibleName("Host")
			.setUserVisibleDescription("Host name or IP address of the CIFS server.").build();
	public static final ConfigPropertyInfo PROPERTY_SHARE_NAME = new ConfigProperty.Builder().setName("shareName")
			.setValue(null).setRequired(true).setUserVisibleName("Share name").setUserVisibleDescription("Share name")
			.build();
	public static final ConfigPropertyInfo PROPERTY_BASE_FOLDER = new ConfigProperty.Builder().setName("baseFolder")
			.setValue(null).setRequired(false).setUserVisibleName("Base directory")
			.setUserVisibleDescription("The path to a directory to crawl on the CIFS share.").build();
	
	public static final ConfigPropertyInfo PROPERTY_USERNAME = new ConfigProperty.Builder().setName("userName")
			.setValue(null).setRequired(false).setUserVisibleName("Username").setUserVisibleDescription("Username")
			.build();
	private static final ConfigPropertyInfo PROPERTY_DOMAIN_NAME = new ConfigProperty.Builder().setName("domainName")
			.setValue(null).setRequired(false).setUserVisibleName("Domain").setUserVisibleDescription("Domain").build();
	public static final ConfigPropertyInfo PROPERTY_PASSWORD = new ConfigProperty.Builder().setName("password")
			.setValue(null).setRequired(false).setUserVisibleName("Password").setUserVisibleDescription("Password")
			.setType(PropertyType.PASSWORD).build();
	private static final ConfigPropertyInfo PROPERTY_METADATA_DIRNAME = new ConfigProperty.Builder().setName("metadata")
			.setValue(null).setRequired(false).setUserVisibleName("Metadata File Path")
			.setUserVisibleDescription("Path to the Metadata File on the CIFS Share").build();
	private static final PluginConfig DEFAULT_CONFIG = PluginConfig.builder()
			.addGroup(new ConfigPropertyGroup.Builder("Share Info", null).setConfigProperties(ImmutableList.of(
					new ConfigProperty.Builder(PROPERTY_HOST), new ConfigProperty.Builder(PROPERTY_SHARE_NAME),
					new ConfigProperty.Builder(PROPERTY_USERNAME), new ConfigProperty.Builder(PROPERTY_DOMAIN_NAME),
					new ConfigProperty.Builder(PROPERTY_PASSWORD),
					new ConfigProperty.Builder(PROPERTY_METADATA_DIRNAME))))
			.addGroup(new ConfigPropertyGroup.Builder(PATH_GROUP_NAME, null)
					.setConfigProperties(ImmutableList.of(new ConfigProperty.Builder(PROPERTY_BASE_FOLDER),
							new ConfigProperty.Builder(PROPERTY_FILTER_TYPE))))
			.addGroup(WHITELIST_TABLE_GROUP).addGroup(BLACKLIST_TABLE_GROUP)
			.build();

	

	private static final String DISPLAY_NAME = "CIFS Directory Listing Connector";
	private static final String DESCRIPTION = "Connector for accessing a CIFS share and list documnet for directories only.";

	private static final String LONG_DESCRIPTION = "This connector crawls and lists directories and its subdirectories in a CIFS share. All files in the directories are ignored. \n"
			+ "The connector also reads metadata from a  predefined csv template file and tags the directories with metadata."
			+ "\nThis custom connector is based on the Cifs Connector and has been created for a specific use case and intended to be used for DEMO purposes only and not to be used in PRODUCTION!!!" + "\n"
			+ "\nFor Production use please contact Hitachi Vantara Professional Services for more information."  + "\n" + "\n";

	private final String host;
	private final String shareName;
	private final String baseFolder;
	private final String username;
	private final String domain;
	private final String password;
	private final String metadataFilePath;
	private HashMap<String, DirectoryMetadata> mMetaMap = new HashMap<String, DirectoryMetadata>();

	public CIFSDirectoryListingPlugin() {
		super();
		configuration = null;
		callback = null;
		host = null;
		shareName = null;
		baseFolder = null;
		username = null;
		domain = null;
		password = null;
		metadataFilePath = null;
	}

	/**
	 * private constructor with config. Use build() to get a configured
	 * instance.
	 *
	 * @param config
	 *            User customized PluginConfig.
	 * @throws ConfigurationException
	 *             if the configuration is invalid
	 */
	private CIFSDirectoryListingPlugin(PluginConfig config, PluginCallback callback) throws ConfigurationException {
		super(config, callback);
		validateConfig(config);

		host = config.getPropertyValue(PROPERTY_HOST.getName());

		shareName = config.getPropertyValue(PROPERTY_SHARE_NAME.getName());
		username = config.getPropertyValue(PROPERTY_USERNAME.getName());
		domain = config.getPropertyValue(PROPERTY_DOMAIN_NAME.getName());
		password = config.getPropertyValue(PROPERTY_PASSWORD.getName());
		
		String tmpFolder = config.getPropertyValueOrDefault(PROPERTY_BASE_FOLDER.getName(), "");

		baseFolder = removeStartingSlash(tmpFolder);

		String metadataDir = config.getPropertyValueOrDefault(PROPERTY_METADATA_DIRNAME.getName(), "");

		metadataFilePath = removeStartingSlash(metadataDir);

	}

	@Override
	public String getDisplayName() {
		return DISPLAY_NAME;
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	@Override
	public String getLongDescription() {
		return LONG_DESCRIPTION;
	}

	@Override
	public PluginConfig getDefaultConfig() {
		return DEFAULT_CONFIG;
	}

	@Override
	public void validateConfig(PluginConfig config) throws ConfigurationException {
		super.validateConfig(config);
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		try {
			return new CifsSession(host, username, password, domain, shareName);
		} catch (IOException | SMBApiException e) {
			throw new PluginOperationFailedException(e);
		}
	}

	@Override
	public CIFSDirectoryListingPlugin build(PluginConfig config, PluginCallback pluginCallback)
			throws ConfigurationException {
		return new CIFSDirectoryListingPlugin(config, pluginCallback);
	}

	@Override
	public Document root(PluginSession session) throws ConfigurationException, PluginOperationFailedException {
		Document root = getCifsMetadata(baseFolder, session);
		if (!root.isContainer()) {
			throw new PluginOperationFailedException("Path is not a directory.");
		}

		return root;
	}

	@Override
	public Iterator<Document> listContainers(PluginSession session, Document start)
			throws ConfigurationException, PluginOperationFailedException {
		return listInternal(session, start, true);
	}

	@Override
	public Iterator<Document> list(PluginSession session, Document start)
			throws ConfigurationException, PluginOperationFailedException {
		return listInternal(session, start, false);
	}

	public Iterator<Document> listInternal(PluginSession session, Document start, boolean containersOnly)
			throws PluginOperationFailedException {
		String path = removeStartingSlash(encodeDecodePath(start.getUri(), false));

		try {
			FileAllInformation parent = getFile(path, session);

			if (!isDirectory(parent)) {
				throw new PluginOperationFailedException("Not a directory: " + path);
			}

			boolean directoriesOnly = containersOnly || !shouldListFilesInDirectory(getRelativePath(baseFolder, path));

			DiskShare share = getSession(session).getShare();

			List<FileIdBothDirectoryInformation> listing = share.list(path);
			List<String> subDirectories = new ArrayList<>();
			for (FileIdBothDirectoryInformation sub : listing) {
				String filename = sub.getFileName();
				if (".".equals(filename) || "..".equals(filename)) {
					continue;
				}
				if (isDirectory(sub)) {
					subDirectories.add(filename);
					subDirectories.add(filename + ".dir");
				}
			}
			return new StreamingDocumentIterator() {
				private int index = 0;

				@Override
				protected Document getNextDocument() {

					while (index < subDirectories.size()) {
						// Shortcuts in windows are just binary files which
						// means we'll return the
						// contents of the shortcut and not its target (for both
						// directories and
						// files)
						String file = subDirectories.get(index++);
						Path fullPath = Paths.get(path, file);
						SecurityDescriptor securityDescriptor = null;

						try {
							if (file != null && file.endsWith(".dir") && !directoriesOnly) {

								return makeDocument(fullPath, false, null, null, null, new Long(0), securityDescriptor);

							} else if (file != null && !file.endsWith(".dir")) {

								if (!shouldCrawlDirectory(getRelativePath(baseFolder, fullPath.toString()))) {

									continue;
								}

								// Add the folder
								return makeDocument(fullPath, true, null, null, null, new Long(0), securityDescriptor);
							}
						} catch (PluginOperationFailedException e) {
							throw new PluginOperationRuntimeException(e);
						}
					}

					return endOfDocuments();
				}
			};

		} catch (IOException e) {
			throw new PluginOperationFailedException(e);
		} catch (SMBApiException e) {
			if (NtStatus.STATUS_OBJECT_NAME_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NO_SUCH_FILE.equals(e.getStatus())) {
				throw new DocumentNotFoundException(
						String.format("Document not found: \"%s\". Status: %s", path, e.getStatus().name()));
			}
			throw new PluginOperationFailedException(e);
		}
	}

	@Override
	public Document getMetadata(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {

		return getCifsMetadata(encodeDecodePath(uri.toString(), false), session);
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
	public InputStream get(PluginSession session, URI uri)
			throws ConfigurationException, PluginOperationFailedException {
		try {
			DiskShare share = getSession(session).getShare();

			File file = share.openFile(encodeDecodePath(uri.toString(), false), EnumSet.of(AccessMask.GENERIC_READ),
					EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL), SMB2ShareAccess.ALL,
					SMB2CreateDisposition.FILE_OPEN, EnumSet.noneOf(SMB2CreateOptions.class));
			return new CifsFileInputStream(file);
		} catch (IOException e) {
			throw new PluginOperationFailedException(e);
		} catch (SMBApiException e) {
			if (NtStatus.STATUS_OBJECT_NAME_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NO_SUCH_FILE.equals(e.getStatus())) {
				throw new DocumentNotFoundException(
						String.format("Document not found: \"%s\". Status: %s", uri, e.getStatus().name()));
			}
			throw new PluginOperationFailedException(e);
		}
	}

	@Override
	public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
			throws ConfigurationException, PluginOperationFailedException {

		Map<String, String> streamMetadata = doc.getAllStreamMetadata().get(streamName);
		if (streamMetadata == null) {

			return null;
		}
		String docUriStr = doc.getUri();
		URI docUri;
		try {
			docUri = new URI(docUriStr);
			if (streamName.equals(StandardFields.CONTENT)) {
				return get(session, docUri);
			}
			return null;
		} catch (URISyntaxException ex) {
			throw new ConfigurationException("Failed to parse document URI", ex);
		}
	}

	@Override
	public void test(PluginSession session) throws ConfigurationException, PluginOperationFailedException {
		root(session);
	}

	@Override
	public ConnectorPluginCategory getCategory() {
		return ConnectorPluginCategory.FILE;
	}

	@Override
	public String getSubCategory() {
		return "Custom";
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

	private FileAllInformation getFile(String path, PluginSession session) throws PluginOperationFailedException {
		try {
			return getSession(session).getShare().getFileInformation(removeStartingSlash(path));
		} catch (IOException e) {
			throw new PluginOperationFailedException(e);
		} catch (SMBApiException e) {
			if (NtStatus.STATUS_OBJECT_NAME_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NO_SUCH_FILE.equals(e.getStatus())) {
				throw new DocumentNotFoundException(
						String.format("Document not found: \"%s\". Status: %s", path, e.getStatus().name()));
			}
			throw new PluginOperationFailedException(e);
		}
	}

	// DiskShare is very particular about not having a starting slash, even if
	// you're just listing
	// the root.
	private String removeStartingSlash(String path) {
		if (!path.isEmpty() && (path.charAt(0) == '/' || path.charAt(0) == '\\')) {
			return path.substring(1);
		}

		return path;
	}

	private boolean isDirectory(FileAllInformation fileInfo) {
		return fileInfo.getStandardInformation().isDirectory();
	}

	private boolean isDirectory(FileIdBothDirectoryInformation f) {
		long b = (f.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
		return b != 0;
	}

	private Document getCifsMetadata(String path, PluginSession session) throws PluginOperationFailedException {
		try {
			boolean isNotDir = false;
			if (path.endsWith(".dir")) {
				path = path.substring(0, path.indexOf(".dir"));
				isNotDir = true;
			}
			FileAllInformation fileInfo = getFile(removeStartingSlash(path), session);
			
			if (isNotDir) {
				path = path + ".dir";
			}
			return makeDocument(Paths.get(path), !isNotDir, fileInfo.getBasicInformation().getChangeTime(),
					fileInfo.getBasicInformation().getCreationTime(),
					fileInfo.getBasicInformation().getLastAccessTime(),
					fileInfo.getStandardInformation().getEndOfFile(), null);

		} catch (SMBApiException e) {
			if (NtStatus.STATUS_OBJECT_NAME_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NOT_FOUND.equals(e.getStatus())
					|| NtStatus.STATUS_NO_SUCH_FILE.equals(e.getStatus())) {
				throw new DocumentNotFoundException(
						String.format("Document not found: \"%s\". Status: %s", path, e.getStatus().name()));
			}
			throw new PluginOperationFailedException(e);
		}
	}

	private Document makeDocument(Path fullPath, boolean isDir, FileTime changeTime, FileTime created,
			FileTime accessed, long endOfFile, SecurityDescriptor securityDescriptor)
			throws PluginOperationFailedException {
		DocumentBuilder builder = callback.documentBuilder();
		if (isDir) {
			builder.setIsContainer(true).setHasContent(false);
		} else {
			builder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap());
			// String version = makeVersion(changeTime, endOfFile);
			builder.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());
		}

		String filePath = fullPath.toString();
		String path = "";
		String name;

		if (fullPath.getParent() != null) {
			path = fullPath.getParent().toString();
		}
		// Empty path is the root of the share
		String id = filePath;

		// DiskShare doesn't use a forward slash for root, you just use an empty
		// string. But we
		// can't save an empty string as an id or path so use a forward slash
		// and we'll need to
		// strip it every time we need to access root
		if (filePath.isEmpty()) {
			id = "/";
			name = "/";
			filePath = "/";
		}

		if (fullPath.getFileName() != null) {
			name = fullPath.getFileName().toString();
		} else {
			name = filePath;
		}
		String folderName = null;
		if (name.endsWith(".dir")) {
			folderName = name.substring(0, name.indexOf(".dir"));
		}
		builder.addMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(id).build());
		builder.addMetadata(StandardFields.URI,
				StringDocumentFieldValue.builder().setString(encodeDecodePath(filePath, true)).build());
		
		if (folderName != null && !folderName.isEmpty()) {
			builder.addMetadata(StandardFields.FILENAME,
					StringDocumentFieldValue.builder().setString(folderName).build());
			builder.addMetadata(StandardFields.DISPLAY_NAME, StringDocumentFieldValue.builder().setString(folderName).build());
			builder.addMetadata(StandardFields.PATH,
					StringDocumentFieldValue.builder().setString(fullPath.toString().substring(0, fullPath.toString().indexOf(".dir"))).build());
		} else {
			builder.addMetadata(StandardFields.FILENAME, StringDocumentFieldValue.builder().setString(name).build());
			builder.addMetadata(StandardFields.DISPLAY_NAME, StringDocumentFieldValue.builder().setString(name).build());
			builder.addMetadata(StandardFields.PATH,
					StringDocumentFieldValue.builder().setString(fullPath.toString()).build());
		}

		builder.addMetadata(StandardFields.RELATIVE_PATH,
				StringDocumentFieldValue.builder().setString(getRelativePath(baseFolder, path)).build());

		builder.addMetadata("HCI_cifsHost", StringDocumentFieldValue.builder().setString(host).build());
		builder.addMetadata("HCI_shareName", StringDocumentFieldValue.builder().setString(shareName).build());

		if (!mMetaMap.isEmpty()) {
			if (folderName != null && !folderName.isEmpty()) {
				DirectoryMetadata dMetadata = mMetaMap.get(folderName.trim());

				if (dMetadata != null) {
					builder.addMetadata("FRANCHISE",
							StringDocumentFieldValue.builder().setString(dMetadata.getFranchise()).build());
					builder.addMetadata("ITEM_NUMBER",
							StringDocumentFieldValue.builder().setString(dMetadata.getItemNumber()).build());
					builder.addMetadata("ITEM_DESCRIPTION",
							StringDocumentFieldValue.builder().setString(dMetadata.getItemDescription()).build());
					builder.addMetadata("IMPRINT",
							StringDocumentFieldValue.builder().setString(dMetadata.getImprint()).build());
					builder.addMetadata("MAIN_TITLE",
							StringDocumentFieldValue.builder().setString(dMetadata.getMainTitle()).build());
					builder.addMetadata("NAMING_CONVENTION",
							StringDocumentFieldValue.builder().setString(dMetadata.getNamingConvention()).build());
					builder.addMetadata("ITEM_TYPE",
							StringDocumentFieldValue.builder().setString(dMetadata.getItemType()).build());
					builder.addMetadata("EDITOR_NAME",
							StringDocumentFieldValue.builder().setString(dMetadata.getEditorName()).build());
					builder.addMetadata("DIRECT_ONSALE_DATE",
							StringDocumentFieldValue.builder().setString(dMetadata.getDirectOnsaleDate()).build());
					builder.addMetadata("ISBN",
							StringDocumentFieldValue.builder().setString(dMetadata.getIsbn()).build());
					builder.addMetadata("eBook_ISBN-13",
							StringDocumentFieldValue.builder().setString(dMetadata.getEbookIsbn()).build());
					builder.addMetadata("UPC",
							StringDocumentFieldValue.builder().setString(dMetadata.getUpc()).build());
					builder.addMetadata("EAN",
							StringDocumentFieldValue.builder().setString(dMetadata.getEan()).build());

				}
			}
		}

		return builder.build();
	}

	private String encodeDecodePath(String path, boolean encode) throws PluginOperationFailedException {
		try {
			if (encode) {
				return StringUtils.encodeFilename(path);
			} else {
				return StringUtils.decodeFilename(path);
			}
		} catch (UnsupportedEncodingException e) {
			// Should not happen
			throw new PluginOperationFailedException(e);
		}
	}

	private CifsSession getSession(PluginSession session) throws PluginOperationFailedException {
		if (session instanceof CifsSession) {
			return (CifsSession) session;
		}

		throw new PluginOperationFailedException("Session is not a CifsSession");
	}

	private class CifsSession implements PluginSession {
		private SMBClient client = new SMBClient();
		private Connection connection;
		private Session session;
		private DiskShare share;
		private String host;
		private String shareName;
		private AuthenticationContext ac;

		public CifsSession(String host, String username, String password, String domain, String shareName)
				throws IOException {
			this.host = host;
			this.shareName = shareName;

			if (Strings.isNullOrEmpty(username)) {
				// Just a convenience method for AuthenticationContext("", new
				// char[0], null)
				ac = AuthenticationContext.anonymous();
			} else {
				ac = new AuthenticationContext(username, password.toCharArray(), domain);
			}

			share = connectShare();

			if (mMetaMap.isEmpty()) {

				if (this.share.folderExists(metadataFilePath)) {
					List<FileIdBothDirectoryInformation> listing = share.list(metadataFilePath);
					// FileIdBothDirectoryInformation metadataFile =
					// listing.get(0);
					List<String> subDirectories = new ArrayList<>();
					for (FileIdBothDirectoryInformation sub : listing) {
						String filename = sub.getFileName();
						if (".".equals(filename) || "..".equals(filename)) {
							continue;
						}
						if (filename.endsWith(".csv")) {
							subDirectories.add(filename);
							break;
						}
					}

					if (subDirectories.size() == 1) {

						File file;
						try {
							file = share.openFile(
									encodeDecodePath(Paths.get(metadataFilePath, subDirectories.get(0)).toString(),
											false),
									EnumSet.of(AccessMask.GENERIC_READ),
									EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL), SMB2ShareAccess.ALL,
									SMB2CreateDisposition.FILE_OPEN, EnumSet.noneOf(SMB2CreateOptions.class));

							InputStream metadataInputStream = new CifsFileInputStream(file);

							String line = "";

							BufferedReader br = null;
							try {
								br = new BufferedReader(new InputStreamReader(metadataInputStream));
								while ((line = br.readLine()) != null) {

									// use comma as separator
									String[] metadata = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

									if (metadata.length == 13) {

										if ("NAMING_CONVENTION".equalsIgnoreCase(metadata[5])) {
											continue;
										}

										DirectoryMetadata dirMetaData = new DirectoryMetadata(metadata[0], metadata[1],
												metadata[2], metadata[3], metadata[4], metadata[5], metadata[6],
												metadata[7], metadata[8], metadata[9], metadata[10], metadata[11],
												metadata[12]);
										mMetaMap.put(metadata[5], dirMetaData);
									}

								}

							} catch (IOException e) {
								e.printStackTrace();
							} finally {
								br.close();
							}

						} catch (PluginOperationFailedException e) {

							e.printStackTrace();
						}
					}
				}
			}
		}

		@Override
		public void close() {
			if (share != null) {
				IOUtils.closeQuietly(share);
			}

			if (session != null) {
				IOUtils.closeQuietly(session);
			}

			if (connection != null) {
				IOUtils.closeQuietly(connection);
			}
		}

		public DiskShare getShare() throws IOException {
			if (share != null && share.isConnected()) {
				return share;
			}
			close();
			share = connectShare();
			return share;
		}

		private DiskShare connectShare() throws IOException {
			connection = client.connect(host);
			session = connection.authenticate(ac);
			return (DiskShare) session.connectShare(shareName);
		}
	}

	/**
	 * InputStream obtains from a CIFS File object.
	 *
	 * Delegates all methods to File.getInputStream()
	 *
	 * Closes the File when the stream is closed.
	 */
	private class CifsFileInputStream extends InputStream {
		File file;
		InputStream is;

		public CifsFileInputStream(File file) {
			this.file = file;
			this.is = file.getInputStream();
		}

		@Override
		public void close() throws IOException {
			is.close();
			file.close();
		}

		@Override
		public int read() throws IOException {
			return is.read();
		}

		@Override
		public int available() throws IOException {
			return is.available();
		}

		@Override
		public synchronized void mark(int readlimit) {
			is.mark(readlimit);
		}

		@Override
		public boolean markSupported() {
			return is.markSupported();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return is.read(b, off, len);
		}

		@Override
		public int read(byte[] b) throws IOException {
			return is.read(b);
		}

		@Override
		public synchronized void reset() throws IOException {
			is.reset();
		}

		@Override
		public long skip(long n) throws IOException {
			return is.skip(n);
		}
	}

	@Override
	protected void delete(PluginSession arg0, Action arg1, Document arg2)
			throws ConfigurationException, PluginOperationFailedException {
		
		
	}

	@Override
	protected void write(PluginSession arg0, Action arg1, Document arg2)
			throws ConfigurationException, PluginOperationFailedException {
		
		
	}
}