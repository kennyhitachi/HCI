/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */
package com.hitachi.hci.plugins.connector.custom;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.hds.commons.util.StringUtils;
import com.hds.ensemble.logging.HdsLogger;
import com.hds.ensemble.plugin.PluginNames;
import com.hds.ensemble.plugins.SystemCategories;
import com.hds.ensemble.plugins.file.BaseFileSystemConnectorPlugin;
import com.hds.ensemble.sdk.action.Action;
import com.hds.ensemble.sdk.action.ActionType;
import com.hds.ensemble.sdk.action.StandardActions;
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
import com.hds.ensemble.sdk.model.LongDocumentFieldValue;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StreamingDocumentIterator;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;
import com.hierynomus.msdtyp.ACL;
import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.msdtyp.SID;
import com.hierynomus.msdtyp.SecurityDescriptor;
import com.hierynomus.msdtyp.SecurityDescriptor.Control;
import com.hierynomus.msdtyp.SecurityInformation;
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
import com.hierynomus.smbj.common.SMBRuntimeException;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

public class CifsMultiplePathConnector extends BaseFileSystemConnectorPlugin {
    public static final String NAME = "CifsMultiplePathConnector";

    public static final ConfigPropertyInfo PROPERTY_HOST = new ConfigProperty.Builder()
            .setName("host")
            .setValue("localhost")
            .setRequired(true)
            .setUserVisibleName("Host")
            .setUserVisibleDescription("Host name or IP address of the CIFS server.")
            .build();
    public static final ConfigPropertyInfo PROPERTY_SHARE_NAME = new ConfigProperty.Builder()
            .setName("shareName")
            .setValue("test")
            .setRequired(true)
            .setUserVisibleName("Share name")
            .setUserVisibleDescription("Share name")
            .build();
    public static final ConfigPropertyInfo PROPERTY_BASE_FOLDER = new ConfigProperty.Builder()
            .setName("baseFolder")
            .setValue(null)
            .setRequired(false)
            .setUserVisibleName("Base directory")
            .setUserVisibleDescription("The path to a directory to crawl on the CIFS share.")
            .build();
    public static final ConfigPropertyInfo PROPERTY_INCLUDE_SECURITY_INFO = new ConfigProperty.Builder()
            .setName("includeSecurityInfo")
            .setType(PropertyType.CHECKBOX)
            .setValue(Boolean.FALSE.toString())
            .setRequired(false)
            .setUserVisibleName("Include owner and group SIDs")
            .setUserVisibleDescription("Set to true to include owner and group security identifiers in file listings. This may cause slower performance.")
            .build();
    public static final ConfigPropertyInfo PROPERTY_USERNAME = new ConfigProperty.Builder()
            .setName("userName")
            .setValue("kenng")
            .setRequired(false)
            .setUserVisibleName("Username")
            .setUserVisibleDescription("Username")
            .build();
    private static final ConfigPropertyInfo PROPERTY_DOMAIN_NAME = new ConfigProperty.Builder()
            .setName("domainName")
            .setValue("hds.com")
            .setRequired(false)
            .setUserVisibleName("Domain")
            .setUserVisibleDescription("Domain")
            .build();
    public static final ConfigPropertyInfo PROPERTY_PASSWORD = new ConfigProperty.Builder()
            .setName("password")
            .setValue("work@HV2019")
            .setRequired(false)
            .setUserVisibleName("Password")
            .setUserVisibleDescription("Password")
            .setType(PropertyType.PASSWORD)
            .build();

    private static final PluginConfig DEFAULT_CONFIG = PluginConfig
            .builder()
            .addGroup(new ConfigPropertyGroup.Builder("Share Info", null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_HOST),
                                new ConfigProperty.Builder(PROPERTY_SHARE_NAME),
                                new ConfigProperty.Builder(PROPERTY_INCLUDE_SECURITY_INFO),
                                new ConfigProperty.Builder(PROPERTY_USERNAME),
                                new ConfigProperty.Builder(PROPERTY_DOMAIN_NAME),
                                new ConfigProperty.Builder(PROPERTY_PASSWORD))))
            .addGroup(new ConfigPropertyGroup.Builder(PATH_GROUP_NAME, null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_BASE_FOLDER),
                                new ConfigProperty.Builder(PROPERTY_FILTER_TYPE))))
            .addGroup(WHITELIST_TABLE_GROUP)
            .addGroup(BLACKLIST_TABLE_GROUP)
            .addGroup(new ConfigPropertyGroup.Builder("Options", null)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_MAX_VISITED_FILE_SIZE))))
            .build();

    private static final String WRITE_OWNER_SID = "write.owner.sid";
    private static final String WRITE_GROUP_SID = "write.group.sid";
    private static final String OUTPUT_OWNER_SID = "output.owner.sid";
    private static final String OUTPUT_GROUP_SID = "output.group.sid";
    protected static final String PROP_VISIBLE_NAME_OWNER_SID = "Owner SID";
    protected static final String PROP_DESCRIPTION_OWNER_SID = "Owner Security Identifier (SID) to set on the created file. Can be either a document field (eg "
            + StandardFields.OWNER + ") or a value.";
    protected static final String PROP_VISIBLE_NAME_GROUP_SID = "Group SID";
    protected static final String PROP_DESCRIPTION_GROUP_SID = "Group Security Identifier (SID) to set on the created file. Can be either a document field (eg "
            + StandardFields.GID + ") or a value.";

    private static final ConfigPropertyInfo PROPERTY_WRITE_OWNER_SID = new ConfigProperty.Builder()
            .setName(WRITE_OWNER_SID)
            .setUserVisibleName(PROP_VISIBLE_NAME_OWNER_SID)
            .setUserVisibleDescription(PROP_DESCRIPTION_OWNER_SID)
            .setType(PropertyType.TEXT)
            .build();
    private static final ConfigPropertyInfo PROPERTY_WRITE_GROUP_SID = new ConfigProperty.Builder()
            .setName(WRITE_GROUP_SID)
            .setUserVisibleName(PROP_VISIBLE_NAME_GROUP_SID)
            .setUserVisibleDescription(PROP_DESCRIPTION_GROUP_SID)
            .setType(PropertyType.TEXT)
            .build();
    private static final ConfigPropertyInfo PROPERTY_OUTPUT_OWNER_SID = new ConfigProperty.Builder()
            .setName(OUTPUT_OWNER_SID)
            .setUserVisibleName(PROP_VISIBLE_NAME_OWNER_SID)
            .setUserVisibleDescription(PROP_DESCRIPTION_OWNER_SID)
            .setType(PropertyType.TEXT)
            .build();
    private static final ConfigPropertyInfo PROPERTY_OUTPUT_GROUP_SID = new ConfigProperty.Builder()
            .setName(OUTPUT_GROUP_SID)
            .setUserVisibleName(PROP_VISIBLE_NAME_GROUP_SID)
            .setUserVisibleDescription(PROP_DESCRIPTION_GROUP_SID)
            .setType(PropertyType.TEXT)
            .build();

    private static final PluginConfig CIFS_WRITE_CONFIG = PluginConfig.builder()
            .addGroup(new ConfigPropertyGroup.Builder()
                    .setName(WRITE_CONFIG_GROUP)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_WRITE_STREAM),
                                new ConfigProperty.Builder(PROPERTY_WRITE_FILENAME),
                                new ConfigProperty.Builder(PROPERTY_WRITE_RELATIVE_PATH),
                                new ConfigProperty.Builder(PROPERTY_WRITE_BASE_PATH),
                                new ConfigProperty.Builder(PROPERTY_WRITE_OWNER_SID),
                                new ConfigProperty.Builder(PROPERTY_WRITE_GROUP_SID))))
            .build();
    private static final PluginConfig CIFS_OUTPUT_CONFIG = PluginConfig.builder()
            .addGroup(new ConfigPropertyGroup.Builder()
                    .setName(OUTPUT_CONFIG_GROUP)
                    .setConfigProperties(ImmutableList
                            .of(new ConfigProperty.Builder(PROPERTY_OUTPUT_STREAM),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_FILENAME),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_RELATIVE_PATH),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_BASE_PATH),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_OWNER_SID),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_GROUP_SID),
                                new ConfigProperty.Builder(PROPERTY_OUTPUT_EMPTY_PARENTS))))
            .build();

    private static final String DISPLAY_NAME = "CIFS Multipath";
    private static final String DESCRIPTION = "Connector for accessing a CIFS share.";

    private static final String LONG_DESCRIPTION = "This connector can access objects in a CIFS share. "
            + "With this connector, the CIFS 2.x protocol is used to gather files."
            + "\n"
            + "\nYou can configure a " + DISPLAY_NAME
            + " data connection to access all objects under a base directory on a specified host."
            + "\n"
            + "\n";

    private static final Logger log = HdsLogger.getLogger();
    private static final Set<SecurityInformation> SECURITY_INFO = new HashSet<>(Arrays
            .asList(new SecurityInformation[] { SecurityInformation.OWNER_SECURITY_INFORMATION,
                    SecurityInformation.GROUP_SECURITY_INFORMATION }));

    // In CIFS there are two different file properties that show when the file was changed/modified.
    // We'll use StandardFields.MODIFIED_DATE field for when file content was modified (aka last
    // written) and this additional field for when file metadata was changed.
    private static final String CHANGED_DATE_MILLIS = StandardFields.HCI_PREFIX
            + "changedDateMillis";
    private static final String CHANGED_DATE_STRING = StandardFields.HCI_PREFIX
            + "changedDateString";

    private final String host;
    private final String shareName;
    private final String baseFolder;
    private final String username;
    private final String domain;
    private final String password;
    private final Boolean includeSecurityInfo;

    public CifsMultiplePathConnector() {
        super();
        configuration = null;
        callback = null;
        host = "localhost";
        shareName = null;
        baseFolder = null;
        username = null;
        domain = null;
        password = null;
        includeSecurityInfo = false;
        
    }

    /**
     * private constructor with config. Use build() to get a configured instance.
     *
     * @param config User customized PluginConfig.
     * @throws ConfigurationException if the configuration is invalid
     */
    private CifsMultiplePathConnector(PluginConfig config, PluginCallback callback)
            throws ConfigurationException {
        super(config, callback);
        validateConfig(config);

        host = config.getPropertyValue(PROPERTY_HOST.getName());

        shareName = config.getPropertyValue(PROPERTY_SHARE_NAME.getName());
        username = config.getPropertyValue(PROPERTY_USERNAME.getName());
        domain = config.getPropertyValue(PROPERTY_DOMAIN_NAME.getName());
        password = config.getPropertyValue(PROPERTY_PASSWORD.getName());
        includeSecurityInfo = Boolean.parseBoolean(config
                .getPropertyValueOrDefault(PROPERTY_INCLUDE_SECURITY_INFO.getName(),
                                           Boolean.FALSE.toString()));
        String tmpFolder = config.getPropertyValueOrDefault(PROPERTY_BASE_FOLDER.getName(), "");

        baseFolder = removeStartingSlash(tmpFolder);
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
    public PluginSession startSession()
            throws ConfigurationException, PluginOperationFailedException {
        try {
            return new CifsSession(host, username, password, domain, shareName);
        } catch (IOException | SMBRuntimeException e) {
            throw new PluginOperationFailedException(e);
        }
    }

    @Override
    public CifsMultiplePathConnector build(PluginConfig config, PluginCallback pluginCallback)
            throws ConfigurationException {
        return new CifsMultiplePathConnector(config, pluginCallback);
    }

    @Override
    public Document root(PluginSession session)
            throws ConfigurationException, PluginOperationFailedException {
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

    public Iterator<Document> listInternal(PluginSession session, Document start,
                                           boolean containersOnly)
            throws PluginOperationFailedException {
        String path = removeStartingSlash(encodeDecodePath(start.getUri(), false));

        try {
            FileAllInformation parent = getFile(path, session);

            if (!isDirectory(parent)) {
                throw new PluginOperationFailedException("Not a directory: " + path);
            }

            boolean directoriesOnly = containersOnly
                    || !shouldListFilesInDirectory(getRelativePath(baseFolder, path));

            DiskShare share = getSession(session).getShare();

            List<FileIdBothDirectoryInformation> listing = share.list(path);
            return new StreamingDocumentIterator() {
                private int index = 0;

                @Override
                protected Document getNextDocument() {

                    while (index < listing.size()) {
                        // Shortcuts in windows are just binary files which means we'll return the
                        // contents of the shortcut and not its target (for both directories and
                        // files)
                        FileIdBothDirectoryInformation file = listing.get(index++);
                        Path fullPath = Paths.get(path, file.getFileName());
                        SecurityDescriptor securityDescriptor = null;

                        try {
                            if (isSystem(file)) {
                                log.debug("{} is a system file. Skipping", fullPath);
                                continue;
                            }

                            if (isHidden(file)) {
                                log.debug("{} is a hidden file. Skipping", fullPath);
                                continue;
                            }

                            if (!isDirectory(file) && !directoriesOnly) {
                                if (file.getAllocationSize() <= maxFileSize) {
                                    if (includeSecurityInfo) {
                                        try {
                                            securityDescriptor = share
                                                    .getSecurityInfo(removeStartingSlash(fullPath
                                                            .toString()), SECURITY_INFO);
                                        } catch (SMBRuntimeException e) {
                                            // Continue w/o security info if we can't get it
                                            log.debug(e.getMessage(), e);
                                        }
                                    }
                                    return makeDocument(fullPath,
                                                        isDirectory(file), file.getChangeTime(),
                                                        file.getCreationTime(),
                                                        file.getLastAccessTime(),
                                                        file.getLastWriteTime(),
                                                        file.getEndOfFile(),
                                                        securityDescriptor);
                                } else {
                                    log.debug("File {} exceeds the maximum size ({}) specified for this data source. Skipping.",
                                              path, maxFileSize);
                                    continue;
                                }

                            } else if (isDirectory(file)) {
                                if (".".equals(file.getFileName())
                                        || "..".equals(file.getFileName())) {
                                    continue;
                                }

                                if (!shouldCrawlDirectory(getRelativePath(baseFolder,
                                                                          fullPath.toString()))) {
                                    log.debug("Directory {} is blacklisted. Skipping", fullPath);
                                    continue;
                                }

                                if (includeSecurityInfo) {
                                    try {
                                        securityDescriptor = share
                                                .getSecurityInfo(removeStartingSlash(fullPath
                                                        .toString()), SECURITY_INFO);
                                    } catch (SMBRuntimeException smbex) {
                                        // Continue w/o security info if we can't get it
                                        log.debug(smbex.getMessage(), smbex);
                                    }
                                }
                                // Add the folder
                                return makeDocument(fullPath,
                                                    isDirectory(file), file.getChangeTime(),
                                                    file.getCreationTime(),
                                                    file.getLastAccessTime(),
                                                    file.getLastWriteTime(),
                                                    file.getEndOfFile(), securityDescriptor);
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
            if (isFileNotFound(e.getStatus())) {
                throw new DocumentNotFoundException(String
                        .format("Document not found: \"%s\". Status: %s",
                                path, e.getStatus().name()));
            }
            throw new PluginOperationFailedException(e);
        } catch (SMBRuntimeException e) {
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

            File file = share
                    .openFile(encodeDecodePath(uri.toString(), false),
                              EnumSet.of(AccessMask.GENERIC_READ),
                              EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                              SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN,
                              EnumSet.noneOf(SMB2CreateOptions.class));
            return new CifsFileInputStream(file);
        } catch (IOException e) {
            throw new PluginOperationFailedException(e);
        } catch (SMBApiException e) {
            if (isFileNotFound(e.getStatus())) {
                throw new DocumentNotFoundException(String
                        .format("Document not found: \"%s\". Status: %s",
                                uri, e.getStatus().name()));
            }
            throw new PluginOperationFailedException(e);
        } catch (SMBRuntimeException e) {
            throw new PluginOperationFailedException(e);
        }
    }

    @Override
    public InputStream openNamedStream(PluginSession session, Document doc, String streamName)
            throws ConfigurationException, PluginOperationFailedException {
        log.debug("Reading stream {} on {}", streamName, doc.getUri());
        Map<String, String> streamMetadata = doc.getAllStreamMetadata().get(streamName);
        if (streamMetadata == null) {
            log.info("Stream metadata not found");
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
    public void test(PluginSession session)
            throws ConfigurationException, PluginOperationFailedException {
        root(session);
    }

    @Override
    public ConnectorPluginCategory getCategory() {
        return ConnectorPluginCategory.FILE;
    }

    @Override
    public String getSubCategory() {
        return SystemCategories.BUILT_IN;
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
    protected void write(PluginSession session, Action action, Document d)
            throws ConfigurationException, PluginOperationFailedException {
        if (d.isDeleted()) {
            return;
        }

        String name = getStringConfigProperty(action.getConfig(), PROPERTY_WRITE_FILENAME.getName(),
                                              d, true);
        if (Strings.isNullOrEmpty(name)) {
            throw new PluginOperationFailedException("Document does not have a file name");
        }

        ConfigProperty streamProp = action.getConfig().getProperty(PROPERTY_WRITE_STREAM.getName());

        String relativePath = getStringConfigProperty(action.getConfig(),
                                                      PROPERTY_WRITE_RELATIVE_PATH.getName(),
                                                      d, true);
        String basePath = action.getConfig().getProperty(PROPERTY_WRITE_BASE_PATH.getName())
                .getValue();

        String pathToWrite = removeStartingSlash(Paths.get(baseFolder, basePath, relativePath, name)
                .toString());

        String ownerSid = getStringConfigProperty(action.getConfig(),
                                                  PROPERTY_WRITE_OWNER_SID.getName(), d, false);
        String groupSid = getStringConfigProperty(action.getConfig(),
                                                  PROPERTY_WRITE_GROUP_SID.getName(), d, false);

        try {
            DiskShare share = getSession(session).getShare();

            createParentDirectory(share, Paths.get(pathToWrite).getParent(), ownerSid, groupSid);

            if (d.isContainer()) {
                if (!share.folderExists(pathToWrite)) {
                    log.debug("Creating directory {}", pathToWrite);
                    try {
                        share.mkdir(pathToWrite);
                    } catch (SMBApiException e) {
                        if (NtStatus.STATUS_OBJECT_NAME_COLLISION.equals(e.getStatus())) {
                            // The directory we are trying to create already exists
                            // This can happen due to races between multiple workflow executors
                            return;
                        }
                        throw e;
                    }
                }
            } else {
                log.debug("Writing stream {} to file {}", streamProp.getValue(), pathToWrite);
                File outfile = openFileForWrite(share, pathToWrite, true);
                try (InputStream inputStream = callback.openNamedStream(d, streamProp.getValue());
                        OutputStream outputStream = outfile.getOutputStream()) {
                    org.apache.commons.io.IOUtils.copyLarge(inputStream, outputStream);
                    outputStream.flush();
                } finally {
                    outfile.flush();
                    outfile.close();
                }
            }

            setOwnership(share, pathToWrite, ownerSid, groupSid);
        } catch (IOException | SMBRuntimeException e) {
            throw new PluginOperationFailedException("Failed to write file " + pathToWrite, e);
        }
    }

    @Override
    protected void delete(PluginSession session, Action action, Document d)
            throws ConfigurationException, PluginOperationFailedException {
        String name = getStringConfigProperty(action.getConfig(),
                                              PROPERTY_DELETE_FILENAME.getName(), d, true);
        String relativePath = getStringConfigProperty(action.getConfig(),
                                                      PROPERTY_DELETE_RELATIVE_PATH.getName(),
                                                      d, true);
        String pathToDelete = removeStartingSlash(Paths.get(baseFolder, relativePath, name)
                .toString());

        log.debug("Deleting file {}", pathToDelete);
        try {
            DiskShare share = getSession(session).getShare();

            if (d.isContainer()) {
                share.rmdir(pathToDelete, false);
            } else {
                share.rm(pathToDelete);
            }

            boolean deleteEmptyParents = Boolean.parseBoolean(action.getConfig()
                    .getPropertyValue(PROPERTY_DELETE_EMPTY_PARENTS.getName()));
            if (deleteEmptyParents) {
                deleteParentDirectories(share, pathToDelete);
            }
        } catch (IOException e) {
            throw new PluginOperationFailedException(e);
        } catch (SMBApiException e) {
            if (isFileNotFound(e.getStatus())) {
                // Already deleted? Treat as success
                log.debug("Delete of {} failed because it is not there. Status: {}", pathToDelete,
                          e.getStatus().name(), e);
            } else {
                throw new PluginOperationFailedException(e);
            }
        } catch (SMBRuntimeException e) {
            throw new PluginOperationFailedException(e);
        }
    }

    @Override
    protected Action createWriteAction() {
        return Action.builder().name(StandardActions.WRITE_FILE)
                .description("For each document, writes the document's contents to a file in the data source.  The destination for the write is made up of: the location from the data connection; the file path from the data connection (if any); an optional base path that you can specify; and the values for two document fields, one containing a relative path, and the other a filename.")
                .config(CIFS_WRITE_CONFIG).available(true)
                .types(EnumSet.of(ActionType.OUTPUT, ActionType.STAGE)).build();
    }

    @Override
    protected Action createOutputAction() {
        return Action.builder().name(OUTPUT_ACTION_NAME)
                .description("Performs a writeFile action if the value for a document's "
                        + StandardFields.OPERATION
                        + " field is CREATED, or a delete action if the value is DELETED.")
                .config(CIFS_OUTPUT_CONFIG).available(true)
                .types(EnumSet.of(ActionType.OUTPUT, ActionType.STAGE)).build();
    }

    @Override
    protected void output(PluginSession session, Action action, Document d)
            throws ConfigurationException, PluginOperationFailedException {

        // Set the Write/Delete config values with the ones from the Output config
        // And call into either write() or delete()

        if (d.isDeleted()) {
            Action.Builder builder = new Action.Builder();
            builder.name(DELETE_ACTION.getName());

            ConfigProperty.Builder deleteFilenameProp = new ConfigProperty.Builder(
                    PROPERTY_DELETE_FILENAME);
            ConfigProperty.Builder deletePathProp = new ConfigProperty.Builder(
                    PROPERTY_DELETE_RELATIVE_PATH);
            ConfigProperty.Builder deleteEmptyParentsProp = new ConfigProperty.Builder(
                    PROPERTY_DELETE_EMPTY_PARENTS);

            deleteFilenameProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_FILENAME));
            deletePathProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_RELATIVE_PATH));
            deleteEmptyParentsProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_EMPTY_PARENTS));

            ConfigPropertyGroup.Builder configGroupBuilder = new ConfigPropertyGroup.Builder()
                    .setName(DELETE_CONFIG_GROUP)
                    .setConfigProperties(ImmutableList.of(deleteFilenameProp, deletePathProp,
                                                          deleteEmptyParentsProp));

            PluginConfig config = PluginConfig.builder().setGroup(configGroupBuilder).build();

            builder.config(config);

            delete(session, builder.build(), d);
        } else {
            Action.Builder builder = new Action.Builder();
            builder.name(WRITE_ACTION.getName());

            ConfigProperty.Builder writeRelativePathProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_RELATIVE_PATH);
            ConfigProperty.Builder writeBasePathProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_BASE_PATH);
            ConfigProperty.Builder writeFilenameProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_FILENAME);
            ConfigProperty.Builder writeStreamProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_STREAM);
            ConfigProperty.Builder writeOwnerSidProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_OWNER_SID);
            ConfigProperty.Builder writeGroupSidProp = new ConfigProperty.Builder(
                    PROPERTY_WRITE_GROUP_SID);

            writeRelativePathProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_RELATIVE_PATH));
            writeBasePathProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_BASE_PATH));
            writeFilenameProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_FILENAME));
            writeStreamProp.setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_STREAM));
            writeOwnerSidProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_OWNER_SID));
            writeGroupSidProp
                    .setValue(action.getConfig().getPropertyValue(PROPERTY_OUTPUT_GROUP_SID));

            ConfigPropertyGroup.Builder configGroupBuilder = new ConfigPropertyGroup.Builder()
                    .setName(WRITE_CONFIG_GROUP)
                    .setConfigProperties(ImmutableList
                            .of(writeRelativePathProp, writeBasePathProp, writeFilenameProp,
                                writeStreamProp, writeOwnerSidProp, writeGroupSidProp));

            PluginConfig config = PluginConfig.builder().setGroup(configGroupBuilder).build();

            builder.config(config);

            write(session, builder.build(), d);
        }
    }

    /**
     * Deletes empty parent directories of the given file, up to connector's base path.
     *
     * @param share
     * @param path to delete parents of
     * @throws PluginOperationFailedException
     */
    private void deleteParentDirectories(DiskShare share, String pathToDelete)
            throws PluginOperationFailedException {
        Path path = Paths.get(pathToDelete);
        Path parent = path.getParent();
        while (parent != null && !parent.toString().equals(baseFolder)) {
            log.debug("Deleting directory {}", parent);
            try {
                share.rmdir(removeStartingSlash(parent.toString()), false);
            } catch (SMBApiException e) {
                if (NtStatus.STATUS_DIRECTORY_NOT_EMPTY.equals(e.getStatus())) {
                    // We reached a parent directory that is not empty. Get out of here.
                    log.debug("Delete of directory {} failed because it is not empty", parent, e);
                    break;
                } else if (isFileNotFound(e.getStatus())) {
                    // Already deleted? Treat as success
                    log.debug("Delete of directory {} failed because it is not there. Status: {}",
                              parent, e.getStatus().name(), e);
                } else {
                    throw new PluginOperationFailedException(e);
                }
            } catch (SMBRuntimeException e) {
                throw new PluginOperationFailedException(e);
            }
            parent = parent.getParent();
        }
    }

    /**
     * Creates parent directories for the given path.
     *
     * @param path path to create parent directories for
     * @param ownerSid owner to set on the created directories, or null if owner should not be set
     * @param groupSid group to set on the created directories, or null if group should not be set
     * @throws PluginOperationFailedException
     */
    private void createParentDirectory(DiskShare share, Path path, String ownerSid, String groupSid)
            throws PluginOperationFailedException {
        if (path != null && path.getParent() != null
                && !share.folderExists(removeStartingSlash(path.getParent().toString()))) {
            createParentDirectory(share, path.getParent(), ownerSid, groupSid);
        }
        if (path != null && !share.folderExists(removeStartingSlash(path.toString()))) {
            log.debug("Creating directory {}", path);
            String tmpPath = removeStartingSlash(path.toString());

            try {
                share.mkdir(tmpPath);
                setOwnership(share, tmpPath, ownerSid, groupSid);
            } catch (SMBApiException e) {
                if (NtStatus.STATUS_OBJECT_NAME_COLLISION.equals(e.getStatus())) {
                    // The directory we are trying to create already exists
                    // This can happen due to races between multiple workflow executors
                    return;
                }
                throw new PluginOperationFailedException(e);
            } catch (SMBRuntimeException e) {
                throw new PluginOperationFailedException(e);
            }
        }
    }

    private void setOwnership(DiskShare share, String path, String owner, String group)
            throws PluginOperationFailedException {
        if (!Strings.isNullOrEmpty(owner) || !Strings.isNullOrEmpty(group)) {
            SID ownerSid = null;
            SID groupSid = null;
            try {
                ownerSid = !Strings.isNullOrEmpty(owner) ? SID.fromString(owner) : null;
                groupSid = !Strings.isNullOrEmpty(group) ? SID.fromString(group) : null;
            } catch (IllegalArgumentException e) {
                throw new PluginOperationFailedException(e);
            }
            ACL sacl = null;
            ACL dacl = null;

            Set<Control> controls = new HashSet<>();
            Set<SecurityInformation> securityInformation = new HashSet<>();

            if (ownerSid != null) {
                controls.add(Control.OD);
                securityInformation.add(SecurityInformation.OWNER_SECURITY_INFORMATION);
            }
            if (groupSid != null) {
                controls.add(Control.GD);
                securityInformation.add(SecurityInformation.GROUP_SECURITY_INFORMATION);
            }
            SecurityDescriptor securityDescriptor = new SecurityDescriptor(
                    controls, ownerSid, groupSid, sacl, dacl);
            try {
                share.setSecurityInfo(path, securityInformation, securityDescriptor);
            } catch (SMBRuntimeException e) {
                throw new PluginOperationFailedException("Failed to set owner/group SID on " + path,
                        e);
            }
        }
    }

    private File openFileForWrite(DiskShare share, String path, boolean overwrite) {
        try {
            return share
                    .openFile(path,
                              new HashSet<>(
                                      Arrays.asList(new AccessMask[] { AccessMask.GENERIC_ALL })),
                              null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_CREATE, null);
        } catch (SMBApiException ex) {
            if (ex.getStatus() != NtStatus.STATUS_OBJECT_NAME_COLLISION || !overwrite) {
                throw ex;
            }
        }

        return share
                .openFile(path,
                          new HashSet<>(Arrays.asList(new AccessMask[] { AccessMask.GENERIC_ALL })),
                          null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OVERWRITE, null);
    }

    private FileAllInformation getFile(String path, PluginSession session)
            throws PluginOperationFailedException {
        try {
            return getSession(session).getShare().getFileInformation(removeStartingSlash(path));
        } catch (IOException e) {
            throw new PluginOperationFailedException(e);
        } catch (SMBApiException e) {
            if (isFileNotFound(e.getStatus())) {
                throw new DocumentNotFoundException(
                        String.format("Document not found: \"%s\". Status: %s", path,
                                      e.getStatus().name()));
            }
            throw new PluginOperationFailedException(e);
        } catch (SMBRuntimeException e) {
            throw new PluginOperationFailedException(e);
        }
    }

    // DiskShare is very particular about not having a starting slash, even if you're just listing
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

    private boolean isSystem(FileIdBothDirectoryInformation f) {
        long b = (f.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_SYSTEM.getValue());
        return b != 0;
    }

    private boolean isHidden(FileIdBothDirectoryInformation f) {
        long b = (f.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_HIDDEN.getValue());
        return b != 0;
    }

    private Document getCifsMetadata(String path, PluginSession session)
            throws PluginOperationFailedException {
        try {
            FileAllInformation fileInfo = getFile(removeStartingSlash(path), session);
            SecurityDescriptor securityDescriptor = null;

            if (includeSecurityInfo) {
                securityDescriptor = this.getSession(session).getShare()
                        .getSecurityInfo(removeStartingSlash(path), SECURITY_INFO);
            }
            return makeDocument(Paths.get(path), isDirectory(fileInfo),
                                fileInfo.getBasicInformation().getChangeTime(),
                                fileInfo.getBasicInformation().getCreationTime(),
                                fileInfo.getBasicInformation().getLastAccessTime(),
                                fileInfo.getBasicInformation().getLastWriteTime(),
                                fileInfo.getStandardInformation().getEndOfFile(),
                                securityDescriptor);

        } catch (IOException e) {
            throw new PluginOperationFailedException(e);
        } catch (SMBApiException e) {
            if (isFileNotFound(e.getStatus())) {
                throw new DocumentNotFoundException(
                        String.format("Document not found: \"%s\". Status: %s", path,
                                      e.getStatus().name()));
            }
            throw new PluginOperationFailedException(e);
        } catch (SMBRuntimeException e) {
            throw new PluginOperationFailedException(e);
        }
    }

    private Document makeDocument(Path fullPath, boolean isDir, FileTime changeTime,
                                  FileTime createTime, FileTime lastAccessTime,
                                  FileTime lastWriteTime, long endOfFile,
                                  SecurityDescriptor securityDescriptor)
            throws PluginOperationFailedException {
        DocumentBuilder builder = callback.documentBuilder();
        if (isDir) {
            builder.setIsContainer(true).setHasContent(false);
        } else {
            builder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap());
            String version = makeVersion(changeTime, endOfFile);
            builder.addMetadata(StandardFields.VERSION,
                                StringDocumentFieldValue.builder().setString(version).build());
        }

        String filePath = fullPath.toString();
        String path = "";
        String name;

        if (fullPath.getParent() != null) {
            path = fullPath.getParent().toString();
        }
        // Empty path is the root of the share
        String id = filePath;

        // DiskShare doesn't use a forward slash for root, you just use an empty string. But we
        // can't save an empty string as an id or path so use a forward slash and we'll need to
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
        builder.addMetadata(StandardFields.ID,
                            StringDocumentFieldValue.builder().setString(id).build());
        builder.addMetadata(StandardFields.URI,
                            StringDocumentFieldValue.builder()
                                    .setString(encodeDecodePath(filePath, true))
                                    .build());
        builder.addMetadata(StandardFields.DISPLAY_NAME,
                            StringDocumentFieldValue.builder().setString(name).build());
        builder.addMetadata(StandardFields.FILENAME,
                            StringDocumentFieldValue.builder().setString(name).build());
        builder.addMetadata(StandardFields.SIZE,
                            LongDocumentFieldValue.builder()
                                    .setLong(endOfFile)
                                    .build());
        builder.addMetadata(StandardFields.PATH,
                            StringDocumentFieldValue.builder().setString(fullPath.toString())
                                    .build());
        builder.addMetadata(StandardFields.RELATIVE_PATH,
                            StringDocumentFieldValue.builder()
                                    .setString(getRelativePath(baseFolder, path))
                                    .build());
        if (changeTime != null) {
            builder.addMetadata(CHANGED_DATE_MILLIS, LongDocumentFieldValue.builder()
                    .setLong(changeTime.toEpochMillis()).build());
            builder.addMetadata(CHANGED_DATE_STRING, StringDocumentFieldValue.builder()
                    .setString(Instant.ofEpochMilli(changeTime.toEpochMillis()).toString())
                    .build());
        }
        if (createTime != null) {
            builder.addMetadata(StandardFields.CREATED_DATE_MILLIS, LongDocumentFieldValue.builder()
                    .setLong(createTime.toEpochMillis()).build());
            builder.addMetadata(StandardFields.CREATED_DATE_STRING,
                                StringDocumentFieldValue.builder().setString(Instant
                                        .ofEpochMilli(createTime.toEpochMillis()).toString())
                                        .build());
        }
        if (lastAccessTime != null) {
            builder.addMetadata(StandardFields.ACCESS_TIME_MILLIS, LongDocumentFieldValue.builder()
                    .setLong(lastAccessTime.toEpochMillis()).build());
            builder.addMetadata(StandardFields.ACCESS_TIME_STRING,
                                StringDocumentFieldValue.builder().setString(Instant
                                        .ofEpochMilli(lastAccessTime.toEpochMillis()).toString())
                                        .build());
        }
        if (lastWriteTime != null) {
            builder.addMetadata(StandardFields.MODIFIED_DATE_MILLIS, LongDocumentFieldValue
                    .builder().setLong(lastWriteTime.toEpochMillis()).build());
            builder.addMetadata(StandardFields.MODIFIED_DATE_STRING,
                                StringDocumentFieldValue.builder().setString(Instant
                                        .ofEpochMilli(lastWriteTime.toEpochMillis()).toString())
                                        .build());
        }

        if (securityDescriptor != null) {
            builder.addMetadata(StandardFields.OWNER,
                                StringDocumentFieldValue.builder()
                                        .setString(securityDescriptor.getOwnerSid().toString())
                                        .build());
            builder.addMetadata(StandardFields.GID,
                                StringDocumentFieldValue.builder()
                                        .setString(securityDescriptor.getGroupSid().toString())
                                        .build());
        }

        return builder.build();
    }

    private String makeVersion(FileTime changeTime, long allocationSize) {
        return changeTime.toEpochMillis() + " " + allocationSize;
    }

    private String encodeDecodePath(String path, boolean encode)
            throws PluginOperationFailedException {
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

        public CifsSession(String host, String username, String password, String domain,
                String shareName) throws IOException {
            this.host = host;
            this.shareName = shareName;

            if (Strings.isNullOrEmpty(username)) {
                // Just a convenience method for AuthenticationContext("", new char[0], null)
                ac = AuthenticationContext.anonymous();
            } else {
                ac = new AuthenticationContext(username, password.toCharArray(),
                        domain);
            }

            share = connectShare();
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

    private boolean isFileNotFound(NtStatus status) {
        switch (status) {
            case STATUS_OBJECT_NAME_NOT_FOUND:
            case STATUS_OBJECT_PATH_NOT_FOUND:
            case STATUS_DELETE_PENDING:
            case STATUS_NOT_FOUND:
            case STATUS_NO_SUCH_FILE:
                return true;
            default:
                return false;
        }
    }
}
