/*
 *   Copyright (c) 2012 Hitachi Data Systems, Inc.
 *
 *   Permission is hereby granted to  this software and associated
 *   documentation files (the "Software"), subject to the terms and
 *   conditions of the Sample Source Code License (SSCL) delivered
 *   with this Software. If you do not agree to the terms and
 *   conditions of the SSCL,
 *
 *     (i)  you must close this file and delete all copies of the
 *          Software, and
 *     (ii) any permission to use the Software is expressly denied.
 *
 * Disclaimer: This code is only a sample and is provided for educational purposes.
 * The consumer of this sample assumes full responsibility for any effects due to
 * coding errors.
 * 
 */
package nl.rabobank.comet.generator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TimeZone;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.FileSystemItem;
import com.hds.hcp.tools.comet.generator.BaseGeneratorProperties;
import com.hds.hcp.tools.comet.generator.BaseMetadataGenerator;
import com.hds.hcp.tools.comet.generator.CustomMetadataContainer;
import com.hds.hcp.tools.comet.generator.ObjectContainer;
import com.hds.hcp.tools.comet.generator.SystemMetadataContainer;
import com.hds.hcp.tools.comet.utils.RegExprFilenameFilter;
import com.hds.hcp.tools.comet.utils.RegExprMatcher;
import com.hds.hcp.tools.comet.utils.StaticUtils;
import com.hds.hcp.tools.comet.utils.URIWrapper;

public class ReutersTransformGenerator extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	ReutersTransformGeneratorProperties mProps = new ReutersTransformGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();

	LinkedList<String> mUserList; // Used to populate users when parsing files.

	LinkedList<RegExprMatcher> mMetadataFileSpecs;
	LinkedList<RegExprMatcher> mAttachmentFileSpecs;

	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmZ");
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");
	protected SimpleDateFormat mOutputChildFolderDateFormat = new SimpleDateFormat("MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	/*
	 * Construct a private properties class to construct module specific information.
	 */
	private class ReutersTransformGeneratorProperties extends BaseGeneratorProperties {
		public ReutersTransformGeneratorProperties() {
			super();
		}

		public String getSourcePathIdentifier() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("source.pathIdentifier"));
		}

		public String[] getSourceFilePrefixes() {
			String[] retvals = null;
			
			String inputString = StaticUtils.resolveEnvVars(mProps.getProperty("source.filePrefixes"));
			if (null != inputString) {
				retvals = inputString.trim().split(",");
			}
			
			return retvals;
		}

		public String getRegionFolderPostFix() {
			return mProps.getProperty("source.regionFolderPostFix",  "_data");
		}
		
		private LinkedList<RegExprMatcher> mAttachToFilesCache;
		private LinkedList<RegExprMatcher> mIncludeFilesCache;
		private LinkedList<RegExprMatcher> mAttachmentFilesCache;

		public LinkedList<RegExprMatcher> getMetadataAttachToFiles() {
			LinkedList<RegExprMatcher> retval = mAttachToFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				retval = mAttachToFilesCache = new LinkedList<RegExprMatcher>();  // Always at least have an empty list.

				String propVal = StaticUtils.resolveEnvVars(mProps.getProperty("metadata.files"));
				if (null != propVal) {
					String values[] = propVal.split(",");
					for (int i = 0; i < values.length; i++) {
						mAttachToFilesCache.add(new RegExprMatcher(values[i]));
					}
				}
			}
			
			return retval;
		}
		
		public String getMetadataEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.enclosingTag", "TopTag"));
		}

		public LinkedList<RegExprMatcher> getMetadataIncludeFiles() {
			LinkedList<RegExprMatcher> retval = mIncludeFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				String propVal = StaticUtils.resolveEnvVars(mProps.getProperty("metadata.includeFiles"));

				if (null != propVal) {
					mIncludeFilesCache = new LinkedList<RegExprMatcher>();

					retval = new LinkedList<RegExprMatcher>();
					
					String values[] = propVal.split(",");
					for (int i = 0; i < values.length; i++) {
						mIncludeFilesCache.add(new RegExprMatcher(values[i]));
					}
				}
				
				retval = mIncludeFilesCache;
			}
			
			return retval;
		}

		public boolean getMetadataIncludeContainingUsersInfo() {
			return mProps.getProperty("metadata.containingUsers", "false").equals("true") ? true : false;
		}
		
		public String getMetadataAttachmentEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.attachment.enclosingTag", "TopTag"));
		}

		public String getAttachmentMetadataFilePostfix() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.attachment.metadataFilePostFix", "_Metadata"));
		}
		
		public String getMessageMetadataFileExtension() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.message.metadataFileExt", "att"));
		}

		public LinkedList<RegExprMatcher> getMetadataAttachmentFiles() {
			LinkedList<RegExprMatcher> retval = this.mAttachmentFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				retval = mAttachmentFilesCache = new LinkedList<RegExprMatcher>();  // Always at least have an empty list.

				String propVal = StaticUtils.resolveEnvVars(mProps.getProperty("metadata.attachment.files"));
				if (null != propVal) {
					String values[] = propVal.split(",");
					for (int i = 0; i < values.length; i++) {
						mAttachmentFilesCache.add(new RegExprMatcher(values[i]));
					}
				}
			}
			
			return retval;
		}
		
		public TimeZone getOutputDateTimeZone() {
			return TimeZone.getTimeZone(mProps.getProperty("destination.outputDateTimeZone", "GMT"));
		}
	}
	
	public class ForcedStopSAXException extends SAXException {

		private static final long serialVersionUID = -8880284669721877221L;

		public ForcedStopSAXException(String arg0) {
			super(arg0);
		}
	}
	
	/****
	 **** SAX Parser Events for extracting User entries for under Participant tag.
	 ****/
	
	// This class processes the SAX events for an generic XML file.
	//  The core idea behind this event handler is create separate XML files
	//  with the tag and all its sub elements.
	private class UserInfoSAXParserEvents extends DefaultHandler {

		boolean bInParentTag = true;
		StringBuilder sUserValue;
		String sParentTag;
		
		UserInfoSAXParserEvents(String inParentTag) {
			super();
			sParentTag = inParentTag;
			if ( null != inParentTag )
			    bInParentTag = false;
		}
		
		public void startElement(String namespaceURI, String localName, String qName, Attributes attrs) 
		   throws SAXException {
			
			// Did we find the tag we are looking for?
			if ( null != sParentTag && qName.equals(sParentTag)) {
				bInParentTag = true;
				mUserList = null;  // Start with a fresh list with every parse attempt.
			} else if (bInParentTag && qName.equals("User")) {
				// There is a User, so if we don't have one yet, allocate a user list.
				if (null == mUserList) {
					mUserList = new LinkedList<String>();
				}
				sUserValue = new StringBuilder();  // Create the string buffer to put user value in.
			}
		}
		
		public void endElement(String namespaceURI, String localName, String qName) throws ForcedStopSAXException {
			if (bInParentTag) {
				if (null != sParentTag && qName.equals(sParentTag)) {
					bInParentTag = false;
					
					throw new ForcedStopSAXException("Finished Parent Tag");
					
				} else if (qName.equals("User")) {
					mUserList.add(sUserValue.toString());
					
					sUserValue = null;
				}
			}
		}
		
		// Function that receives tag value characters.
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (null != sUserValue) {
				sUserValue.append(new String(ch, start, length));
			}
		}
	}
	
	private void AppendIncludeFiles(LinkedList<RegExprMatcher> inFileList, File inSrcFile, StringBuilder metadataContent) throws IOException {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		if (null != inFileList) {
			Iterator<RegExprMatcher> includeIter = inFileList.iterator();
			
			File fileFolder = inSrcFile.getParentFile();
			while (includeIter.hasNext()) {
				String files[] = fileFolder.list(new RegExprFilenameFilter(includeIter.next()));
				
				if (null != files) {
					for (int i = 0; i < files.length; i++) {
						File thisFile = new File(inSrcFile.getParentFile(), files[i]);
						
						logger.debug("Appending file to custom metadata: {}", thisFile.getName());
						
						BufferedReader reader = null;
						try {
							reader = new BufferedReader(new FileReader(thisFile));
							String line;
							while ((line = reader.readLine()) != null) {
								if (! line.startsWith("<?xml")) {
									metadataContent.append(line + "\n");
								}
							}
						} catch (IOException e) {
							logger.fatal("Unexpected failure reading metadata inclusion file.", e);

							StaticUtils.TRACE_METHOD_EXIT(logger);
							throw e;
						} finally {
							try {
								if (null != reader) reader.close();
							} catch (IOException e) {
								// Just eat it.
							}
						}
					}
				}
			}
		}
	}

	private void PopulateUserList(File inSrcFile, String inParentTag) throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		mUserList = null; // Blank it out.
		
		/*
		 *  Now feed the Chat file through the parser to build the User List.
		 */
		
		XMLReader xmlReader = null;
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(inSrcFile);
			
			xmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			
			xmlReader.setContentHandler(new UserInfoSAXParserEvents(inParentTag));
			
			// Do the SAX Parsing.
			xmlReader.parse(new InputSource(inputStream));
		} catch (ForcedStopSAXException e2) {
			logger.debug("Found end of User list in file");
		} catch (SAXException | IOException | ParserConfigurationException e) {
			logger.fatal("Unexpected Exception when populating the user list");
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			throw e;
		} finally {
			if (null != inputStream) {
				inputStream.close();
			}
		}

		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	private void AppendUserList(File inSrcFile, StringBuilder metadataContent, String parentTag) throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		/*
		 * First Collect the users in the file.
		 */
		PopulateUserList(inSrcFile, parentTag);
		
		if (null != mUserList) {
			Iterator<String> userIter = mUserList.iterator();
			while (userIter.hasNext()) {
				File thisFile = new File(inSrcFile.getParentFile().getParentFile(), "UserInfo_" + userIter.next() + ".xml");

				logger.debug("Appending file to custom metadata: {}", thisFile.getName());
				
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(thisFile));
					String line;
					while ((line = reader.readLine()) != null) {
						if ( ! line.startsWith("<?xml") ) {
							metadataContent.append(line + "\n");
						}
					}
				} catch (IOException e) {
					logger.fatal("Unexpected failure reading metadata inclusion file.", e);

					StaticUtils.TRACE_METHOD_EXIT(logger);
					throw e;
				} finally {
					try {
						if (null != reader) reader.close();
					} catch (IOException e) {
						// Just eat it.
					}
				}
			}
		}
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	public void initialize() throws Exception { 
		// Verify that the mapper is functional.
		mRegionMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputChildFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		
		mMetadataFileSpecs = mProps.getMetadataAttachToFiles();
		mAttachmentFileSpecs = mProps.getMetadataAttachmentFiles();

		return; 
	}
	
	public LinkedList<ObjectContainer> getMetadataList(BaseWorkItem inItem) {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		// To work properly here, we must have an HCPItem.  Make sure we do.
		if ( ! (inItem instanceof FileSystemItem) ) {
			logger.fatal("Unexpected object type passed in to getMetadataList. Expected " 
		                 + FileSystemItem.class.getName() + " Received " + inItem.getClass().getName());

			// TODO:  Need something here...  Probably need to think about returning an exception???
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		FileSystemItem thisItem = (FileSystemItem)inItem;

		// Verify that this object is one for this generator.  This is done by making sure the pathIdentifier
		//   is in the file path.
		String pathIdentifier = mProps.getSourcePathIdentifier();
		
		if (null == pathIdentifier) {
			logger.fatal("source.pathIdentifier is not set for this generator.");
			
			// TODO:  Need something here...  Probably need to think about returning an exception???

			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}

		String fileAbsolutePath = thisItem.getFile().getAbsolutePath();
		if (-1 == fileAbsolutePath.indexOf(pathIdentifier)) {
			logger.debug("Item not for generator. Path identifier (" + pathIdentifier + ") is not contained in item path: " + fileAbsolutePath );
			return null;
		}
		
		/*
		 *  The item is ours, so let's see about setting up the credentials and system metadata based on the mapping.
		 */
		
		File inSrcFile = thisItem.getFile();
		File inBaseFolder = (File)thisItem.getBaseSpecification();
		
		// Go get the credentials based on the region mapping.
		// Either the grand parent or great-grand-parent folder has an appropriate code.
		RegionSpec regionConfig = null;

		String regionCode = null;
		File folder = thisItem.getFile().getParentFile().getParentFile().getParentFile(); // Start off with the grand parent folder.
		int tries = 2;
		do {
			String folderName = folder.getName();
			regionCode = folderName.substring(0, Math.max(folderName.lastIndexOf(mProps.getRegionFolderPostFix()), 0));
			
			try {
				regionConfig = mRegionMap.getMatch(regionCode);
			} catch (InvalidConfigurationException e) {
				logger.fatal("Unable to find region code in RegionMapper for item: " + thisItem.getName(), e);
				
				return null;
			}
			
			if (null != regionConfig) break;  // All done;

			folder = folder.getParentFile(); // Try its parent.
			tries--;
		} while (tries > 0);

		if (null == regionConfig) {
			logger.fatal("Unable to find region code ({}) in RegionMapper for item: {}", regionCode, thisItem.getName());
			
			return null;
		}

		logger.debug("Established Region Code ({})", regionCode);
		
		/**
		 * Build out the destination for the one item.
		 */

		// TimeZone of input datetime is taken from region Config.
		mInputDateFormat.setTimeZone(regionConfig.getTimeZone());
		mOutputMetadataDateFormat.setTimeZone(regionConfig.getTimeZone());
		
		Date collectionDate = null;  // Will be used both below and further down for creating metadata.
		File accountFolder = null;  // Will be squirrelling this away for adding to metadata.
		
		// First transform the collection folder.
		File parentFolder = inSrcFile.getParentFile();
		File grandParentFolder = inSrcFile.getParentFile().getParentFile();
		
		boolean hasSubFolder = false;
		String transformedCollectionFolders = null;
		try {
			collectionDate = mInputDateFormat.parse(parentFolder.getName());
			transformedCollectionFolders = mOutputYearFolderDateFormat.format(collectionDate)
					+ File.separator 
					+ mOutputChildFolderDateFormat.format(collectionDate);
			
			accountFolder = parentFolder.getParentFile();
		} catch (ParseException e) {
			try {
				hasSubFolder=true;
				
				collectionDate = mInputDateFormat.parse(grandParentFolder.getName());
				
				// Now try the grandparent folder.
				transformedCollectionFolders = mOutputYearFolderDateFormat.format(collectionDate)
						+ File.separator 
						+ mOutputChildFolderDateFormat.format(collectionDate);
				
				accountFolder = grandParentFolder.getParentFile();
			} catch (ParseException e2) {
				logger.fatal("Unable to formulate Date/Time from folder ({}) or ({})", parentFolder.getName(), grandParentFolder.getName());
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
			}
		}
		
		// First formulate the destination object path based on source file path, base folder path,
		//   and the settings in the properties file.
		//
		StringBuilder destFilePath = new StringBuilder();
		
		if (hasSubFolder) {
			destFilePath.append(inSrcFile.getParentFile().getParentFile().getParentFile().getAbsolutePath());
		} else {
			destFilePath.append(inSrcFile.getParentFile().getParentFile().getAbsolutePath());
		}
		destFilePath.append(File.separator);
		
		// Remove the initial path from the full path.
		if (0 == destFilePath.indexOf(inBaseFolder.getAbsolutePath())) {
			destFilePath.delete(0, inBaseFolder.getAbsolutePath().length() + File.separator.length());
		}
		
		// Add root path if specified in configuration.
		String rootPath = mProps.getDestinationRootPath();
		if (null != rootPath && ! rootPath.isEmpty()) {
			rootPath = rootPath.trim();
			if (rootPath.lastIndexOf(StaticUtils.HTTP_SEPARATOR) != ( rootPath.length() - 1 )) {
				rootPath += StaticUtils.HTTP_SEPARATOR;
			}
			destFilePath.insert(0,rootPath);
		}
		
		// Add back in the Trailing initial path, if indicated
		if (mProps.shouldAppendSourcePathTrailingFolder()) {
			destFilePath.insert(0, inBaseFolder.getName() + File.separator);
		}

		// Add in transformed collection folder.
		destFilePath.append(transformedCollectionFolders + File.separator);
		
		// Add back subfolder of collection, if any.
		if (hasSubFolder) {
			destFilePath.append(parentFolder.getName() + File.separator);
		}

		// Add back in file name, with region code, if applicable.
		String objectName = inSrcFile.getName();
		
		String[] prefixes = mProps.getSourceFilePrefixes();
		if ( null != prefixes && prefixes.length >= 1 ) {
			for (String onePrefix : prefixes) {
				if (null != onePrefix && ! onePrefix.isEmpty() && objectName.startsWith(onePrefix)) {
					// Construct new name
					String newObjectName = onePrefix + regionCode + "_" + objectName.substring(onePrefix.length());
					objectName = newObjectName;
					
					break;
				}
			}
		}
		destFilePath.append(objectName);
		
		// Need to be OS agnostic and replace any FS separators with HTTP separator, if it isn't the same.
		if (! File.separator.equals(StaticUtils.HTTP_SEPARATOR)) {
			int charIndex = destFilePath.indexOf(File.separator);
			while (-1 != charIndex) {
				destFilePath.replace(charIndex, charIndex + File.separator.length(), StaticUtils.HTTP_SEPARATOR);
				charIndex = destFilePath.indexOf(File.separator);
			}
		}
		
		// Make sure it isn't going to barf when passed into the Apache HTTP Client.
		ObjectContainer retObject = null;
		try {
			// Construct the destination root string paying attention as to whether we need to add
			//   an HTTP Separator or not.
			String hostName = regionConfig.getNamespace()
					+ "." + regionConfig.getTenant()
					+ "." + regionConfig.getHCPName();

			retObject = new ObjectContainer(new URIWrapper("https", hostName, "/rest/" + destFilePath), mProps);
		} catch(URISyntaxException e) {
			logger.fatal("URL formulation issue (Skipping)...", e);

			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		try {
			logger.debug("Constructed destination URI: {}", retObject.getSystemMetadata().toPathOnlyURI().toString());
		} catch (URISyntaxException e1) {
			// Best try.
		}
		

		/*
		 * Set the core system metadata for the object.
		 */
		LinkedList<ObjectContainer> retval = new LinkedList<ObjectContainer>();
		
		SystemMetadataContainer sysMeta = retObject.getSystemMetadata();

		// Yes. Use the credentials from properties file.
		sysMeta.setCredentials(regionConfig.getEncodedUserName(), regionConfig.getEncodedPassword());
		
		/*
		 * Build the custom metadata for the object
		 */
		
		boolean matchingFile = false;
		
		String fileName = inSrcFile.getParentFile().getName() + File.separator + inSrcFile.getName();
		Iterator<RegExprMatcher> attachIter = mMetadataFileSpecs.iterator();
		while (attachIter.hasNext()) {
			if ( attachIter.next().isMatch(fileName) ) {
				matchingFile = true;
				break;
			}
		}

		if (! matchingFile) {
			logger.debug("Custom Metadata will not be generated. File did match file configuration. {}", inSrcFile.getAbsolutePath());
		} else {
			// Determine if the current file is an attachment file.
			boolean isAttachment = false;
			Iterator<RegExprMatcher> attachmentFileIter = mAttachmentFileSpecs.iterator();
			while (attachmentFileIter.hasNext()) {
				if ( attachmentFileIter.next().isMatch(fileName) ) {
					isAttachment = true;
					break;
				}
			}
			
			logger.debug("Custom Metadata being generated (isAttachment: {}).", isAttachment);

			/*
			 *  Time to start building the custom metadata.
			 */
			CustomMetadataContainer customMeta = retObject.getCustomMetadata();
			
			StringBuilder metadataContent = new StringBuilder();
			metadataContent.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			metadataContent.append("<" + mProps.getMetadataEnclosingTag() + ">\n");

			metadataContent.append("  <CollectionInfo>\n");
			metadataContent.append("    <Region>" + regionConfig.getIdentifier() + "</Region>\n");
			metadataContent.append("    <DateTime>" + mOutputMetadataDateFormat.format(collectionDate) + "</DateTime>\n");
			metadataContent.append("    <Account>" + accountFolder.getName() + "</Account>\n");
			metadataContent.append("  </CollectionInfo>\n");

			//
			// Now put in the shared include file(s).
			//
			try {
				AppendIncludeFiles(mProps.getMetadataIncludeFiles(), inSrcFile, metadataContent);
			} catch (IOException e) {
				logger.fatal("Unexpected failure processing core include files", e);
				StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
				
				return null;  // Won't be writing file.
			}
			
			// If an attachment, all other metadata goes under this level.
			if (isAttachment) {
				metadataContent.append("  <" + mProps.getMetadataAttachmentEnclosingTag() + ">\n");
			} else {
				try {
				    LinkedList<RegExprMatcher> includeFiles = new LinkedList<RegExprMatcher>();
				    includeFiles.add(new RegExprMatcher(inSrcFile.getName().replaceAll("\\.xml", "\\\\") + "." + mProps.getMessageMetadataFileExtension()));
				    AppendIncludeFiles(includeFiles, inSrcFile, metadataContent);
				} catch (Exception e) {
					logger.fatal("Unexpected failure attempting to append Attachment Info List to metadata", e);
					StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
					
					return null;  // Won't be writing file.
				}
			}
				
			//
			// Now put in all the UserInfo information, if configured.
			//
			if (! isAttachment && mProps.getMetadataIncludeContainingUsersInfo()) {
				try {
					AppendUserList(inSrcFile, metadataContent, "Participants");
				} catch (Exception e) {
					logger.fatal("Unexpected failure attempting to append UserInfo List to metadata", e);
					StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
					
					return null;  // Won't be writing file.
				}
			}
			
			if (isAttachment) {
				try {
					LinkedList<RegExprMatcher> includeFiles = new LinkedList<RegExprMatcher>();
					includeFiles.add(new RegExprMatcher(inSrcFile.getName().replaceAll("\\.", "\\\\.") + mProps.getAttachmentMetadataFilePostfix()));
					AppendIncludeFiles(includeFiles, inSrcFile, metadataContent);
				} catch (IOException e) {
					logger.fatal("Unexpected failure processing Attachment include files", e);
					
					StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
					
					return null;  // Won't be writing file.
				}
				
				metadataContent.append("  </" + mProps.getMetadataAttachmentEnclosingTag() + ">\n");
			}
				

			//
			// Finish up closing the top tag and committing the metadata.
			//
			metadataContent.append("</" + mProps.getMetadataEnclosingTag() + ">\n");
			
			customMeta.put(metadataContent.toString());
		}

		// Put the base object into the linked list to be returned.
		retval.add(retObject);
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return retval;
	}
}
