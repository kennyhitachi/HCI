package nl.rabobank.comet.generator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InvalidClassException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.TimeZone;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import nl.rabobank.comet.BloombergItem;
import nl.rabobank.comet.generator.ReutersTransformGenerator.ForcedStopSAXException;
import nl.rabobank.comet.generator.utils.BloombergConversationMetadata;
import nl.rabobank.comet.generator.utils.BloombergMessageMetadata;
import nl.rabobank.comet.generator.utils.BloombergMetadataBaseContainer;
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
import com.hds.hcp.tools.comet.generator.BaseGeneratorProperties;
import com.hds.hcp.tools.comet.generator.BaseMetadataGenerator;
import com.hds.hcp.tools.comet.generator.CustomMetadataContainer;
import com.hds.hcp.tools.comet.generator.ObjectContainer;
import com.hds.hcp.tools.comet.generator.SystemMetadataContainer;
import com.hds.hcp.tools.comet.utils.RegExprFilenameFilter;
import com.hds.hcp.tools.comet.utils.RegExprMatcher;
import com.hds.hcp.tools.comet.utils.StaticUtils;
import com.hds.hcp.tools.comet.utils.URIWrapper;

public class BloombergTransformGenerator extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");
	protected SimpleDateFormat mOutputChildFolderDateFormat = new SimpleDateFormat("MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	LinkedList<RegExprMatcher> mMetadataFileSpecs;

	/*
	 * Construct a private properties class to construct module specific information.
	 */
	private class BloombergTransformGeneratorProperties extends BaseGeneratorProperties {
		public BloombergTransformGeneratorProperties() {
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
		private LinkedList<RegExprMatcher> mConversationFilesCache;
		private LinkedList<RegExprMatcher> mMessageFilesCache;
		private LinkedList<RegExprMatcher> mAttachmentFilesCache;

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
		public LinkedList<RegExprMatcher> getMetadataAttachToFiles() {
			LinkedList<RegExprMatcher> retval = mAttachToFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				retval = mAttachToFilesCache = new LinkedList<RegExprMatcher>();  // Always at least have an empty list.

				// Add in Conversation File Cache, if we haven't do prior.
				if (null == mConversationFilesCache) {
					if (null != getMetadataConversationFiles()) {
						Iterator<RegExprMatcher> iter = mConversationFilesCache.iterator();
						while (iter.hasNext()) {
							mAttachToFilesCache.add(iter.next());
						}
					}
				}
				
				// Add in Message File Cache, if we haven't do prior.
				if (null == mMessageFilesCache) {
					if (null != getMetadataMessageFiles()) {
						Iterator<RegExprMatcher> iter = mMessageFilesCache.iterator();
						while (iter.hasNext()) {
							mAttachToFilesCache.add(iter.next());
						}
					}
				}
				
				// Add in Attachment File Cache, if we haven't do prior.
				if (null == mAttachmentFilesCache) {
					if (null != getMetadataAttachmentFiles()) {
						Iterator<RegExprMatcher> iter = mAttachmentFilesCache.iterator();
						while (iter.hasNext()) {
							mAttachToFilesCache.add(iter.next());
						}
					}
				}
			}
			
			return retval;
		}
		
		public LinkedList<RegExprMatcher> getMetadataConversationFiles() {
			LinkedList<RegExprMatcher> retval = this.mConversationFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				retval = mConversationFilesCache = new LinkedList<RegExprMatcher>();  // Always at least have an empty list.

				String propVal = StaticUtils.resolveEnvVars(mProps.getProperty("metadata.conversation.files"));
				if (null != propVal) {
					String values[] = propVal.split(",");
					for (int i = 0; i < values.length; i++) {
						mConversationFilesCache.add(new RegExprMatcher(values[i]));
					}
				}
			}
			
			return retval;
		}
		
		
		public boolean isConversationFile(File inFile) {
			boolean retval = false;
			
			String fileName = inFile.getParentFile().getName() + File.separator + inFile.getName();

			if (null != mConversationFilesCache) {
				Iterator<RegExprMatcher> iter = mConversationFilesCache.iterator();
				while (iter.hasNext()) {
					if ( iter.next().isMatch(fileName) ) {
						retval = true;
						break;
					}
				}
			}
			
			return retval;
		}
		
		public LinkedList<RegExprMatcher> getMetadataMessageFiles() {
			LinkedList<RegExprMatcher> retval = this.mMessageFilesCache;
			
			// Only build it once during the execution.
			if (null == retval) {
				retval = mMessageFilesCache = new LinkedList<RegExprMatcher>();  // Always at least have an empty list.

				String propVal = StaticUtils.resolveEnvVars(mProps.getProperty("metadata.message.files"));
				if (null != propVal) {
					String values[] = propVal.split(",");
					for (int i = 0; i < values.length; i++) {
						mMessageFilesCache.add(new RegExprMatcher(values[i]));
					}
				}
			}
			
			return retval;
		}
		
		public boolean isMessageFile(File inFile) {
			boolean retval = false;
			
			String fileName = inFile.getParentFile().getName() + File.separator + inFile.getName();

			if (null != mMessageFilesCache) {
				Iterator<RegExprMatcher> iter = mMessageFilesCache.iterator();
				while (iter.hasNext()) {
					if ( iter.next().isMatch(fileName) ) {
						retval = true;
						break;
					}
				}
			}
			
			return retval;
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
		
		public boolean isAttachmentFile(File inFile) {
			boolean retval = false;
			
			String fileName = inFile.getParentFile().getName() + File.separator + inFile.getName();

			if (null != mAttachmentFilesCache) {
				Iterator<RegExprMatcher> iter = mAttachmentFilesCache.iterator();
				while (iter.hasNext()) {
					if ( iter.next().isMatch(fileName) ) {
						retval = true;
						break;
					}
				}
			}
			
			return retval;
		}
		
		public String getMetadataAttachmentEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.attachment.enclosingTag"));
		}

		public TimeZone getOutputDateTimeZone() {
			return TimeZone.getTimeZone(mProps.getProperty("destination.outputDateTimeZone", "GMT"));
		}
	}
	
	/****
	 **** SAX Parser Events for extracting User entries for under Participant tag.
	 ****/
	
	BloombergMetadataBaseContainer mMetadataInfo;
	
	// This class processes the SAX events for an generic XML file.
	//  The core idea behind this event handler is create separate XML files
	//  with the tag and all its sub elements.
	private class MetadataInfoSAXParserEvents extends DefaultHandler {

		StringBuilder sDateValue;
		StringBuilder sRoomID;  // Special field for Conversation
		Stack<String> mScopeStack = new Stack<String>();
		
		public void startElement(String namespaceURI, String localName, String qName, Attributes attrs) 
		   throws SAXException {
			
			// Did we find the tag we are looking for?
			if ( qName.equals(mMetadataInfo.getFieldName())) {
				sDateValue = new StringBuilder();
			} else if ( qName.equals(BloombergConversationMetadata.ROOM_ID)){
				sRoomID = new StringBuilder();
			} else {
				mScopeStack.push(qName);
			}
		}
		
		public void endElement(String namespaceURI, String localName, String qName) {
			if (qName.equals(mMetadataInfo.getFieldName())) {
				if (null != sDateValue) {
					Long dateUTC = new Long(sDateValue.toString());
					
					try {
						mMetadataInfo.incrementItem(BloombergMetadataBaseContainer.COUNT_ID);
						mMetadataInfo.incrementItem(mScopeStack.peek() + "_" + BloombergMetadataBaseContainer.COUNT_ID);
						
						mMetadataInfo.minItem(BloombergMetadataBaseContainer.START_TIME_ID, dateUTC);
						mMetadataInfo.minItem(mScopeStack.peek() + "_" + BloombergMetadataBaseContainer.START_TIME_ID, dateUTC);

						mMetadataInfo.maxItem(BloombergMetadataBaseContainer.END_TIME_ID, dateUTC);
						mMetadataInfo.maxItem(mScopeStack.peek() + "_" + BloombergMetadataBaseContainer.END_TIME_ID, dateUTC);
					} catch (InvalidClassException e) {
						logger.fatal("Unexpected Failure during metadata processing", e);
					} finally {
						sDateValue = null;  // Don't need this and is a indicator for characters function.
					}
				}
			} else if ( qName.equals(BloombergConversationMetadata.ROOM_ID)) {
				if ( null != sRoomID )
					try {
						mMetadataInfo.setItem(qName, sRoomID.toString());
					} catch (InvalidClassException e) {
						logger.fatal("Unexpected Failure during metadata processing", e);
					} finally {
						sRoomID = null;
					}
			} else {
				mScopeStack.pop();
			}
		}
		
		// Function that receives tag value characters.
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (null != sDateValue) {
				sDateValue.append(new String(ch, start, length));
			}
			if (null != sRoomID) {
				sRoomID.append(new String(ch, start, length));
			}
		}
	}
	
	BloombergTransformGeneratorProperties mProps = new BloombergTransformGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();

	// This method will parse the input file and append on metadata for the specific type
	private void AppendMetadataInfo(BloombergItem inItem, StringBuilder inMetadataString) throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		File srcFile = inItem.getFile();

		// Setup mMetaData Structure for the kind of file we have.
		if (mProps.isMessageFile(srcFile)) {
			mMetadataInfo = new BloombergMessageMetadata(mOutputMetadataDateFormat);
		} else {
			mMetadataInfo = new BloombergConversationMetadata(mOutputMetadataDateFormat);
		}

		/*
		 *  Now feed the File through the parser to build the Metadata Info.
		 */
		
		XMLReader xmlReader = null;
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(srcFile);
			
			xmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			
			xmlReader.setContentHandler(new MetadataInfoSAXParserEvents());
			
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

		File metadataFile = inItem.getMetadataFile();
		StringBuilder trailingMetadata = new StringBuilder();
		if (null != metadataFile && metadataFile.exists()) {
            try {
            	List<String> metadataLines = Files.readAllLines(Paths.get(metadataFile.getAbsolutePath()), Charset.defaultCharset());
            	for (String oneline : metadataLines) {
            		if ( ! oneline.trim().startsWith("<?xml") ) {
                		trailingMetadata.append(oneline);
                		trailingMetadata.append("\n");
            		}
            	}
			} catch (IOException e) {
				logger.fatal("Unexpected Exception trying to read metadata file", e);

				StaticUtils.TRACE_METHOD_EXIT(logger);
				throw e;
			}
		}

		// Add the metadata to the inMetdataString StringBuilder.
		inMetadataString.append(mMetadataInfo.generateXML(trailingMetadata.toString()));

		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	// Based on the configuration this routine will add all files in the configuration to the metadata.
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

	public void initialize() throws Exception { 
		// Verify that the mapper is functional.
		mRegionMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputChildFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		
		mMetadataFileSpecs = mProps.getMetadataAttachToFiles();

		return; 
	}
	
	public LinkedList<ObjectContainer> getMetadataList(BaseWorkItem inItem) {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		// To work properly here, we must have an HCPItem.  Make sure we do.
		if ( ! (inItem instanceof BloombergItem) ) {
			logger.fatal("Unexpected object type passed in to getMetadataList. Expected {} Received {}",
					BloombergItem.class.getName(), inItem.getClass().getName());

			// TODO:  Need something here...  Probably need to think about returning an exception???
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		BloombergItem thisItem = (BloombergItem)inItem;

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
			logger.debug("Item not for generator. Path identifier ({}) is not contained in item path: {}", 
					pathIdentifier, fileAbsolutePath );
			return null;
		}
		
		// The item is ours, so let's see about setting up the credentials and system metadata based on the mapping.
		
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

		
		File inSrcFile = thisItem.getFile();
		File inBaseFolder = (File)thisItem.getBaseSpecification();
		
		// Build out the destination for the one item.
		//
		
		// TimeZone of input datetime is taken from region Config.
		mInputDateFormat.setTimeZone(regionConfig.getTimeZone());
		mOutputMetadataDateFormat.setTimeZone(regionConfig.getTimeZone());

		Date collectionDate = null;  // Will be used both below and further down for creating metadata.
		File accountFolder = null;  // Will be squirreling this away for adding to metadata.
		
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
		
		// Formulate the destination object path based on source file path, base folder path,
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

		/**
		 *  Build out the Custom Metadata
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
			logger.debug("Custom Metadata being generated.");

			// Time to start building the custom metadata.
			CustomMetadataContainer customMeta = retObject.getCustomMetadata();
			
			StringBuilder metadataContent = new StringBuilder();
			metadataContent.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			metadataContent.append("<" + mProps.getMetadataEnclosingTag() + ">\n");

			//
			// Put in the basic Collection Info
			//
			metadataContent.append("  <CollectionInfo>\n");
			metadataContent.append("    <Region>" + regionConfig.getIdentifier() + "</Region>\n");
			metadataContent.append("    <DateTime>" + mOutputMetadataDateFormat.format(collectionDate) + "</DateTime>\n");
			metadataContent.append("    <Account>" + accountFolder.getName() + "</Account>\n");
			metadataContent.append("  </CollectionInfo>\n");

			// Append first level metadata include files.
			try {
				AppendIncludeFiles(mProps.getMetadataIncludeFiles(), inSrcFile, metadataContent);
			} catch (IOException e) {
				logger.fatal("Unexpected Exception", e);
				StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
				
				return null;  // Won't be writing file.
			}

			// Processing for Attachment metadata.
			if ( mProps.isAttachmentFile(inSrcFile) ) {
				if (null != mProps.getMetadataAttachmentEnclosingTag()) {
					metadataContent.append("  <" + mProps.getMetadataAttachmentEnclosingTag() + ">\n");
				}
				
				File metadataFile = thisItem.getMetadataFile();
				if (null != metadataFile && metadataFile.exists()) {
					String trailingMetadata = null;
		            try {
						trailingMetadata = new String(Files.readAllBytes(Paths.get(metadataFile.getAbsolutePath())));
					} catch (IOException e) {
						logger.fatal("Unexpected Exception trying to read metadata file", e);

						StaticUtils.TRACE_METHOD_EXIT(logger);
						return null;  // Won't be writing file.
					}
		            
		            metadataContent.append(trailingMetadata);
				}

				if (null != mProps.getMetadataAttachmentEnclosingTag()) {
					metadataContent.append("  </" + mProps.getMetadataAttachmentEnclosingTag() + ">\n");
				}
			} else {
				//
				// Processing message and conversation files.
				//
				// Collect the count and date information for non attachment files.
				try {
					
					if (mProps.isMessageFile(inSrcFile) || mProps.isConversationFile(inSrcFile)) {
						
						File attachmentmetadataFile = thisItem.getAttachmentMetadataFile();
						if (null != attachmentmetadataFile && attachmentmetadataFile.exists()) {
							String trailingMetadata = null;
				            try {
								trailingMetadata = new String(Files.readAllBytes(Paths.get(attachmentmetadataFile.getAbsolutePath())));
							} catch (IOException e) {
								logger.fatal("Unexpected Exception trying to read attachment metadata file", e);

								StaticUtils.TRACE_METHOD_EXIT(logger);
								return null;  // Won't be writing file.
							}
				            
				            metadataContent.append(trailingMetadata);
						}
					}
					
					AppendMetadataInfo(thisItem, metadataContent);
				} catch (Exception e1) {
					logger.fatal("Unexpected Failure", e1);
					
					return null;
				}
			}
			
			//
			// Finish up closing the top tag and committing the metadata.
			//
			metadataContent.append("</" + mProps.getMetadataEnclosingTag() + ">\n");
			
			customMeta.put(metadataContent.toString());
		}

		/*
		 * Set the core system metadata for the object.
		 */
		LinkedList<ObjectContainer> retval = new LinkedList<ObjectContainer>();
		
		SystemMetadataContainer sysMeta = retObject.getSystemMetadata();

		// Yes. Use the credentials from properties file.
		sysMeta.setCredentials(regionConfig.getEncodedUserName(), regionConfig.getEncodedPassword());
		
		// Put the base object into the linked list to be returned.
		retval.add(retObject);
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return retval;
	}
}
