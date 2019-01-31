package nl.rabobank.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.hds.hcp.tools.comet.utils.RegExprMatcher;
import com.hds.hcp.tools.comet.utils.StaticUtils;

public class BlmIMXMLTagToFilesProcessorImpl implements
		XMLTagProcessorInterface {
	
	private static Logger logger = LogManager.getLogger();

	XMLProcessorProperties mProps = new XMLProcessorProperties();

	final String DEFAULT_INPUT_DATE_FORMAT = "yyyy-MM-dd'T'HHmmssZ";
	SimpleDateFormat mOutputDateFolderFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmssZ");

	SimpleDateFormat mInputDateFormat;   // Set from properties info
	Boolean bQuietMode = false;          // Set from xargs["quite-mode"]
	Boolean bOverwriteOff = false;       // Set from xargs["overwrite-off"]
	Boolean bFormatXML = false;          // Set from xargs["format-xml"]
	Boolean bMultipleFiles = false;      // Set from xargs["multiple-files"]
	int mFileNumber = 0;
	String mTagName;                     // Set from xargs["tag-name"]
	Date mCollectionDate;                // Set from xargs["collection-date"]
	String mDestinationBaseFileName;     // Default from properties, overridden by xargs["output-name"]
	String mDestinationFileFirstPart;    // Derived from the mDestinationBaseFileName.
	String mDestinationFileSecondPart;    // Derived from the mDestinationBaseFileName.
	String mUnknownRegionName;           // Default from properties, overridden by xargs["failure-region"]+"_data"
	String mNoMapRegionName;             // Default from properties, overridden by xargs["nomap-region"]+"_data"
	File mSourceAttachmentsFolder;       // Set from xargs["source-attachments-folder"]
	File mDestinationRootFolder;         // Set from xargs["destination-root-folder"]

	MsgFileFilter mMsgFileFilter;        // Used to get listing of all message files.
	HashMap<String, String> mAccountRegionMap = new HashMap<String, String>();
	DocumentBuilder mDocumentBuilder;

	Boolean isInitialized = false;
	
	ByteArrayOutputStream mOutputStream;
	
	class XMLProcessorProperties {
		
		final File DEFAULT_FILE = new File("XMLProcessor.properties");
		static final String DEFAULT_FILENAME_PROPERTY = "nl.rabobank.tools.xmlprocessor.properties.file";

		private File mPropertiesFile = DEFAULT_FILE;
		protected Properties mProps;
		
		XMLProcessorProperties() {
			String propFile = System.getProperty(DEFAULT_FILENAME_PROPERTY);
			
			// If we got something from the environment, use it.
			if (null != propFile && 0 < propFile.length()) {
				mPropertiesFile = new File(propFile);
			}

			refresh();
		}
		
		XMLProcessorProperties(File inFile) {
			mPropertiesFile = inFile;
			
			refresh();
		}
		
		XMLProcessorProperties(String inFileNameProperty) {
			String propFile = System.getProperty(inFileNameProperty);
			
			// If we got something from the environment, use it.
			if (null != propFile && 0 < propFile.length()) {
				mPropertiesFile = new File(propFile);
			}
			
			refresh();
		}
		
		void refresh() {
			if (null == mPropertiesFile) {

				return;  // Don't have a file so do nothing.
			}
			
			if ( ! mPropertiesFile.exists() || ! mPropertiesFile.isFile() || ! mPropertiesFile.canRead() ) {
				System.err.printf("WARNING: Property file (%s) is not an existing readable regular file.\n", mPropertiesFile.getPath());
				return;
			}

			mProps = new Properties();

			FileInputStream propFileStream = null;
			try {
				propFileStream = new FileInputStream(mPropertiesFile);
				mProps.load(propFileStream);
			} catch (IOException e) {
				System.err.printf("ERROR: Failed to read properties file (%s). Reason: \"%s\"\n", mPropertiesFile.getPath(), e.getMessage());
				return;
			} finally {
				if (null != propFileStream) {
					try {
						propFileStream.close();
					} catch (IOException e) {
						// best try.
						logger.fatal("Failed to close InputStream", e);
					}
				}
			}
		}
		
		String getDestinationMessageFolderName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.message.folderName", "messages"));
		}
		
		String getDestinationMessageFileName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.message.fileName", "Message.xml"));
		}
		
		String getDestinationAttachmentFolderName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.attachment.folderName", "attachments"));
		}
		
		String getDestinationAttachmentMetadataPostFix() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.attachment.metadataFilePostFix", "_Metadata"));
		}
		
		String getDestinationUnknownRegionName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.unknownRegionName", "Unknown"));
		}
		
		String getDestinationNoMapRegionName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.noMapRegionName", "NoMap"));
		}
		
		String getDestinationUnknownAccountValue() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.unknownAccountValue", "Unknown"));
		}
		
		TimeZone getDestinationFolderDateTimeZone() {
			return TimeZone.getTimeZone(StaticUtils.resolveEnvVars(mProps.getProperty("destination.folderDateTimeZone", "UTC")));
		}
		
		String getRegionFolderPostFix() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("destination.regionFolderPostfix",  "_data"));
		}
		
		String getConfigCollectionDateFormat() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("config.collectionDateFormat", DEFAULT_INPUT_DATE_FORMAT));
		}
		
		TimeZone getConfigCollectionDateTimeZone() {
			return TimeZone.getTimeZone(StaticUtils.resolveEnvVars(mProps.getProperty("config.collectionDateTimeZone", "UTC")));
		}
		
		File getConfigAccountMappingFile() {
			return new File(StaticUtils.resolveEnvVars(mProps.getProperty("config.accountMappingFile", "AccountMapping.lst")));
		}
		
		String getFirmNumber() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("config.firmNumber"));
		}
		
		String getMetadataAttachmentEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.attachment.enclosingTag", "TopTag"));
		}

		String getExtensionMetadataParentTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("extension.BLMIM.metadata.parentTag"));
		}

		List<String> getExtensionMetadataFields() {
			List<String> retval = null;
			String rawValue = StaticUtils.resolveEnvVars(mProps.getProperty("extension.BLMIM.metadata.fieldList"));
			
			if (null != rawValue) {
				retval = Arrays.asList(rawValue.replace(" ", "").split(","));
			}

			return retval;
		}
		
		String getExtensionMetadataGroupByField() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("extension.BLMIM.metadata.groupByField"));
		}
		
		String getExtensionMetadataFilePostfix() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("extension.BLMIM.metadata.filePostfix", "_Metadata"));
		}
	}
	
	
	@Override
	public boolean initialize(String inParams) {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		isInitialized = false;

		mFileNumber = 1;
		
		try {
			mDocumentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch (ParserConfigurationException e2) {
			System.err.println("ERROR: Failed to construct a document builder.");

			logger.fatal("Failed to construct a document builder.", e2);
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return false;
		}

		mProps = new XMLProcessorProperties();
		
		// Build Account to Region map
		try {
			loadAccountRegionMap();
		} catch (IOException e1) {
			System.err.println("ERROR: Failed to load Region Mapping from file.");

			logger.fatal("Failed to load Region Mapping from file.", e1);
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return false;
		}

		// Setup "defaults"
		mUnknownRegionName = mProps.getDestinationUnknownRegionName();
		mNoMapRegionName = mProps.getDestinationNoMapRegionName();
		mDestinationBaseFileName = mProps.getDestinationMessageFileName();
		mSourceAttachmentsFolder = new File(".");
		mDestinationRootFolder = new File(".");  //  Current working directory

		// Setup Date Formatters from properties config.
		mInputDateFormat = new SimpleDateFormat(mProps.getConfigCollectionDateFormat());
		mInputDateFormat.setTimeZone(mProps.getConfigCollectionDateTimeZone());

		mOutputDateFolderFormat.setTimeZone(mProps.getDestinationFolderDateTimeZone());

		// Process Params if passed in.
		if (null != inParams) {

			String paramsArray[] = inParams.split(",");

			// Process each param.
			for (int i = 0; i < paramsArray.length; i++) {
				String parts[] = paramsArray[i].split("=");
				
				if (parts.length != 2) {
					System.err.printf("WARN: Encountered unexpected parameter (%s).\n", paramsArray[i]);
					
					logger.warn("Encountered unexpected parameter ({}).\n", paramsArray[i]);
					continue;
				}
				
				if (parts[0].trim().equals("output-name")) {
					String val = parts[1].trim();
					
					// This Processor does not understand "stdout", so don't use.
					if ( ! val.equals("-") ) {
						// Setup the message file parts and filter.
						String pieces[] = mDestinationBaseFileName.split("\\.");
						StringBuilder secondPart = new StringBuilder();
						for (int j = 1; j < pieces.length; j++) {
							secondPart.append(".");
							secondPart.append(pieces[j]);
						}
						mDestinationBaseFileName = parts[1].trim() + secondPart.toString();
					}
					continue;
				}
				
				if (parts[0].trim().equals("quiet-mode")) {
					bQuietMode = new Boolean(parts[1].trim());
					continue;
				}

				if (parts[0].trim().equals("overwrite-off")) {
					bOverwriteOff = new Boolean(parts[1].trim());
					continue;
				}
				
				if (parts[0].equals("overwrite-off")) {
					bOverwriteOff = new Boolean(parts[1].trim());
					continue;
				}
				
				if (parts[0].equals("multiple-files")) {
					bMultipleFiles = new Boolean(parts[1].trim());
					continue;
				}
				
				if (parts[0].trim().equals("format-xml")) {
					bFormatXML = new Boolean(parts[1].trim());
					continue;
				}
				
				if (parts[0].trim().equals("collection-date")) {
					try {
						mCollectionDate = mInputDateFormat.parse(parts[1].trim());
					} catch (ParseException e) {
						System.err.printf("ERROR: Failed to parse collection-date (%s) with date with format of %s.",
								parts[1].trim(), mProps.getConfigCollectionDateFormat());
						
						logger.fatal("Failed to parse collection-date ({}) with date with format of {}.",
								parts[1].trim(), mProps.getConfigCollectionDateFormat());

						// Let nature take its course.
					}

					continue;
				}
				
				if (parts[0].trim().equals("unknown-region")) {
					mUnknownRegionName = parts[1].trim();
					continue;
				}
				
				if (parts[0].trim().equals("nomap-region")) {
					mUnknownRegionName = parts[1].trim();
					continue;
				}
				
				if (parts[0].trim().equals("destination-root-folder")) {
					mDestinationRootFolder = new File(parts[1].trim());
					continue;
				}
				
				if (parts[0].trim().equals("source-attachments-folder")) {
					mSourceAttachmentsFolder = new File(parts[1].trim());
					continue;
				}
			}
		}

		isInitialized = true; // Be optimistic
		
		// Setup the message file parts and filter.
		String parts[] = mDestinationBaseFileName.split("\\.");
		switch (parts.length) {
		case 0:
			System.err.println("ERROR: Invalid configuration for destination file name (" + mDestinationBaseFileName + "). Must have atleast a non-empty value.");
			logger.fatal("Invalid configration destination file name ({}). Must be a non-empty value.", mDestinationBaseFileName);
			
			isInitialized = false;
		case 1:
			mDestinationFileFirstPart = parts[0];
			mDestinationFileSecondPart = null;
			break;
		default:
			mDestinationFileFirstPart = parts[0];
			StringBuilder secondPart = new StringBuilder();
			for (int i = 1; i < parts.length; i++) {
				secondPart.append(parts[i]);
				// Only add "." if not last.
				if (i < parts.length - 1)
					secondPart.append(".");
			}
			mDestinationFileSecondPart = secondPart.toString();
		}

		mMsgFileFilter = new MsgFileFilter(mDestinationBaseFileName);

		/*
		 * Perform input configuration validation.
		 */
		if (null == mCollectionDate) {
			System.err.println("ERROR: Invalid configration for Processor. Missing/Invalid collection-date extended argument.");
			logger.fatal("Invalid configration for Processor. Missing/Invalid collection-date extended argument.");
			
			isInitialized = false;
		}

		if ( ! ( mSourceAttachmentsFolder.exists() && mSourceAttachmentsFolder.isDirectory()) ) {
			System.err.println("ERROR: Invalid configuration for Processor. Extended argument source-attachments-folder does not reference a valid folder.");
			logger.fatal("Invalid configuration for Processor. Extended argument source-attachments-folder does not reference a valid folder.");

			isInitialized = false;
		}
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return isInitialized;
	}

	public class MsgFileFilter implements FilenameFilter {

		MsgFileFilter(String inBaseName) {
			StringBuilder tmpStr = new StringBuilder();
			String parts[] = inBaseName.split("\\.");
			
			tmpStr.append("^"); // Beginning Match only.
			if (parts.length == 1) {
				// Only have one part to the name, just put the number pattern match at the end.
				tmpStr.append(parts[0]);
				tmpStr.append("([0-9]+?)?");
			} else {
				// Have multiple parts, so put file number on first part and maintain the rest.
				for (int i = 0; i < parts.length - 1; i++) {
					tmpStr.append(parts[i]);
					if (0 == i) {
						tmpStr.append("([0-9]+?)?");
					}
					tmpStr.append("\\.");
				}
				// Matches optional .xml 1.xml 123.xml
				tmpStr.append(parts[parts.length - 1]);
			}
			tmpStr.append("$");  // End of string match.

			mRegExpr = new RegExprMatcher(tmpStr.toString());
		}
		
		private RegExprMatcher mRegExpr;
		
		@Override
		public boolean accept(File dir, String name) {
			return mRegExpr.isMatch(name);
		}
	}
	
	private void loadAccountRegionMap() throws IOException {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		File mappingFile = mProps.getConfigAccountMappingFile();
		
		BufferedReader br = new BufferedReader(new FileReader(mappingFile));
		
		String line;
		while ((line = br.readLine()) != null) {
			
			// Remove any possible ending line comments
			String rawParts[] = line.split("#");
			
			if (rawParts.length < 1) {
				continue;  // junk line
			}

			// Get the parts of the meat of the line and remove extra spaces and such.
			String parts[] = rawParts[0].trim().replaceAll("\\s+",  " ").split(" ");
			
			if (parts.length >= 2) {
				String regionCode = parts[0];
				// Process all account(s) on the line
				for (int i = 1; i < parts.length; i++) {
					mAccountRegionMap.put(parts[i], regionCode); // Latest always wins.
				}
			}
		}
		
		br.close();
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	void printDocument(Document doc, OutputStream out, boolean omitXMLDeclaration) throws IOException, TransformerException {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
	    TransformerFactory tf = TransformerFactory.newInstance();
	    Transformer transformer = tf.newTransformer();
	    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	    if ( omitXMLDeclaration )
	    	transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	    
	    if ( bFormatXML ) {
	    	transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
	    }
	    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

	    transformer.transform(new DOMSource(doc), 
	         new StreamResult(new OutputStreamWriter(out, "UTF-8")));
	    
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	@Override
	public OutputStream start() throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		// Destroy anything we might still have.
		close();
		
		mOutputStream = new ByteArrayOutputStream();
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return mOutputStream;
	}
	
	private String lookup_firm(Document inDoc, String inAcct) {
		String retval = null;
		
		try {
			NodeList userInfo = inDoc.getElementsByTagName("UserInfo");
			for (int i=0; i < userInfo.getLength(); i++) {
				if (Node.ELEMENT_NODE == userInfo.item(i).getNodeType()) {
					Element accountNode = null;
					Element firmNode = null;

					// Loop through all the children of the UserInfo structure
					//   looking for the AccountNumber and FirmNumber, if any.
					NodeList childNodes = userInfo.item(i).getChildNodes();
					for (int j=0; j < childNodes.getLength(); j++) {
						if (Node.ELEMENT_NODE == childNodes.item(j).getNodeType()) {
							if (childNodes.item(j).getNodeName().equals("AccountNumber")) {
								accountNode = (Element)childNodes.item(j);
								
								if (null != firmNode) break; // we have both of what we need.
							}
							if (childNodes.item(j).getNodeName().equals("FirmNumber")) {
								firmNode = (Element)childNodes.item(j);
								
								if (null != accountNode) break; // we have both of what we need.
							}
						}
					}

					// If we have an accountNode and is what we are looking for, BONUS!
					if (null != accountNode && accountNode.getTextContent().equals(inAcct)) {
						// If we have a firmNode, then return the value.
						if (null != firmNode) {
							retval = firmNode.getTextContent();
						}
						break;  // All Done!
					}
				}
			}
		} catch (Exception e) {
			logger.fatal("Failed to determine firm number from account", e);
		}
	
		return retval;
	}

	@Override
	public void process(String inIdentifier) throws Exception {
 		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		Document messageDoc = null;
		ByteArrayInputStream parserInputStream = null;
		try {
			parserInputStream = new ByteArrayInputStream(mOutputStream.toByteArray());
			messageDoc = mDocumentBuilder.parse(parserInputStream);
		} catch (SAXException e) {
			System.err.println("ERROR: Failed to parse XML in processor.");
			logger.fatal("Failed to parse XML in processor.", e);

			StaticUtils.TRACE_METHOD_EXIT(logger);
			throw e;
		} finally {
			if (null != parserInputStream) {
				try {
					parserInputStream.close();
				} catch (IOException e) {
					// best try.
					logger.fatal("Failed to close InputStream", e);
				}
			}
		}
	 
		//optional, but recommended
		//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
		messageDoc.getDocumentElement().normalize();

		logger.debug("Root element: {}", messageDoc.getDocumentElement().getNodeName());

		String participantsParentTag = mProps.getExtensionMetadataParentTag();
		
        // Build a list of unique participants with fields specified.
		HashMap<String, HashMap<String, String>> participantHash = new HashMap<String, HashMap<String, String>>();
		if (null != participantsParentTag && ! participantsParentTag.isEmpty()) {
			List<String> participantFields = mProps.getExtensionMetadataFields();
			String participantKey = mProps.getExtensionMetadataGroupByField();
			
			NodeList participants = messageDoc.getElementsByTagName(participantsParentTag);
			
			// Add all participant elements for the configured tag, collect the needed fields
			//  and add to participant hash.
			
			for (int i=0; i < participants.getLength(); i++) {
				if (Node.ELEMENT_NODE == participants.item(i).getNodeType()) {
					Element userElement = (Element)participants.item(i);

					HashMap<String, String> fieldValues = new HashMap<String, String>();
					
					// We have an element, so now collect all the fields we need.
					for (String oneFieldName : participantFields) {
						NodeList currentFields = userElement.getElementsByTagName(oneFieldName);
						
						for (int j=0; j < currentFields.getLength(); j++) {
							if (Node.ELEMENT_NODE == currentFields.item(j).getNodeType()) {
								fieldValues.put(oneFieldName, currentFields.item(j).getTextContent());
								break;  // Just take the first element node we find.
							}
						}
					}
					
					// Now put the fields into the participant hash table, but only if this set has
					//  the key specified.
					String key = fieldValues.get(participantKey);
					if (null != key) {
						participantHash.put(fieldValues.get(participantKey), fieldValues);
					}
				}
			}
			
			logger.debug("Participants - Unique: {}", participantHash.size() );
		}
		
        // Build a list of unique accounts
		HashSet<String> accountHash = new HashSet<String>();
		
		NodeList accountNumbers = messageDoc.getElementsByTagName("AccountNumber");
		// Add all account elements to account number hash.
		for (int i=0; i < accountNumbers.getLength(); i++) {
			if (Node.ELEMENT_NODE == accountNumbers.item(i).getNodeType()) {
				accountHash.add(((Element)accountNumbers.item(i)).getTextContent().trim());
			}
		}
		
		logger.debug("Accounts - Found: {}  Unique: {}", accountNumbers.getLength(), accountHash.size() );

		// Build a list of unique attachments
		HashMap<String, Node> attachmentMap = new HashMap<String, Node>();
		
		NodeList attachments = messageDoc.getElementsByTagName("Attachment");
		for (int i=0; i < attachments.getLength(); i++) {
			if (Node.ELEMENT_NODE == attachments.item(i).getNodeType()) {
				NodeList fileIDs = ((Element)attachments.item(i)).getElementsByTagName("FileID");
				for (int j=0; j < fileIDs.getLength(); j++) {
					attachmentMap.put(fileIDs.item(j).getTextContent().trim(), attachments.item(i));
				}
			}
		}
		
		logger.debug("Attachments - Found: {} Unique: {}", attachments.getLength(), attachmentMap.size() );
		
		if (logger.isDebugEnabled()) {
			Iterator<String> iter = attachmentMap.keySet().iterator();
			while (iter.hasNext()) {
				logger.debug("Attachment FileID: {}", iter.next());
			}
		}
		
		// If the XML didn't have any accounts, need to put the message in the failure one.
		if (accountHash.isEmpty()) {
			accountHash.add(mProps.getDestinationUnknownAccountValue());
			logger.warn("Current message did not have any account entries");
		}

		// First determine if we have at least one account with a valid mapping.
		boolean haveOneKnownAccount = false;
		Iterator<String> acctIter = accountHash.iterator();
		while (acctIter.hasNext()) {
			String oneAccount = acctIter.next();
			
			String region = mAccountRegionMap.get(oneAccount);
			
			if (null != region) {
				haveOneKnownAccount = true;
				break;  // Only need to find one.
			}
		}
			
		acctIter = accountHash.iterator();
		while (acctIter.hasNext()) {
			String oneAccount = acctIter.next();
			
			String region = mAccountRegionMap.get(oneAccount);
			if (null == region) {
				region = mUnknownRegionName;
				
				// Go get the firm for this account.
				String firmNumber = lookup_firm(messageDoc, oneAccount);
				
				if (null != firmNumber && firmNumber.equals(mProps.getFirmNumber())) {
					// Humm.. Not good. We found an account claiming to be for our firm, need to consider this unknown.
					logger.warn("Found account ({}) that belongs to firm ({}), but there is no mapping found", oneAccount, firmNumber);
					
					region = mNoMapRegionName;
				} else {
					if (haveOneKnownAccount) {
						continue; // we're good.  Toss this one.
					} else {
						logger.warn("Encountered account ({}) in a message that has no known account.", oneAccount);
					}
				}
			}
			
			logger.debug("Mapped account {} to region \"{}\"", oneAccount, region);
			
			
			// Build the folder structure for message.
			StringBuilder destRootFolder = new StringBuilder();
			
			destRootFolder.append(mDestinationRootFolder.getAbsolutePath());
			destRootFolder.append(File.separator);
			destRootFolder.append(region + mProps.getRegionFolderPostFix());
			destRootFolder.append(File.separator);
			destRootFolder.append(oneAccount);
			destRootFolder.append(File.separator);
			destRootFolder.append(mOutputDateFolderFormat.format(mCollectionDate));
			
			logger.debug("Destination root folder: {}", destRootFolder.toString());
			
			File destMessageFolder = new File(destRootFolder.toString() + File.separator + mProps.getDestinationMessageFolderName());
			destMessageFolder.mkdirs();
			
			// Now that we know the folder has been created, append/create to the message file.
			//   The file name may have either the inIdentifier passed in, or if requested for multiple files
			//   generate a file number.  Otherwise, don't append anything.
			File messageFile = new File(destMessageFolder.getAbsolutePath() + File.separator 
						+ mDestinationFileFirstPart 
						+ (null != inIdentifier ? "_" + inIdentifier : (bMultipleFiles ? mFileNumber++ : ""))
						+ (null != mDestinationFileSecondPart ? "." + mDestinationFileSecondPart : ""));

			// Create a new file if it doesn't already exist.
			messageFile.createNewFile();
			
			// Now append the contents to the file.
			FileOutputStream fos = new FileOutputStream(messageFile, true);
			
			printDocument(messageDoc, fos, false);
			
			fos.flush();
			fos.close();

			// See if we have metadata for the message file.
			if (0 < participantHash.size()) {
				
				// Now that we know the folder has been created, append/create to the message file.
				//   The file name may have either the inIdentifier passed in, or if requested for multiple files
				//   generate a file number.  Otherwise, don't append anything.
				File metadataFile = new File(messageFile.getParentFile(), messageFile.getName() + mProps.getExtensionMetadataFilePostfix());

				logger.debug("Creating metadata file for message: {}.", metadataFile.getAbsolutePath());

				Document messageMetadataDoc = mDocumentBuilder.newDocument();
				
				Element rootElement = messageMetadataDoc.createElement("Participants");
				messageMetadataDoc.appendChild(rootElement);
				
				rootElement.setAttribute("count", Integer.toString(participantHash.size()));

				for (HashMap<String, String> singleParticipant : participantHash.values()) {
					
					Element participantElement = messageMetadataDoc.createElement("Participant");
					rootElement.appendChild(participantElement);

					for (Entry<String, String> oneValue : singleParticipant.entrySet()) {
						Element fieldElement = messageMetadataDoc.createElement(oneValue.getKey());
						fieldElement.appendChild(messageMetadataDoc.createTextNode(oneValue.getValue()));
						
						participantElement.appendChild(fieldElement);
					}
				}
				
				// Create a new file if it doesn't already exist.
				metadataFile.createNewFile();
				
				// Now append the contents to the file.
				FileOutputStream messageMetadataFOS = new FileOutputStream(metadataFile, true);
				
				printDocument(messageMetadataDoc, messageMetadataFOS, false);
				
				messageMetadataFOS.flush();
				messageMetadataFOS.close();
			}

			// Now move the attachments, if we have any
			if (0 < attachmentMap.size()) {
				File destAttachmentFolder = new File(destRootFolder + File.separator + mProps.getDestinationAttachmentFolderName());
				
				Iterator<String> attchIter = attachmentMap.keySet().iterator();
				while (attchIter.hasNext()) {
					String oneAttachment = attchIter.next();

					File srcAttachment = new File(mSourceAttachmentsFolder, oneAttachment);
					if ( ! srcAttachment.exists() ) {
						logger.fatal("Attachment ({}) referred to in message does not exist at path: {}",
								oneAttachment, srcAttachment.getAbsolutePath());
						continue;
					} 
					if (! destAttachmentFolder.exists()) {
						destAttachmentFolder.mkdirs();
					}
					File dstAttachment = new File(destAttachmentFolder.getAbsolutePath() + File.separator + oneAttachment);
					
					try {
						logger.debug("Copying Attachment: {} -> {}", srcAttachment.getAbsolutePath(), dstAttachment.getAbsolutePath());
						
						Files.copy(srcAttachment.toPath(), dstAttachment.toPath(), (bOverwriteOff ? null : StandardCopyOption.REPLACE_EXISTING));
					} catch (FileAlreadyExistsException e) {
						System.err.printf("WARN: Existing file not overwritten: %s\n", dstAttachment.getAbsolutePath());
						logger.warn("Existing file not overwritten: {}", dstAttachment.getAbsolutePath());
					}

					// Going to fill these in either from already existing document or going to be created below.
					Document attachMetadataDoc = null;
					Node rootElement = null;

					// Now it is time to create the metadata to be ingested with the content.
					File attachmentMetadataFile = new File(dstAttachment.getAbsolutePath() + mProps.getDestinationAttachmentMetadataPostFix());

					// Use createNewFile to create new file and switch logic if already existing.
					if ( ! attachmentMetadataFile.createNewFile() ) {

						logger.debug("Found already existing metadata file for attachment: {}", dstAttachment.getAbsolutePath());
						
						// File already exists, so load the current metadata into a DOM document and
						//   determine if we already have this current message in the metadata.
						try {
							Node currentMsgIDNode = messageDoc.getElementsByTagName("MsgID").item(0);

							// Parse current metadata file.
							attachMetadataDoc = mDocumentBuilder.parse(attachmentMetadataFile);

							// Remember the root element so we will have some place to put the new message, if any.
							rootElement = attachMetadataDoc.getChildNodes().item(0);

							// Loop through all messages looking for the message ID of the current item.
							NodeList messageList = attachMetadataDoc.getElementsByTagName("Message");
							Node messageWithID = null;
							
							for (int i = 0; i < messageList.getLength(); i++) {
								NodeList msgChildren = messageList.item(i).getChildNodes();
								for (int j = 0; j < msgChildren.getLength(); j++) {
									if (msgChildren.item(j).getNodeName().equals("MsgID")) {
										if ( msgChildren.item(j).isEqualNode(currentMsgIDNode) ) {
											messageWithID = msgChildren.item(j);
											break;
										}
									}
								}
								// Have a message, so we are done with loop
								if (null != messageWithID)
									break;
							}
							
							// If we found this Message ID, nothing more to do.
							if ( null != messageWithID ) {
								logger.debug("Found Message ID ({}) already in attachment metadata file. Skipping", messageWithID.getNodeValue());
								continue;  // Don't add anything.
							}
							
							// We are going to fall through and add a new Message
							logger.debug("Adding message ID ({}) to metadata file for attachment: {}", currentMsgIDNode.getNodeValue(), dstAttachment.getAbsolutePath());
							
						} catch (SAXException | IOException e ) {
							logger.fatal("Unexpecting failure reading existing attachment metadata file: " + attachmentMetadataFile.getAbsolutePath(), e);
							continue;
						}
					} else {
						logger.debug("Creating metadata file for attachment: {}.", dstAttachment.getAbsolutePath());
						
						// Don't have any attachment metadata, so build a fresh one.
						attachMetadataDoc = mDocumentBuilder.newDocument();
						
						rootElement = attachMetadataDoc.createElement(mProps.getMetadataAttachmentEnclosingTag());
						attachMetadataDoc.appendChild(rootElement);
					}

					/*
					 * If we got here, we are going to be adding a Message structure and add it to the "rootElement".
					 */
					
					Element msgElement = attachMetadataDoc.createElement("Message");
					rootElement.appendChild(msgElement);

					// Add the HCPReference Tag
					Element hcpRef = attachMetadataDoc.createElement("HCPReference");
					hcpRef.appendChild(attachMetadataDoc.createTextNode("messages/" + messageFile.getName()));
					msgElement.appendChild(hcpRef);

					NodeList tmpNodeList;
					Node tmpMsgNode;
					
					// Add the MsgID, should only be one.
					tmpNodeList = messageDoc.getElementsByTagName("MsgID");
                    tmpMsgNode = tmpNodeList.item(0);
                    msgElement.appendChild(attachMetadataDoc.importNode(tmpMsgNode, true));
					
					// Add the MsgTime
					tmpNodeList = messageDoc.getElementsByTagName("MsgTime");
                    tmpMsgNode = tmpNodeList.item(0);
                    msgElement.appendChild(attachMetadataDoc.importNode(tmpMsgNode, true));
					
					// Add the MsgTimeUTC
					tmpNodeList = messageDoc.getElementsByTagName("MsgTimeUTC");
                    tmpMsgNode = tmpNodeList.item(0);
                    msgElement.appendChild(attachMetadataDoc.importNode(tmpMsgNode, true));

					// Add the Sender
					tmpNodeList = messageDoc.getElementsByTagName("Sender");
                    tmpMsgNode = tmpNodeList.item(0);
                    msgElement.appendChild(attachMetadataDoc.importNode(tmpMsgNode, true));

					// Add the Attachment metadata.
					msgElement.appendChild(attachMetadataDoc.importNode(attachmentMap.get(oneAttachment), true));
					
					// Have a new Metadata document, so overwrite any existing file.
					FileOutputStream metadataFOS = new FileOutputStream(attachmentMetadataFile, false);

					printDocument(attachMetadataDoc, metadataFOS, true);
					
					metadataFOS.flush();
					metadataFOS.close();
				}
			}
		}

		close();
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}

	@Override
	public void close() {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		if ( null != mOutputStream ) {
			try {
				mOutputStream.close();
			} catch (IOException e) {
				// Best Try.
			}
			mOutputStream = null;
		}
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}

}
