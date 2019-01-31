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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.FileSystemItem;
import com.hds.hcp.tools.comet.generator.BaseGeneratorProperties;
import com.hds.hcp.tools.comet.generator.BaseMetadataGenerator;
import com.hds.hcp.tools.comet.generator.CustomMetadataContainer;
import com.hds.hcp.tools.comet.generator.ObjectContainer;
import com.hds.hcp.tools.comet.generator.SystemMetadataContainer;
import com.hds.hcp.tools.comet.utils.ByteArrayInOutStream;
import com.hds.hcp.tools.comet.utils.StaticUtils;
import com.hds.hcp.tools.comet.utils.URIWrapper;

public class AlgomiTransformGenerator extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	AlgomiTransformGeneratorProperties mProps = new AlgomiTransformGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();

	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mPreTransformDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");
	protected SimpleDateFormat mOutputChildFolderDateFormat = new SimpleDateFormat("MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
    Transformer metadataTransformer;
    
	/*
	 * Construct a private properties class to construct module specific information.
	 */
	private class AlgomiTransformGeneratorProperties extends BaseGeneratorProperties {
		public AlgomiTransformGeneratorProperties() {
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
		
		public String getMetadataEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.enclosingTag", "TopTag"));
		}

		public TimeZone getOutputDateTimeZone() {
			return TimeZone.getTimeZone(mProps.getProperty("destination.outputDateTimeZone", "GMT"));
		}
		
		public String getTransformFileName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("transform.FileName"));
		}
	}
	
	private void convertDocumentToStream(Document doc, StreamResult inStream) throws IOException, TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,  "no");
		transformer.setOutputProperty(OutputKeys.METHOD,  "xml");
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty(OutputKeys.ENCODING,  "UTF-8");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount",  "4");
		
		transformer.transform(new DOMSource(doc),  inStream);
	}
	
	private void formatDateTime(Document inDocument, String inElement) {
		
		 //Format startDateTime/endDateTime in the outputDocument
		 
		NodeList nodeList = inDocument.getElementsByTagName(inElement);
        Node nodeDateTime;
        String formatDateString = null;
        
        //If we end up with more than one element, iterate over the elements. 
		for (int i=0; i < nodeList.getLength(); i++) {
			
			nodeDateTime = nodeList.item(i);
			
			Date dateTime; //holds the final startDateTime after formatting
			try {
				String nodeDateTimeString = nodeDateTime.getTextContent();
				dateTime = mPreTransformDateFormat.parse(nodeDateTimeString);

				//Replace the Node Content with the formatted dateTime.
				nodeDateTime.setTextContent(mOutputMetadataDateFormat.format(dateTime).toString());
			} catch (Exception e) {
				logger.fatal("Unable to format Date/Time ({}) from Element ({})",formatDateString, inElement);
			}
		}
	}
	
	private InputStream buildMetadata(File inSrcFile, String inRegionCode, Date inCollectionDate)
			throws ParserConfigurationException, SAXException, IOException, TransformerFactoryConfigurationError, TransformerException {

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        
        /*
         * Perform the transform of the input XML
         */
				
        // Setup Source document for transform with XML.
        Document inputDocument = builder.parse(inSrcFile);
        DOMSource source = new DOMSource(inputDocument);
				
        // Setup Result transformed document.
        Document transformedDocument = builder.newDocument();
        DOMResult result = new DOMResult(transformedDocument);

        // Perform the transform.
        metadataTransformer.transform(source, result);
				
        /*
         * Now construct the final output document from the pieces.
         */
        Document outputDocument = builder.newDocument();
				
        // Build the CollectionInfo Node 
        Element collectionInfoNode = outputDocument.createElement("CollectionInfo");
				
        Element region = outputDocument.createElement("Region");
        region.appendChild(outputDocument.createTextNode(inRegionCode));
        collectionInfoNode.appendChild(region);
				
        Element dateTime = outputDocument.createElement("DateTime");
        dateTime.appendChild(outputDocument.createTextNode(mOutputMetadataDateFormat.format(inCollectionDate).toString()));
        collectionInfoNode.appendChild(dateTime);

        // Create top-level "TransformData" Element.
        Element transformDataElement = outputDocument.createElement(mProps.getMetadataEnclosingTag());

        // Add the CollectionInfo Element
        transformDataElement.appendChild(collectionInfoNode);
				
        // Add the transformed Element.
        transformDataElement.appendChild(outputDocument.importNode(transformedDocument.getFirstChild(), true));

        // Put the TransformData top level element into the document.
        outputDocument.appendChild(transformDataElement);
				
        //Reformat start and end Datetimes.
        formatDateTime(outputDocument, "startDateTime");
        formatDateTime(outputDocument, "endDateTime");
				
				
        // Only output whole document in trace mode.
        if (logger.isTraceEnabled()) {
        	StreamResult transformOutput;
            try {
            	transformOutput=new StreamResult(new StringWriter());
                convertDocumentToStream(outputDocument, transformOutput);
                //Output Transformed XML when set to trace mode.
                logger.trace("Transformed Output: {}", transformOutput.getWriter().toString());
                
            } catch (IOException | TransformerException e) {
                logger.error("Failed to perform tracing of transformed XML metadata.");
                
            }
        }
				
        ByteArrayInOutStream outStream = new ByteArrayInOutStream();
        convertDocumentToStream(outputDocument, new StreamResult(outStream));

        return outStream.getInputStream();
    }
	
	public void initialize() throws Exception { 
		// Verify that the mapper is functional.
		mRegionMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputChildFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		
	    File stylesheet = new File(mProps.getTransformFileName());
		
	    // Check if the stylesheet exists
		if ( ! stylesheet.exists() ) {
			logger.error("Transform Style sheet file does not exist: {}", stylesheet.getAbsolutePath());
			throw new IOException("Missing file: " + stylesheet.getName());
		}

        // Setup the tranformer with the stylesheet.
        StreamSource stylesource = new StreamSource(stylesheet);
        metadataTransformer = TransformerFactory.newInstance().newTransformer(stylesource);

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
		File folder = thisItem.getFile().getParentFile().getParentFile().getParentFile(); // Start off with the great great grand parent folder.
		
			String folderName = folder.getName();
			regionCode = folderName.substring(0, Math.max(folderName.lastIndexOf(mProps.getRegionFolderPostFix()), 0));
			
			try {
				regionConfig = mRegionMap.getMatch(regionCode);
			} catch (InvalidConfigurationException e) {
				logger.fatal("Unable to find region code in RegionMapper for item: " + thisItem.getName(), e);
				
				return null;
			}

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
		
		// First transform the collection folder.
		File parentFolder = inSrcFile.getParentFile();
		File grandParentFolder = inSrcFile.getParentFile().getParentFile();
		
		String combinedDirName = grandParentFolder.getName()+"-"+parentFolder.getName();
		
		String transformedCollectionFolders = null;
		try {
			collectionDate = mInputDateFormat.parse(combinedDirName);
			transformedCollectionFolders = mOutputYearFolderDateFormat.format(collectionDate)
					+ File.separator 
					+ mOutputChildFolderDateFormat.format(collectionDate);
		} catch (ParseException e) {
				logger.fatal("Unable to formulate Date/Time from folder ({})", combinedDirName);
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
		  }
		
		
		// First formulate the destination object path based on source file path, base folder path,
		//   and the settings in the properties file.
		//
		StringBuilder destFilePath = new StringBuilder();
		
			destFilePath.append(inSrcFile.getParentFile().getParentFile().getParentFile().getAbsolutePath());
	
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
		
		      InputStream metadataStream = null;
			  CustomMetadataContainer customMeta = retObject.getCustomMetadata();
			
			  	try {
			  		metadataStream = buildMetadata(inSrcFile, regionConfig.getIdentifier(), collectionDate);
			  		
			  		customMeta.put(metadataStream);
			  		
			  	} catch (IOException | ParserConfigurationException | SAXException | TransformerFactoryConfigurationError | TransformerException e) {
			  		logger.fatal("Unexpected failure processing core include files", e);
			  		StaticUtils.TRACE_METHOD_EXIT(logger, "Aborting metadata generation");
					
			  		return null;  // Won't be writing file.
			  	}

		// Put the base object into the linked list to be returned.
		retval.add(retObject);
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return retval;
	}
}
