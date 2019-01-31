package nl.rabobank.comet.generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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

public class VerintRawGenerator extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
	
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");

	protected SimpleDateFormat mOutputMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	/*
	 * Construct a private properties class to construct module specific information.
	 */
	private class VerintGeneratorProperties extends BaseGeneratorProperties {
		public VerintGeneratorProperties() {
			super();
		}

		public String getSourcePathIdentifier() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("source.pathIdentifier"));
		}

		public String getRegionFolderPostFix() {
			return mProps.getProperty("source.regionFolderPostFix",  "_data");
		}
		
		public TimeZone getOutputDateTimeZone() {
			return TimeZone.getTimeZone(mProps.getProperty("destination.outputDateTimeZone", "GMT"));
		}
		
		public String getMetadataEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.enclosingTag", "TopTag"));
		}

	}
	
	VerintGeneratorProperties mProps = new VerintGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();

	@Override
	public void initialize() throws Exception { 
		// Verify that the mapper is functional.
		mRegionMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputMetadataDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		
		return; 
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
	
	private InputStream buildMetadata(String inJobName, String inJobID, String inJobDateTime, String inJobRegion)
			      throws ParserConfigurationException, IOException, TransformerException
	{
		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

		/*
		 * Now construct the final output document from the pieces.
		 */
		Document outputDocument = builder.newDocument();
		
		// Build the CollectionInfo Node 
		Element collectionInfoNode = outputDocument.createElement("CollectionInfo");
		
		Element jobID = outputDocument.createElement("ID");
		jobID.appendChild(outputDocument.createTextNode(inJobID));
		collectionInfoNode.appendChild(jobID);
		
		Element region = outputDocument.createElement("Region");
		region.appendChild(outputDocument.createTextNode(inJobRegion));
		collectionInfoNode.appendChild(region);
		
		Element name = outputDocument.createElement("Name");
		name.appendChild(outputDocument.createTextNode(inJobName));
		collectionInfoNode.appendChild(name);
		
		Element dateTime = outputDocument.createElement("DateTime");
		dateTime.appendChild(outputDocument.createTextNode(inJobDateTime));
		collectionInfoNode.appendChild(dateTime);

		// Create top-level "TransformData" Element.
		Element transformDataElement = outputDocument.createElement(mProps.getMetadataEnclosingTag());

		// Add the JobName Element
		transformDataElement.appendChild(collectionInfoNode);
		
		// Put the TransformData top level element into the document.
		outputDocument.appendChild(transformDataElement);

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
		
	@Override
	public LinkedList<ObjectContainer> getMetadataList(BaseWorkItem inItem) {
		StaticUtils.TRACE_METHOD_ENTER(logger);

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
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		// The item is ours, so let's see about setting up the credentials and system metadata based on the mapping.
		
		File inSrcFile = thisItem.getFile();
		File inBaseFolder = (File)thisItem.getBaseSpecification();
		
		/*
		 *  Now determine if a top level special file: CD_Mappings.xml.
		 */
		boolean isCDMappingFile = inSrcFile.getName().equals("CD_Mappings.xml");

		String jobName = null;
		String jobID = null;
		String jobMetadataDateTime = null;
		
		/* 
		 * Setup collection folder and figure out the country code from the appropriate 
		 *    folder name.  This is needed to look up HCP destination information.
		 */
		String regionCode = null;
		File collectionFolder = null;
		try {
			String folderName = null;
			
			if ( isCDMappingFile ) {
				collectionFolder = inSrcFile.getParentFile();
			} else {
				collectionFolder = inSrcFile.getParentFile().getParentFile();
			}
			folderName = collectionFolder.getParentFile().getName();
			
			regionCode = folderName.substring(0, Math.max(folderName.lastIndexOf(mProps.getRegionFolderPostFix()), 0));
			
		} catch (NullPointerException e) {
			logger.fatal("Failed extract region code from file path ({})", collectionFolder.getAbsolutePath());
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}

		// Setup the jobName, jobID, and jobMetadataDateTime for the custom metadata.
		try {
			String namePartsLevel1[] = collectionFolder.getName().split(";");
			String namePartsLevel2[] = namePartsLevel1[1].split("_");
			jobName = namePartsLevel1[0];
			jobID = namePartsLevel2[0];

			jobMetadataDateTime = mOutputMetadataDateFormat.format(mInputDateFormat.parse(namePartsLevel2[1] + "_" + namePartsLevel2[2]));
		} catch (NullPointerException | ArrayIndexOutOfBoundsException | ParseException e) {
			logger.fatal("Unable to formulate jobName, jobID, and Date/Time from folder (" + collectionFolder.getName() + ")", e);
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		// Get the region configuration mapping.
		RegionSpec regionConfig = null;
		try {
			regionConfig = mRegionMap.getMatch(regionCode);
			
			if (null == regionConfig) {
				logger.fatal("Unable to find region code ({}) in RegionMapper for item: {}", regionCode, thisItem.getName());
				
				return null;
			}
		} catch (InvalidConfigurationException e) {
			logger.fatal("Unable to find region code in RegionMapper for item: " + thisItem.getName(), e);
			
			return null;
		}
		
		logger.debug("Established Region Code ({})", regionCode);

		// TimeZone of input datetime is taken from region Config.
		mInputDateFormat.setTimeZone(regionConfig.getTimeZone());
		
		String collectionFolderDateTime = null;
		try {
			String dateTimePortion = collectionFolder.getName().split(";")[1];
			collectionFolderDateTime = dateTimePortion.substring(dateTimePortion.indexOf('_')+1);
		} catch (Exception e) {
			logger.error("Failed to parse date/time out of collection folder: (" + collectionFolder.getName() + ")", e);
			return null;
		}
		
		String transformedCollectionFolders = null;
		try {
			transformedCollectionFolders = mOutputYearFolderDateFormat.format(mInputDateFormat.parse(collectionFolderDateTime))
					+ File.separator + collectionFolder.getName();
		} catch (ParseException e) {
			logger.fatal("Unable to formulate Date/Time from folder ({})", collectionFolder.getName());
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		// Build out the destination for the one item.
		//
		// First formulate the destination object path based on source file path, base folder path,
		//   and the settings in the properties file.
		//
		StringBuilder destFilePath = new StringBuilder();
		
		if ( isCDMappingFile ) {
			destFilePath.append(inSrcFile.getParentFile().getParentFile().getAbsolutePath() + File.separator);
		} else {
			destFilePath.append(inSrcFile.getParentFile().getParentFile().getParentFile().getAbsolutePath() + File.separator);
		}

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
		
		// Add in transformed collection folders.
		destFilePath.append(transformedCollectionFolders + File.separator);

		// Add back in the rest of the path(s).
		if ( ! isCDMappingFile ) {
			destFilePath.append(inSrcFile.getParentFile().getName() + File.separator);
		}
		
		// Add back in file name
		destFilePath.append(inSrcFile.getName());
		
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

		/*
		 * Build the custom metadata
		 */
		InputStream metadataStream = null;
		try {
			CustomMetadataContainer customMeta = retObject.getCustomMetadata();
			
			metadataStream = buildMetadata(jobName, jobID, jobMetadataDateTime, regionCode);
			customMeta.put(metadataStream);
		} catch (IOException | ParserConfigurationException | TransformerFactoryConfigurationError | TransformerException e) {
			logger.fatal("Unexpected Exception trying to read index file stream into custom metadata container.", e);
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		} finally {
			try {
				if (null != metadataStream)
					metadataStream.close();
			} catch (IOException e) {
				// Best attempt..
			}
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
