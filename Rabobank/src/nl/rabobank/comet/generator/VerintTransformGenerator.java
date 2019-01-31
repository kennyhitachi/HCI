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
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import nl.rabobank.comet.VerintItem;
import nl.rabobank.comet.util.FilePrefixProperties;
import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.generator.BaseGeneratorProperties;
import com.hds.hcp.tools.comet.generator.BaseMetadataGenerator;
import com.hds.hcp.tools.comet.generator.CustomMetadataContainer;
import com.hds.hcp.tools.comet.generator.ObjectContainer;
import com.hds.hcp.tools.comet.generator.SystemMetadataContainer;
import com.hds.hcp.tools.comet.utils.ByteArrayInOutStream;
import com.hds.hcp.tools.comet.utils.StaticUtils;
import com.hds.hcp.tools.comet.utils.URIWrapper;

public class VerintTransformGenerator extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");
	protected SimpleDateFormat mOutputChildFolderDateFormat = new SimpleDateFormat("MM-dd'T'HHmmssZ");
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
		
		public String getTransformRootPath() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("transform.rootPath"));
		}
		public String getTransformDefaultFileName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("transform.defaultFileName"));
		}
		
		public String getMetadataEnclosingTag() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.enclosingTag", "TopTag"));
		}

	}
	
	VerintGeneratorProperties mProps = new VerintGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();
	FilePrefixProperties mFilePrefixMap = new FilePrefixProperties();

	@Override
	public void initialize() throws Exception { 
		// Verify that the mappers are functional.
		mRegionMap.getMatch("default");
		mFilePrefixMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputChildFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
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
	
	private InputStream buildMetadata(File inMetadataFile, String inJobName, String inJobID, String inJobDateTime, String inJobRegion)
		throws ParserConfigurationException, SAXException, IOException, TransformerFactoryConfigurationError, TransformerException {
		    File stylesheet = new File(mProps.getTransformRootPath() + File.separator + inJobName.replaceAll(" ", "") + ".xslt");
		    
			// If generated name does not exist, try the default one.
			if ( ! stylesheet.exists() ) {
				logger.info("Stylesheet for specific job name ({}), does not exist.  Trying default stylesheet.", inJobName);
			    stylesheet = new File(mProps.getTransformRootPath() + File.separator + mProps.getTransformDefaultFileName());

			    // Make sure it exists
				if ( ! stylesheet.exists() ) {
					logger.error("Transform Style sheet file does not exist: {}", stylesheet.getAbsolutePath());
					throw new IOException("Missing file: " + stylesheet.getName());
				}
			}

			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

			/*
			 * Perform the transform of the input CAudioFile XML
			 */
			
			// Setup Source document for transform with CAudioFile XML.
			Document inputDocument = builder.parse(inMetadataFile);
			DOMSource source = new DOMSource(inputDocument);

			// Setup Result transformed document.
			Document transformedDocument = builder.newDocument();
			DOMResult result = new DOMResult(transformedDocument);

			// Setup the tranformer with the stylesheet.
			StreamSource stylesource = new StreamSource(stylesheet);
			Transformer transformer = TransformerFactory.newInstance().newTransformer(stylesource);

			// Perform the transform.
			transformer.transform(source, result);
			
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

			// Copy over the xmlns attributes from CAudioFile to the TransformData Element.
			String xsiValue=transformedDocument.getFirstChild().getAttributes().getNamedItem("xmlns:xsi").getNodeValue();
			String xsdValue=transformedDocument.getFirstChild().getAttributes().getNamedItem("xmlns:xsd").getNodeValue();

			if (null != xsiValue) {
				transformDataElement.setAttribute("xmlns:xsi", xsiValue);
				transformedDocument.getFirstChild().getAttributes().removeNamedItem("xmlns:xsi");
			}
			if (null != xsdValue) {
				transformDataElement.setAttribute("xmlns:xsd", xsdValue);
				transformedDocument.getFirstChild().getAttributes().removeNamedItem("xmlns:xsd");
			}

			// Add the JobName Element
			transformDataElement.appendChild(collectionInfoNode);
			
			// Add the transformed CAudioFile Element.
			transformDataElement.appendChild(outputDocument.importNode(transformedDocument.getFirstChild(), true));

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

		// To work properly here, we must have an HCPItem.  Make sure we do.
		if ( ! (inItem instanceof VerintItem) ) {
			logger.fatal("Unexpected object type passed in to getMetadataList. Expected " 
		                 + VerintItem.class.getName() + " Received " + inItem.getClass().getName());

			// TODO:  Need something here...  Probably need to think about returning an exception???
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		VerintItem thisItem = (VerintItem)inItem;
		
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
		
		File inSrcFile = thisItem.getFile();

		/*
		 *  Now determine if a top level special file: CD_Mappings.xml.
		 */
		boolean isCDMappingFile = inSrcFile.getName().equals("CD_Mappings.xml");

		if ( ! isCDMappingFile ) {
			// Make sure it is a WAV file.
			try {
				if ( ! inSrcFile.getName().split("\\.")[1].equalsIgnoreCase("WAV") ) {
					logger.debug("Item not for generator. File is not a WAV file: {}", inSrcFile.getAbsolutePath() );
					StaticUtils.TRACE_METHOD_EXIT(logger);
					return null;
				}
			} catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
				// Should never get here unless the scanner is misconfigured to pass in incorrect files.
				logger.debug("Item not for generator. File is not a WAV file or expected top level file: {}", inSrcFile.getAbsolutePath() );
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
			}
		}
		
		// For WAV files make sure we have a metadata xml file.
		File metadataFile = null;
		if ( ! isCDMappingFile ) {
			try {
				metadataFile = new File(inSrcFile.getParentFile().getParentFile().getAbsolutePath() 
						+ File.separator + "IDX" + File.separator + inSrcFile.getName().split("\\.")[0] + ".xml");
				
				if ( ! metadataFile.exists() ) {
					logger.fatal("Metadata file does not exist ({})", metadataFile.getAbsolutePath());
					StaticUtils.TRACE_METHOD_EXIT(logger);
					return null;
				}
			} catch (NullPointerException e) {
				logger.fatal("Failed to locate metadata file. Unable to locate IDX folder.");
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
				
			} catch (Exception e) {
				logger.fatal("Unexpected Exception", e);
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
			}
		}
		

		/* 
		 * Figure out the country code from the appropriate folder name.  This is needed to look up 
		 *    HCP destination information.
		 */
		String regionCode = null;
		File folder = null;
		try {
			String folderName = null;
			
			if ( isCDMappingFile ) {
				folder = thisItem.getFile().getParentFile().getParentFile();
				folderName = folder.getName();
			} else {
				folder = thisItem.getFile().getParentFile().getParentFile().getParentFile();
				folderName = folder.getName();
			}
			
			regionCode = folderName.substring(0, Math.max(folderName.lastIndexOf(mProps.getRegionFolderPostFix()), 0));
			
		} catch (NullPointerException e) {
			logger.fatal("Failed extract region code from file path ({})", folder.getAbsolutePath());
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		RegionSpec regionConfig = null;
		try {
			regionConfig = mRegionMap.getMatch(regionCode);
			
			if (null == regionConfig) {
				logger.fatal("Unable to find region code ({}) in RegionMapper for item: {}", regionCode, thisItem.getName());
				
				return null;
			}
		} catch (InvalidConfigurationException e) {
			logger.fatal("Unable to find region code in RegionMapper for item: " + thisItem.getName(), e);
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		logger.debug("Established Region Code ({})", regionCode);

		// TimeZone of input datetime is taken from region Config.
		mInputDateFormat.setTimeZone(regionConfig.getTimeZone());
		
		File inBaseFolder = (File)thisItem.getBaseSpecification();
		
		/*
		 *  Build out the destination path for this item.
		 */
		
		// Reconstruct the folders so that the job folder and the WAV folder is transformed into
		//  the jobname with a sub-folder of the date/time of the job folder.
		String jobName = null;
		String jobID = null;
		String jobFoldersDateTime = null;
		String jobMetadataDateTime = null;
		
		File jobFolder = null;
		if ( isCDMappingFile ) {
			jobFolder = inSrcFile.getParentFile();
		} else {
			jobFolder = inSrcFile.getParentFile().getParentFile();
		}
		try {
			String namePartsLevel1[] = jobFolder.getName().split(";");
			String namePartsLevel2[] = namePartsLevel1[1].split("_");
			jobName = namePartsLevel1[0];
			jobID = namePartsLevel2[0];

			jobFoldersDateTime = mOutputYearFolderDateFormat.format(mInputDateFormat.parse(namePartsLevel2[1] + "_" + namePartsLevel2[2]))
					+ File.separator 
					+ mOutputChildFolderDateFormat.format(mInputDateFormat.parse(namePartsLevel2[1] + "_" + namePartsLevel2[2]));
			
			jobMetadataDateTime = mOutputMetadataDateFormat.format(mInputDateFormat.parse(namePartsLevel2[1] + "_" + namePartsLevel2[2]));

		} catch (NullPointerException | ArrayIndexOutOfBoundsException | ParseException e) {
			logger.fatal("Unable to formulate jobName, jobID, and Date/Time from folder (" + jobFolder.getName() + ")", e);
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		StringBuilder generatedFilePath = new StringBuilder();
		
		if (isCDMappingFile) {
			generatedFilePath.append(inSrcFile.getParentFile().getParent());
		} else {
			generatedFilePath.append(inSrcFile.getParentFile().getParentFile().getParent());
		}
		generatedFilePath.append(File.separator);
		generatedFilePath.append(jobName);
		generatedFilePath.append(File.separator);
		generatedFilePath.append(jobFoldersDateTime);
		
		generatedFilePath.append(File.separator);
		
		String srcFileName = inSrcFile.getName();
		if ( ! isCDMappingFile ) {
			
			// It isn't a CD Mapping file so add on a file prefix based on the jobName
			try {
				String filePrefix = mFilePrefixMap.getMatch(jobName);
				
				srcFileName = filePrefix + regionCode + "_" + srcFileName;
			} catch (InvalidConfigurationException e) {
				logger.fatal("Failed to obtain mapping for jobName: \"" + jobName + "\"", e);
				
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
			}
		}
		
		generatedFilePath.append(srcFileName);

		// Now manipulated generated path based on source file path, base folder path,
		//   and the settings in the properties file.
		//
		StringBuffer destFilePath = new StringBuffer(generatedFilePath);
		
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
		 * Set the core system metadata for the object.
		 */
		LinkedList<ObjectContainer> retval = new LinkedList<ObjectContainer>();
		
		SystemMetadataContainer sysMeta = retObject.getSystemMetadata();

		// Yes. Use the credentials from properties file.
		sysMeta.setCredentials(regionConfig.getEncodedUserName(), regionConfig.getEncodedPassword());
		
		// Put the base object into the linked list to be returned.
		retval.add(retObject);

		if ( ! isCDMappingFile ) {

			// It is a WAV file.
			// Time to start building the custom metadata.
			InputStream metadataStream = null;
			try {
				CustomMetadataContainer customMeta = retObject.getCustomMetadata();
				
				metadataStream = buildMetadata(metadataFile, jobName, jobID, jobMetadataDateTime, regionCode);
				customMeta.put(metadataStream);
			} catch (IOException | ParserConfigurationException | SAXException | TransformerFactoryConfigurationError | TransformerException e) {
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
		}
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
		return retval;
	}

}
