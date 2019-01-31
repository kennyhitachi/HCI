package nl.rabobank.comet.generator;

import java.io.File;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.TimeZone;

import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.FileSystemItem;
import com.hds.hcp.tools.comet.generator.BaseGeneratorProperties;
import com.hds.hcp.tools.comet.generator.BaseMetadataGenerator;
import com.hds.hcp.tools.comet.generator.CustomMetadataContainer;
import com.hds.hcp.tools.comet.generator.ObjectContainer;
import com.hds.hcp.tools.comet.generator.SystemMetadataContainer;
import com.hds.hcp.tools.comet.utils.StaticUtils;
import com.hds.hcp.tools.comet.utils.URIWrapper;

public class BloombergRawGenerator  extends BaseMetadataGenerator {

	private static Logger logger = LogManager.getLogger();
	
	protected SimpleDateFormat mInputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputYearFolderDateFormat = new SimpleDateFormat("yyyy");
	protected SimpleDateFormat mOutputChildFolderDateFormat = new SimpleDateFormat("MM-dd'T'HHmmssZ");
	protected SimpleDateFormat mOutputMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	/*
	 * Construct a private properties class to construct module specific information.
	 */
	private class BloombergRawGeneratorProperties extends BaseGeneratorProperties {
		public BloombergRawGeneratorProperties() {
			super();
		}

		public String getSourcePathIdentifier() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("source.pathIdentifier"));
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
	}
	
	BloombergRawGeneratorProperties mProps = new BloombergRawGeneratorProperties();
	RegionMapperProperties mRegionMap = new RegionMapperProperties();

	public void initialize() throws Exception { 
		// Verify that the mapper is functional.
		mRegionMap.getMatch("default");
		
		// Always use UTC time zone for writing dates.
		mOutputYearFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		mOutputChildFolderDateFormat.setTimeZone(mProps.getOutputDateTimeZone());
		
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
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		// The item is ours, so let's see about setting up the credentials and system metadata based on the mapping.
		
		// TODO:
		// TODO: Need better validation that the path has this many levels and does not cause an exception
		// TODO:
		
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
				
				StaticUtils.TRACE_METHOD_EXIT(logger);
				return null;
			}
			
			if (null != regionConfig) break;  // All done;

			folder = folder.getParentFile(); // Try its parent.
			tries--;
		} while (tries > 0);

		if (null == regionConfig) {
			logger.fatal("Unable to find region code ({}) in RegionMapper for item: {}", regionCode, thisItem.getName());
			
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}

		logger.debug("Established Region Code ({})", regionCode);

		
		File inSrcFile = thisItem.getFile();
		File inBaseFolder = (File)thisItem.getBaseSpecification();
		
		// TimeZone of input datetime is taken from region Config.
		mInputDateFormat.setTimeZone(regionConfig.getTimeZone());
		mOutputMetadataDateFormat.setTimeZone(regionConfig.getTimeZone());
		
		Date collectionDate = null;  // Will be used both below and further down for creating metadata.
		File accountFolder = null;  // Will be squirrelling this away for adding to metadata.
		
		// First transform the collection folder.
		File parentFolder = inSrcFile.getParentFile();
		
		String transformedCollectionFolders = null;
		try {
			collectionDate = mInputDateFormat.parse(parentFolder.getName());
			transformedCollectionFolders = mOutputYearFolderDateFormat.format(collectionDate)
					+ File.separator 
					+ mOutputChildFolderDateFormat.format(collectionDate);
			
			accountFolder = parentFolder.getParentFile();
		} catch (ParseException e) {
			logger.fatal("Unable to formulate Date/Time from folder ({})", parentFolder.getName());
			StaticUtils.TRACE_METHOD_EXIT(logger);
			return null;
		}
		
		// Build out the destination for the one item.
		//
		// First formulate the destination object path based on source file path, base folder path,
		//   and the settings in the properties file.
		//
		StringBuilder destFilePath = new StringBuilder();
		
		destFilePath.append(inSrcFile.getParentFile().getParentFile().getAbsolutePath() + File.separator);

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
		 * Set the custom metadata for the files.
		 */
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
		
		//
		// Finish up closing the top tag and committing the metadata.
		//
		metadataContent.append("</" + mProps.getMetadataEnclosingTag() + ">\n");
		
		customMeta.put(metadataContent.toString());
		
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
