package nl.rabobank.comet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.FileSystemItem;
import com.hds.hcp.tools.comet.utils.StaticUtils;

public class BloombergItem extends FileSystemItem {

	static final File DEFAULT_FILE = new File("item.properties");
	static final String DEFAULT_FILENAME_PROPERTY = "nl.rabobank.comet.item.properties.file";

	private File mPropertiesFile = DEFAULT_FILE;
	protected BloombergItemProperties mProps = new BloombergItemProperties();
	
	private static Logger logger = LogManager.getLogger();

	private class BloombergItemProperties extends Properties {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		BloombergItemProperties() {
			String propFile = System.getProperty(DEFAULT_FILENAME_PROPERTY);
			
			// If we got something from the environment, use it.
			if (null != propFile && 0 < propFile.length()) {
				mPropertiesFile = new File(propFile);
			}
	
			refresh();
		}
		
		public void refresh() {
			StaticUtils.TRACE_METHOD_ENTER(logger);
	
			if (null == mPropertiesFile) {
	
				StaticUtils.TRACE_METHOD_EXIT(logger, "No Property file");
				return;  // Don't have a file so do nothing.
			}
			
			if ( ! mPropertiesFile.exists() || ! mPropertiesFile.isFile() || ! mPropertiesFile.canRead() ) {
				logger.warn("Property file ({}) is not an existing readable regular file.", mPropertiesFile.getPath());
				return;
			}
	
			mProps = this;
			
			FileInputStream propsInputStream = null;
			try {
				propsInputStream = new FileInputStream(mPropertiesFile);
				mProps.load(propsInputStream);
			} catch (IOException e) {
				logger.fatal("Failed to read properties file ({}). Reason: \"{}\"", mPropertiesFile.getPath(), e.getMessage());
			} finally {
				if (null != propsInputStream) {
					try {
						propsInputStream.close();
					} catch (IOException e) {
						// best try.
						logger.fatal("Failed to close InputStream", e);
					}
				}
			}
	
			StaticUtils.TRACE_METHOD_EXIT(logger);
		}
		
		/***
		 * 
		 * DESTINATION PROPERTIES
		 * 
		 ***/
		public String getMetadataFilePostFix() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.filePostFix", "_Metadata"));
		}
		public String getAttachmentMetadataFileExtension() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("metadata.message.metadataFileExt", "att"));
		}
		public String getAttachmentFolderName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("attachment.folderName", "attachments"));
		}
	}

	File mMetadataFile;
	File mAttMetadataFile;
	
	public BloombergItem(String inFile, String inBaseFolder) {
		super(inFile, inBaseFolder);
	}
	
	public BloombergItem(BaseWorkItem inFile, BaseWorkItem inBaseFolder) {
		super(inFile, inBaseFolder);
	}
	
	public BloombergItem(File inFile, File inBaseFolder) {
		super(inFile, inBaseFolder);
		
		mMetadataFile = new File(inFile.getAbsolutePath() + mProps.getMetadataFilePostFix());
		
		// Attachment metadata file is for "regular" files only.  Don't set for attachment.
		if (! isAttachmentFile(inFile) ) {
			mAttMetadataFile = new File((inFile.getAbsolutePath()).replaceAll("\\.xml", "." + mProps.getAttachmentMetadataFileExtension()));
		}
	}

	private boolean isAttachmentFile(File inFile) {
		if (null == inFile) return false;
		
		return inFile.getParentFile().getName().equals(mProps.getAttachmentFolderName());
	}
	
	@Override
	public boolean delete() {
		if (null != mMetadataFile && mMetadataFile.exists()) {
			mMetadataFile.delete();
			// No big deal if it fails...  Most important is the actual file.
			// TODO Still toying with the possibility of failing.
		}
		if (null != mAttMetadataFile && mAttMetadataFile.exists()) {
			mAttMetadataFile.delete();
		}
		
		return super.delete();
	}

	@Override
	public boolean setWritable() {
		if (null != mMetadataFile && mMetadataFile.exists()) {
			mMetadataFile.setWritable(true,  true);
			
			// No big deal if it fails...  Most important is the actual file.
			// TODO Still toying with the possibility of failing.
		}
		if (null != mAttMetadataFile && mAttMetadataFile.exists()) {
			mAttMetadataFile.setWritable(true,  true);
		}
		
		return ((File)getHandle()).setWritable(true,  true);
	}
	
	public File getMetadataFile() {
		return mMetadataFile;
	}
	public File getAttachmentMetadataFile() {
		return mAttMetadataFile;
	}
}
