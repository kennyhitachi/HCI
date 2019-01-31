package nl.rabobank.comet;

import java.io.File;

import com.hds.hcp.tools.comet.BaseWorkItem;
import com.hds.hcp.tools.comet.FileSystemItem;

public class VerintItem extends FileSystemItem {

	File mMetadataFile;
	
	public VerintItem(String inFile, String inBaseFolder) {
		super(inFile, inBaseFolder);
	}
	
	public VerintItem(BaseWorkItem inFile, BaseWorkItem inBaseFolder) {
		super(inFile, inBaseFolder);
	}
	
	public VerintItem(File inFile, File inBaseFolder) {
		super(inFile, inBaseFolder);
		
		try {
			File metadataFolder = new File(inFile.getParentFile().getParent() + File.separator + "IDX");
			if (null != metadataFolder && metadataFolder.isDirectory()) {
				String metadataFileName = metadataFolder.getPath()
						+ File.separator
						+ inFile.getName().substring(0, inFile.getName().lastIndexOf("."))
						+ ".xml";
				mMetadataFile = new File(metadataFileName);
			}
		} catch (NullPointerException e) {
			// If got a null pointer, then must be the folder or file does not exist.
		}
	}
	
	@Override
	public boolean delete() {
		if (null != mMetadataFile && mMetadataFile.exists()) {
			mMetadataFile.delete();
			// No big deal if it fails...  Most important is the actual file.
			// TODO Still toying with the possibility of failing.
		}
		
		return ((File)getHandle()).delete();
	}

	@Override
	public boolean setWritable() {
		if (null != mMetadataFile && mMetadataFile.exists()) {
			mMetadataFile.setWritable(true,  true);
			
			// No big deal if it fails...  Most important is the actual file.
			// TODO Still toying with the possibility of failing.
		}
		
		return ((File)getHandle()).setWritable(true,  true);
	}
	
}
