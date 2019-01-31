package nl.rabobank.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class XMLTagToFileProcessorImpl implements XMLTagProcessorInterface {

	String mParams[];
	String mFileName;
	String mTagName;
	OutputStream mOutputStream;
	File mOutputFile;
	Boolean bQuietMode = false;
	Boolean bOverwriteOff = false;
	Boolean bMultipleFiles = false;
	Boolean isInitialized = false;
	int mFileNumber;
	
	@Override
	public boolean initialize(String inParams) {

		mFileNumber = 1;
		
		// Process Params is passed in any.
		if (null != inParams) {

			mParams = inParams.split(",");

			// Process each param.
			for (int i = 0; i < mParams.length; i++) {
				String parts[] = mParams[i].split("=");
				
				if (parts.length != 2) {
					continue;
				}
				
				if (parts[0].equals("output-name")) {
					mFileName = parts[1].trim();
					continue;
				}

				if (parts[0].equals("tag-name")) {
					mTagName = parts[1].trim();
					continue;
				}
				
				if (parts[0].equals("quiet-mode")) {
					bQuietMode = new Boolean(parts[1].trim());
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
			}
		}
		
		// Default mFileName to mTagName if none specified.
		if (null == mFileName )
			mFileName = mTagName;

		isInitialized = true;
		
		return isInitialized;
	}

	@Override
	public OutputStream start() throws Exception {
		if ( ! isInitialized ) {
			throw new IllegalStateException("Object Not initialized");
		}
		
		if (mFileName.equals("-")) {
			mOutputStream = System.out;
		} else {
			if (null != mOutputStream) {
				try {
					mOutputStream.close();
				} catch (IOException e) {
					// Nice try.  Ignore.
				}
			}
			
			mOutputFile = new File((null == mFileName ? mTagName : mFileName) + "_tmp.xml");
			mOutputFile.createNewFile();
				
			mOutputStream = new FileOutputStream(mOutputFile);
		}
		
		return mOutputStream;
	}
	
	@Override
	public void process(String inTagId) throws IOException {
		if ( ! isInitialized) {
			throw new IllegalStateException("Object not initialized");
		}
		
		mOutputStream.flush();
		
		// If not going to stdout, then have file handling to do.
		if ( ! mFileName.equals("-")) {
			StringBuilder finalFileName = new StringBuilder();
			finalFileName.append(mFileName);
			if (null != inTagId) {
				finalFileName.append("_" + inTagId);
			} else {
				if (bMultipleFiles) {
					finalFileName.append(mFileNumber);
					mFileNumber++;
				}
			}
			finalFileName.append(".xml");
			
			File destFile = new File(finalFileName.toString());
			
			if ( bOverwriteOff && destFile.exists()) {
				// TODO Make this better.  An determine if best to abort or not.
				System.out.println("WARNING: File " + destFile.getPath() + " already exists. Not allowed to overwrite.");
				mOutputFile.delete();
			} else if ( ! mOutputFile.renameTo(destFile) ) {
				// TODO This actually is a fatal error....
				System.err.println("ERROR: Failed to rename temp file");
				mOutputFile.delete();
			} else if ( ! bQuietMode ) {
					System.out.println(destFile.getPath());
			}
			
			close();
		}
	}

	@Override
	public void close() {
		if (null != mOutputStream) {
			try {
				mOutputStream.close();
				mOutputStream = null;
			} catch (Exception e) {
				// Do nothing.  Best try.
			}
		}
	}
}
