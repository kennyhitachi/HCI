package com.hds.hci.plugins.stage.utils;

import java.io.File;
import java.util.List;

public class EncryptedFileRecord {
	
	private String hciURI;
	private String hciParentFileName;
	private int fileCount;
	private String hciCreationDateString;
	private List<String> fileNamesList; 
	
	public EncryptedFileRecord(String inHciURI, String inHciParentFileName, List<String> inFileNamesList, int inFileCount, String inHciCreationDateString) {
		super();
		this.setFileNamesList(inFileNamesList);
		this.setHciParentFileName(inHciParentFileName);
		this.setHciURI(inHciURI);
		this.fileCount = inFileCount;
		this.hciCreationDateString = inHciCreationDateString;
	}

	/**
	 * @return the hciURI
	 */
	public String getHciURI() {
		return hciURI;
	}

	/**
	 * @param hciURI the hciURI to set
	 */
	public void setHciURI(String hciURI) {
		this.hciURI = hciURI;
	}

	/**
	 * @return the hciParentFileName
	 */
	public String getHciParentFileName() {
		return hciParentFileName;
	}

	/**
	 * @param hciParentFileName the hciParentFileName to set
	 */
	public void setHciParentFileName(String hciParentFileName) {
		this.hciParentFileName = hciParentFileName;
	}

	/**
	 * @return the fileNamesList
	 */
	public List<String> getFileNamesList() {
		return fileNamesList;
	}

	/**
	 * @param fileNamesList the fileNamesList to set
	 */
	public void setFileNamesList(List<String> fileNamesList) {
		this.fileNamesList = fileNamesList;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(hciURI);
		sb.append(",");
		sb.append(hciParentFileName);
		sb.append(",");
		
		boolean first = true; 
		for (String fileName : fileNamesList) { 
			if (first) { 
				first = false; 
			} else { 
				sb.append(";"); 
			} 
			int index = fileName.lastIndexOf(File.separator);
			sb.append(fileName.substring(index + 1)); 
		}
		sb.append(",");
		sb.append(fileCount);
		sb.append(",");
		sb.append(hciCreationDateString);
		return sb.toString();
		
	}

	/**
	 * @return the fileCount
	 */
	public int getFileCount() {
		return fileCount;
	}

	/**
	 * @param fileCount the fileCount to set
	 */
	public void setFileCount(int fileCount) {
		this.fileCount = fileCount;
	}

	/**
	 * @return the hciCreationDateString
	 */
	public String getHciCreationDateString() {
		return hciCreationDateString;
	}

	/**
	 * @param hciCreationDateString the hciCreationDateString to set
	 */
	public void setHciCreationDateString(String hciCreationDateString) {
		this.hciCreationDateString = hciCreationDateString;
	}

}
