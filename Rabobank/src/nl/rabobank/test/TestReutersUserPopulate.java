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
package nl.rabobank.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.hds.hcp.tools.comet.utils.StaticUtils;

public class TestReutersUserPopulate {

	private static Logger logger = LogManager.getLogger();
	
	LinkedList<String> mUserList; // Used to populate users when parsing files.

	public class ForcedStopSAXException extends SAXException {

		private static final long serialVersionUID = -8880284669721877221L;

		public ForcedStopSAXException(String arg0) {
			super(arg0);
		}
	}
	
	/****
	 **** SAX Parser Events for extracting User entries for under Participant tag.
	 ****/
	
	// This class processes the SAX events for an generic XML file.
	//  The core idea behind this event handler is create separate XML files
	//  with the tag and all its sub elements.
	private class UserInfoSAXParserEvents extends DefaultHandler {

		boolean bInParentTag = true;
		StringBuilder sUserValue;
		String sParentTag;
		
		UserInfoSAXParserEvents(String inParentTag) {
			super();
			sParentTag = inParentTag;
			if ( null != inParentTag )
			    bInParentTag = false;
		}
		
		public void startElement(String namespaceURI, String localName, String qName, Attributes attrs) 
		   throws SAXException {
			
			// Did we find the tag we are looking for?
			if ( null != sParentTag && qName.equals(sParentTag)) {
				bInParentTag = true;
				mUserList = null;  // Start with a fresh list with every parse attempt.
			} else if (bInParentTag && qName.equals("User")) {
				// There is a User, so if we don't have one yet, allocate a user list.
				if (null == mUserList) {
					mUserList = new LinkedList<String>();
				}
				sUserValue = new StringBuilder();  // Create the string buffer to put user value in.
			}
		}
		
		public void endElement(String namespaceURI, String localName, String qName) throws ForcedStopSAXException {
			if (bInParentTag) {
				if (null != sParentTag && qName.equals(sParentTag)) {
					bInParentTag = false;
					
					throw new ForcedStopSAXException("Finished Parent Tag");
					
				} else if (qName.equals("User")) {
					mUserList.add(sUserValue.toString());
					
					sUserValue = null;
				}
			}
		}
		
		// Function that receives tag value characters.
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (null != sUserValue) {
				sUserValue.append(new String(ch, start, length));
			}
		}
	}
	
	private void PopulateUserList(File inSrcFile, String inParentTag) throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);
		
		mUserList = null; // Blank it out.
		
		/*
		 *  Now feed the Chat file through the parser to build the User List.
		 */
		
		XMLReader xmlReader = null;
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(inSrcFile);
			
			xmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			
			xmlReader.setContentHandler(new UserInfoSAXParserEvents(inParentTag));
			
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

		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	private void AppendUserList(File inSrcFile, StringBuilder metadataContent) throws Exception {
		AppendUserList(inSrcFile, metadataContent, null);
	}
	
	private void AppendUserList(File inSrcFile, StringBuilder metadataContent, String parentTag) throws Exception {
		StaticUtils.TRACE_METHOD_ENTER(logger);

		/*
		 * First Collect the users in the file.
		 */
		PopulateUserList(inSrcFile, parentTag);
		
		if (null != mUserList) {
			Iterator<String> userIter = mUserList.iterator();
			while (userIter.hasNext()) {
				File thisFile = new File(inSrcFile.getParentFile().getParentFile(), "UserInfo_" + userIter.next() + ".xml");

				logger.debug("Appending file to custom metadata: {}", thisFile.getName());
				
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(thisFile));
					String line;
					while ((line = reader.readLine()) != null) {
						if ( ! line.startsWith("<?xml") ) {
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
		
		StaticUtils.TRACE_METHOD_EXIT(logger);
	}
	
	void doit(File inSrcFile) throws Exception {
		StringBuilder metadataContent = new StringBuilder();
		
		File metadataFile = new File(inSrcFile.getParentFile(), inSrcFile.getName() + "_Metadata");
		
		if ( ! metadataFile.exists() ) {
			logger.warn("Metadata file for attachment does not exist. Incomplete metadata for file: {}",
					metadataFile.getAbsolutePath());
		} else {
			AppendUserList(metadataFile, metadataContent);
		}
		
		System.out.println(metadataContent.toString());
	}
	
	public static void main(String[] args) {
		TestReutersUserPopulate me = new TestReutersUserPopulate();
		
		try {
			me.doit(new File("12.rtf"));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
