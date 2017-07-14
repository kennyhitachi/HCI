/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.cmis;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.Repository;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.hci.plugins.connector.utils.CMISUtils;

public class CMISGateway {
	
	private String sHost;
	private String sBindingUrl;
	private String sBindingType;
	private String sUserName;
	private String sPassword;
	private String sSsl;
	private PluginCallback callback;

	
	 private static Map<String, Session> connections = new  ConcurrentHashMap<String, Session>();
	 
			

	public CMISGateway(String inUrl, String userName, String userPass, String inBinding, PluginCallback callback) throws Exception {
		this.setUserName(userName);
		this.setPassword(userPass);
		this.sBindingUrl = inUrl;
		this.sBindingType = inBinding;
		this.callback = callback;
		
		 SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
		            Map<String, String> parameters = new HashMap<String,String>();
		            Repository cmisRepository = null;
		            try {
		            parameters.put(SessionParameter.USER, this.getUserName());
		            parameters.put(SessionParameter.PASSWORD, this.getPassword());
		            if (CMISUtils.ATOMPUB.equalsIgnoreCase(this.sBindingType)) {
		              parameters.put(SessionParameter.ATOMPUB_URL,this.sBindingUrl);
		              parameters.put(SessionParameter.BINDING_TYPE,BindingType.ATOMPUB.value());
		            } else {
		              parameters.put(SessionParameter.BROWSER_URL,this.sBindingUrl);
		              parameters.put(SessionParameter.BINDING_TYPE,BindingType.BROWSER.value());
		            }
		            parameters.put(SessionParameter.COMPRESSION, "true");
		            parameters.put(SessionParameter.CACHE_TTL_OBJECTS, "0");
		            List<Repository> repositories =  sessionFactory.getRepositories(parameters);
		            
		            if (repositories == null || repositories.size() == 0) {
		            	throw new PluginOperationFailedException("Failed to find a repository. ");
		            } 
		            cmisRepository = repositories.get(0);
		            
					Session session = cmisRepository.createSession();
		            connections.put(CMISUtils.CMIS_CONNECTION, session);
		            } catch (Exception e) {
		    			
		    			throw new PluginOperationFailedException("Failed to establish a session with the cmis Repository ", (Throwable) e);
		    		} 
	}
	    
	public Session getCMISSession() throws PluginOperationFailedException {

		            return connections.get(CMISUtils.CMIS_CONNECTION);
		
		
	}

	public String getBaseUrl() throws MalformedURLException, PluginOperationFailedException {

		StringBuilder baseUriBuilder = new StringBuilder();
		Folder rootFolder = this.getCMISSession().getRootFolder();
		String rootName = rootFolder.getName().replaceAll("\\s", "%20");
		URL url = new URL(this.sBindingUrl);
		
	    baseUriBuilder.append(url.getProtocol());
		
		baseUriBuilder.append("://");
		baseUriBuilder.append(url.getHost());
		baseUriBuilder.append(":");
		baseUriBuilder.append(url.getPort());
		baseUriBuilder.append(CMISUtils.HTTP_SEPERATOR);
		baseUriBuilder.append(rootName);

		return baseUriBuilder.toString();
	}

	/**
	 * @param url
	 * @return Document
	 * 
	 *         This method returns the HCI document metadata for a given url
	 * @throws IOException
	 * @throws PluginOperationFailedException
	 * 
	 */
	public Document getDocumentMetadata(String inUrl) throws IOException, PluginOperationFailedException {
		String path = this.getPathFromUrl(inUrl);
		CmisObject object = this.getCMISSession().getObjectByPath(path);
		return getDocument(inUrl,CMISUtils.CMIS_DIRECTORY.equals(object.getBaseTypeId()), object);

	}
	
	private String getPathFromUrl(String inUrl) throws MalformedURLException, PluginOperationFailedException {
		String uri = inUrl
				     .replaceAll(this.getBaseUrl(), CMISUtils.EMPTY_STRING)
				     .replaceAll("%20", " ");
		if (uri.isEmpty()){
			return CMISUtils.HTTP_SEPERATOR;
		}
		return uri;
	}

	/**
	 * @param url,container flag
	 * @return Document Iterator
	 * 
	 *         This method returns the Document Iterator for a given container url.
	 *         
	 * @throws PluginOperationFailedException
	 * 
	 */
	public Iterator<Document> getDocumentList(String inUrl, boolean listContainers)
			throws PluginOperationFailedException {
		LinkedList<Document> documentList = new LinkedList<Document>();
		try {
			String inUri = this.getPathFromUrl(inUrl);
			CmisObject object = this.getCMISSession().getObjectByPath(inUri);
			Folder folder = (Folder)object;
			 ItemIterable<CmisObject> children = folder.getChildren();
	            for (CmisObject child : children) {
	            	String url = this.getUrlFromObject(child);
	            	if (listContainers && CMISUtils.CMIS_DIRECTORY.equalsIgnoreCase(child.getBaseTypeId().toString())) {
	            		documentList
								.add(getDocument(url,listContainers, child));
					} else if (!listContainers && CMISUtils.CMIS_FILE.equalsIgnoreCase(child.getBaseTypeId().toString())) {
						documentList
								.add(getDocument(url,listContainers, child));
					} else if (!listContainers && CMISUtils.CMIS_DIRECTORY.equalsIgnoreCase(child.getBaseTypeId().toString())) {
						documentList.add(
								getDocument(url,!listContainers, child));
					}
	            }
			return documentList.iterator();
		} catch (Exception e) {
			throw new PluginOperationFailedException("Failed to crawl " + inUrl, (Throwable) e);
		}

	}
    
	private String getUrlFromObject(CmisObject child) throws MalformedURLException, PluginOperationFailedException {
		String path = new String();
		if (CMISUtils.CMIS_DIRECTORY.equalsIgnoreCase(child.getBaseTypeId().toString()) ) {
			Folder folder = (Folder)child;
			path = folder.getPath();
		} else {
			org.apache.chemistry.opencmis.client.api.Document document = (org.apache.chemistry.opencmis.client.api.Document) child;
			List<String> paths = document.getPaths();
			if (!paths.isEmpty()){
			  path = paths.get(0);
			}
		}
		return this.getBaseUrl()+path.replaceAll("\\s", "%20");
	}

	// Create a document with basic HCI metadata.
	private Document getDocument(String inUrl, Boolean isContainer, CmisObject entry) throws IOException {
		
		DocumentBuilder builder = this.callback.documentBuilder();
        
		if (isContainer) {
			builder.setIsContainer(true).setHasContent(false);
		} else {
			builder.setStreamMetadata(StandardFields.CONTENT, Collections.emptyMap());
		}
		builder.addMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(inUrl).build());
		builder.addMetadata(StandardFields.URI, StringDocumentFieldValue.builder().setString(inUrl).build());
		builder.addMetadata(StandardFields.DISPLAY_NAME, StringDocumentFieldValue.builder().setString(entry.getName()).build());
		builder.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());
		return builder.build();

	}

	public Document getRootDocument(String uri, boolean isContainer) throws IOException, PluginOperationFailedException {
		
		if (!uri.endsWith(CMISUtils.HTTP_SEPERATOR)) {
			uri = uri + CMISUtils.HTTP_SEPERATOR;
		}

		return getDocumentMetadata(this.getBaseUrl()+uri);
	}

	// Get the Content of a CMIS file as a String , which is later converted into an InputStream and set as HCI_content.
	public InputStream getContentStream(String uri) throws IllegalStateException, IOException, PluginOperationFailedException {
		String inUri = this.getPathFromUrl(uri);
		CmisObject object = this.getCMISSession().getObjectByPath(inUri);
			 org.apache.chemistry.opencmis.client.api.Document document = (org.apache.chemistry.opencmis.client.api.Document) object;
             ContentStream content = document.getContentStream();
             return content.getStream();
	}

	/**
	 * @return the mHost
	 */
	public String getHost() {
		return sHost;
	}

	/**
	 * @param mHost
	 *            the mHost to set
	 */
	public void setHost(String sHost) {
		this.sHost = sHost;
	}

	/**
	 * @param mPort
	 *            the mPort to set
	 */
	public void setPort(int iPort) {
	}

	/**
	 * @return the mUserName
	 */
	public String getUserName() {
		return sUserName;
	}

	/**
	 * @param mUserName
	 *            the mUserName to set
	 */
	public void setUserName(String sUserName) {
		this.sUserName = sUserName;
	}

	/**
	 * @return the mPassword
	 */
	public String getPassword() {
		return sPassword;
	}

	/**
	 * @param mPassword
	 *            the mPassword to set
	 */
	public void setPassword(String sPassword) {
		this.sPassword = sPassword;
	}

	/**
	 * @return the mSsl
	 */
	public String getSsl() {
		return sSsl;
	}

	/**
	 * @param mSsl
	 *            the mSsl to set
	 */
	public void setSsl(String sSsl) {
		this.sSsl = sSsl;
	}

	/**
	 * @return the callback
	 */
	public PluginCallback getCallback() {
		return callback;
	}

	/**
	 * @param callback
	 *            the callback to set
	 */
	public void setCallback(PluginCallback callback) {
		this.callback = callback;
	}
}
