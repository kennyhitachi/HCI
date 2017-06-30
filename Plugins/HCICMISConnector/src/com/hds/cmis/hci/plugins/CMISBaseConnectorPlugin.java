package com.hds.cmis.hci.plugins;

import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

import com.hds.ensemble.sdk.connector.ConnectorMode;
import com.hds.ensemble.sdk.connector.ConnectorOptionalMethod;
import com.hds.ensemble.sdk.connector.ConnectorPlugin;
import com.hds.ensemble.sdk.connector.ConnectorPluginCategory;
import com.hds.ensemble.sdk.exception.ConfigurationException;
import com.hds.ensemble.sdk.exception.PluginOperationFailedException;
import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentPagedResults;
import com.hds.ensemble.sdk.plugin.PluginCallback;
import com.hds.ensemble.sdk.plugin.PluginConfig;
import com.hds.ensemble.sdk.plugin.PluginSession;

public class CMISBaseConnectorPlugin implements ConnectorPlugin {

	@Override
	public PluginConfig getDefaultConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDisplayName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getHost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getPort() throws ConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PluginSession startSession() throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void validateConfig(PluginConfig arg0) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ConnectorPlugin build(PluginConfig arg0, PluginCallback arg1) throws ConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream get(PluginSession arg0, URI arg1) throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectorPluginCategory getCategory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentPagedResults getChanges(PluginSession arg0, String arg1)
			throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Document getMetadata(PluginSession arg0, URI arg1)
			throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectorMode getMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSubCategory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Document> list(PluginSession arg0, Document arg1)
			throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Document> listContainers(PluginSession arg0, Document arg1)
			throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream openNamedStream(PluginSession arg0, Document arg1, String arg2)
			throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Document root(PluginSession arg0) throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supports(ConnectorOptionalMethod arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void test(PluginSession arg0) throws ConfigurationException, PluginOperationFailedException {
		// TODO Auto-generated method stub
		
	}

}
