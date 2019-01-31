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
package nl.rabobank.comet.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hds.hcp.tools.comet.utils.StaticUtils;

public class RegionMapperProperties {
	
	static final File DEFAULT_FILE = new File("RegionMapper.properties");
	static final String DEFAULT_FILENAME_PROPERTY = RegionMapperProperties.class.getPackage().getName() + ".regionmapper.properties.file";

	private File mPropertiesFile = DEFAULT_FILE;
	protected Properties mProps;
	
	private TreeMap<String, RegionSpec> mRegionSpecs;
	
	protected Logger logger = LogManager.getLogger();

	public RegionMapperProperties() {
		this(DEFAULT_FILENAME_PROPERTY);
	}
	
	public RegionMapperProperties(File inFile) {
		mPropertiesFile = inFile;
		
		if ( ! mPropertiesFile.exists() || ! mPropertiesFile.isFile() || ! mPropertiesFile.canRead() ) {
			logger.warn("Property file ({}) is not an existing readable regular file.", mPropertiesFile.getPath());
			mPropertiesFile = null;
		}
	}
	
	public RegionMapperProperties(String inFileNameProperty) {
		String propFile = System.getProperty(inFileNameProperty);
		
		// If we got something from the environment, use it.
		if (null != propFile && 0 < propFile.length()) {
			mPropertiesFile = new File(propFile);
		}
		
		if ( ! mPropertiesFile.exists() || ! mPropertiesFile.isFile() || ! mPropertiesFile.canRead() ) {
			logger.warn("Property file ({}) is not an existing readable regular file.", mPropertiesFile.getPath());
			mPropertiesFile = null;
		}
	}
	
	public void refresh() throws InvalidConfigurationException {
		if (null == mPropertiesFile) {
			throw new InvalidConfigurationException("Not configured with valid property file.");
		}
		
		mProps = new Properties();

		FileInputStream propFileStream = null;
		try {
			propFileStream = new FileInputStream(mPropertiesFile);
			mProps.load(propFileStream);
		} catch (IOException e) {
			logger.fatal("Failed to read properties file (" + mPropertiesFile.getPath() + ").", e);
		} finally {
			if (null != propFileStream) {
				try {
					propFileStream.close();
				} catch (IOException e) {
					// best try.
					logger.fatal("Failed to close InputStream", e);
				}
			}
		}

		mRegionSpecs = new TreeMap<String, RegionSpec>(); // Get new/fresh one.
		
		RegionSpec defaultSpec = new RegionSpec("default");
		defaultSpec.setValues(StaticUtils.resolveEnvVars(mProps.getProperty("namespace.default")),
				StaticUtils.resolveEnvVars(mProps.getProperty("tenant.default")), 
				StaticUtils.resolveEnvVars(mProps.getProperty("hcpname.default")), 
				StaticUtils.resolveEnvVars(mProps.getProperty("username.default")), 
				StaticUtils.resolveEnvVars(mProps.getProperty("encodedPassword.default")),
				TimeZone.getTimeZone(StaticUtils.resolveEnvVars(mProps.getProperty("timeZone.default", "UTC"))));
		

		mRegionSpecs.put("default",  defaultSpec);
		
		Enumeration<Object> keys=mProps.keys();
		while (keys.hasMoreElements()) {
			String currentKey = (String)keys.nextElement();
			
			String parts[] = currentKey.split("\\.");
			if (2 == parts.length && parts[0].equals("id") && ! parts[1].equals("default")) {
				Integer specIdx = new Integer(parts[1]);

				String specIdentifier = mProps.getProperty(currentKey);

				if ( ! mRegionSpecs.containsKey(specIdentifier) ) {
					RegionSpec newSpec = new RegionSpec(specIdentifier);

					String namespace = StaticUtils.resolveEnvVars(mProps.getProperty("namespace." + specIdx, defaultSpec.getNamespace()));
					String tenant = StaticUtils.resolveEnvVars(mProps.getProperty("tenant." + specIdx, defaultSpec.getTenant()));
					String hcpname = StaticUtils.resolveEnvVars(mProps.getProperty("hcpname." + specIdx, defaultSpec.getHCPName()));
					String username = StaticUtils.resolveEnvVars(mProps.getProperty("username." + specIdx, defaultSpec.getUserName()));
					String encodedPassword = StaticUtils.resolveEnvVars(mProps.getProperty("encodedPassword." + specIdx, defaultSpec.getEncodedPassword()));
					TimeZone timeZone = TimeZone.getTimeZone(StaticUtils.resolveEnvVars(mProps.getProperty("timeZone." + specIdx, defaultSpec.getTimeZone().getID())));
					
					// Makes sure we have resolved all needed values.
					if (null == namespace || null == tenant 
							|| null == hcpname || null == username || null == encodedPassword) {
						logger.fatal("Invalid configuration for region map with index {}. Either specific or default value is mising for a field.", specIdx);
						
						// TODO Throw some error here.
						throw new InvalidConfigurationException("Missing required Region Mapper for configuration index " + specIdx + ". Verify there is a default value for all fields or specific value for mapper index.");
					}
					
					newSpec.setValues(namespace, tenant, hcpname, username, encodedPassword, timeZone);
						
					mRegionSpecs.put(specIdentifier, newSpec);
				}
			}
		}
	}

	public RegionSpec getMatch(String inIdentifier) throws InvalidConfigurationException {
		if (null == mRegionSpecs) refresh();
		
		return mRegionSpecs.get(inIdentifier);
	}
}
