package com.hitachi.hci.content.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.http.client.HttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hitachi.hci.content.utils.HCPUtils;
import com.hitachi.hci.content.utils.StaticUtils;
import com.hitachi.hci.content.utils.TextEncoder;

public class ContentClassProperties {

		protected static Logger logger = LogManager.getLogger();

		// Static "Instance" mechanism.
		protected static ContentClassProperties mStaticMe;

		public static synchronized ContentClassProperties getRetentionPropertiesInstance() {
			if (null == mStaticMe) {
				try {
					mStaticMe = new ContentClassProperties();
				} catch (IOException e) {
					logger.fatal("Unexpected Exception initialization Properties", e);
					// Just fall through and return null.
				}
			}
			return mStaticMe;
		}

		public static synchronized ContentClassProperties getRetentionPropertiesInstance(String inPropertiesFile) {
			if (null == mStaticMe) {
				try {
					mStaticMe = new ContentClassProperties(inPropertiesFile);
				} catch (IOException e) {
					logger.fatal("Unexpected Exception initialization Properties", e);
					// Just fall through and return null.
				}
			}
			return mStaticMe;
		}

		static final String DEFAULT_PROPERTIES_FILE = "retention.properties";

		private String mPropertiesFilename = DEFAULT_PROPERTIES_FILE;
		protected Properties mProps;
		private HttpClient mHttpClient;

		public Object getHCIUserName;

		public Object getHCIPassword;

		public ContentClassProperties() throws IOException {
			String propFile = System.getProperty("com.hds.hcp.tools.retention.properties.file");

			// If we got something from the environment, use it.
			if (null != propFile && 0 < propFile.length()) {
				mPropertiesFilename = propFile;
			}

			refresh();
		}

		public ContentClassProperties(String inPropertiesFile) throws IOException {
			mPropertiesFilename = inPropertiesFile;

			refresh();
		}

		void refresh() throws IOException {
			mProps = new Properties();

			FileInputStream propInputStream = new FileInputStream(mPropertiesFilename);
			mProps.load(propInputStream);

			try {
				propInputStream.close();
			} catch (IOException e) {
				// Don't really care.
			}

			mHttpClient = null;
		}

		/*
		 * HCP System-Level Configuration Details
		 */

		public String getHcpHostServer() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("hcp.dnsName")).trim();
		}
		
		public String getHCIHostServer() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("hci.dnsName")).trim();
		}

	   
	   
	    
	    public String getRawHcpSysUsername() { return StaticUtils.resolveEnvVars(mProps.getProperty("hcp.system.admin.username")); }
	    
	   

	    public String getRawHcpSysPassword() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("hcp.system.admin.password"));
	    }
	    
	    
	    public String getRawHciSysUsername() { return StaticUtils.resolveEnvVars(mProps.getProperty("hci.system.admin.username")); }
	    
		   

	    public String getRawHciSysPassword() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("hci.system.admin.password"));
	    }
	   
	    
	   

		public Boolean shouldDumpHTTPHeaders() {
			return new Boolean(mProps.getProperty("execution.debugging.httpheaders", "false"));
		}
		
		public Boolean shouldDumpConfigToConsole() {
			return new Boolean(mProps.getProperty("execution.dump.config.to.console", "true"));
		}

		/*
		 * database connection properties
		 */
	    public String getDatabaseServerHost() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("database.server.name")).trim();
	    }

	    public String getDatabaseServerPort() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("database.server.port", "3306")).trim();
	    }

		public String getDatabaseUsername() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("database.user.name")).trim();
		}

	    public String getRawDatabasePassword() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("database.user.password")).trim();
	    }
	    
	    public String getDatabasePassword() {
	        String retval=getRawDatabasePassword();

	        // If the password is COMET encoded, decode it.
	        if (isDatabasePasswordEncoded()) {
	            retval = TextEncoder.decode(retval);
	        }

	        return retval;
	    }
	    
	    public Boolean isDatabasePasswordEncoded() {
	        return new Boolean(mProps.getProperty("database.user.isPasswordEncoded", "false"));
	    }
	        
		public String getDatabaseName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("database.database.name")).trim();
		}

		public String getQuery() {
			return mProps.getProperty("database.sql.query");
		}

		public String getUpdateQuery() {
			return mProps.getProperty("database.sql.update.query");
		}

		public String getThresholdPeriod() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("retention.threshold.period")).trim();
		}

		public String getRetentionPeriod() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("retention.period")).trim();
		}

		public String getRetentionClass() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("retention.class")).trim();
		}

		/*
		 * mailing specific properties
		 */
		
		public Boolean shouldSendReport() {
			return new Boolean(mProps.getProperty("mail.send.report", "false").trim());
		}
		
		public String getMailerToList() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.list.to"));
		}

		public String getMailerCCList() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.list.cc"));
		}

		public String getMailerBCCList() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.list.bcc"));
		}

		public String getMailerFromAddress() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.list.from"));
		}

		public String getMailerHostName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.server.hostname")).trim();
		}
		
		public String getMailerPort() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.server.port","25")).trim();
		}

		public String enableAuthentication() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.server.enable.authentication", "true"));
		}

		public String enableTLS() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.server.enable.tls", "true"));
		}

		public String getMailerUserName() {
			return StaticUtils.resolveEnvVars(mProps.getProperty("mail.account.username")).trim();
		}

	    public String getRawMailerPassword() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("mail.account.password")).trim();
	    }
	    
	    public String getMailerPassword() {
	        String retval=getRawMailerPassword();

	        // If the password is COMET encoded, decode it.
	        if (isMailerPasswordEncoded()) {
	            retval = TextEncoder.decode(retval);
	        }

	        return retval;
	    }
	    
	    public Boolean isMailerPasswordEncoded() {
	        return new Boolean(mProps.getProperty("mail.account.isPasswordEncoded", "false"));
	    }
	        
	    public String getMailerSubject() {
	        return StaticUtils.resolveEnvVars(mProps.getProperty("mail.subject")).trim();
	    }
	    
		public Boolean shouldUseHTML() {
			return new Boolean(mProps.getProperty("mail.useHTML", "false").trim());
		}

		/*
		 * HTTP Connection Information
		 */
		public synchronized HttpClient getHTTPClient() throws Exception {
			if (null == mHttpClient) {
				mHttpClient = HCPUtils.initHttpClient(60, 200, 10);
			}
			return mHttpClient;
		}

		/*
		 * Helper Routines
		 */
		public String getMapiBaseApi() {
			return "https://admin." + getHcpHostServer() + ":9090/mapi";
		}

		public String getTenantMapiBaseApi() {
			return getMapiBaseApi() + "/tenants";
		}

		public String getTenatMapiApi(String tenant) {
			return getTenantMapiBaseApi() + "/" + tenant;
		}

		public String getNamespaceMapiBaseApi(String tenant) {
			return "https://" + tenant + "." + getHcpHostServer() + ":9090/mapi/tenants/" + tenant + "/namespaces";
		}

		public String getNamespaceMapiApi(String tenant, String namespace) {
			return getNamespaceMapiBaseApi(tenant) + "/" + namespace;
		}

		public String getComplianceMapiApi(String tenant, String namespace) {
			return getNamespaceMapiApi(tenant, namespace) + "/complianceSettings";

		}

		public String getRetentionClassMapiApi(String tenant, String namespace) {
			return getNamespaceMapiApi(tenant, namespace) + "/retentionClasses/" + getRetentionClass();
		}

		public String getOAuthUrl() {
			// TODO Auto-generated method stub
			
			//return "https://" + getHCIHostServer()+"/auth/oauth/";
			return "https://192.168.1.49/auth/oauth/";
		}

		public String getHCIPassword() {
			// TODO Auto-generated method stub
			return StaticUtils.resolveEnvVars(mProps.getProperty("hci.admin.password")).trim();
		}

		public String getHCIUserName() {
			// TODO Auto-generated method stub
			return StaticUtils.resolveEnvVars(mProps.getProperty("hci.admin.username")).trim();
		}

		public String getDictionaryDocument() {
			// TODO Auto-generated method stub
			return StaticUtils.resolveEnvVars(mProps.getProperty("hci.dictionary.document")).trim();
		}

}
