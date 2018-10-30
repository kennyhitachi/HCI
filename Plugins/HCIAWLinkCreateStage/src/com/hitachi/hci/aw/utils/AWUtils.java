package com.hitachi.hci.aw.utils;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;


public class AWUtils {
	public static final String AUTH_HEADER = "Authorization";
	public static final String AW_LOGIN_URI = "/fss/public/login/oauth";
	public static final String AW_FILE_UPLOAD_URI = "/fss/public/file/stream/create";
	public static final String AW_CREATE_LINK_URI = "/fss/public/link/create";
	public static final String SCHEME_SSL = "https";
	public static final String SCHEME = "http";
	public static final String HTTP_SEPERATOR = "/";
	public static final String HTTP_REST = "/rest";
	public static final String EMPTY_STRING = "";
	
	private static class AWTrustManager implements X509TrustManager {

		AWTrustManager() {
		}
		
		public void checkClientTrusted(X509Certificate[] chain, String authType)
		         throws CertificateException {}

		
		public void checkServerTrusted(X509Certificate[] chain, String authType)
		              throws CertificateException {}

		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}
	}

	private static int HTTPSPort = 443;
	private static int HTTPPort = 80;

	public static int getHTTPSPort() { return HTTPSPort; };
	public static void setHTTPSPort(int port) {
		HTTPSPort = port;
	}

	public static int getHTTPPort() { return HTTPPort; };
	public static void setHTTPPort(int port) {
		HTTPPort = port;
	}

	public static final Integer DEFAULT_CONNECTION_TIMEOUT=5000; 
	public static final Integer DEFAULT_MAX_CONNECTIONS_PER_ROUTE=50;
	public static final Integer DEFAULT_MAX_CONNECTIONS=1000;


	public static HttpClient initHttpClient() throws Exception
	{
		return initDefaultHttpClient(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
	}
	
	public static HttpClient initHttpClient(int inConnectionTimeout, int inMaxConnections, int inMaxConnectionsPerRoute) throws Exception
	{
		return initDefaultHttpClient(inConnectionTimeout, inMaxConnections, inMaxConnectionsPerRoute);
	}
	
	public static DefaultHttpClient initDefaultHttpClient(int inConnectionTimeout, int inMaxConnections, int inMaxConnectionsPerRoute ) throws Exception
    {
		SSLContext sslcontext = SSLContext.getInstance("TLS");
		sslcontext.init(null, new TrustManager[] {new AWTrustManager()}, null);

		SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslcontext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(new Scheme("http", HTTPPort, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(new Scheme("https", HTTPSPort, sslSocketFactory));
		
		PoolingClientConnectionManager connectionMgr = new PoolingClientConnectionManager(schemeRegistry);

		connectionMgr.setDefaultMaxPerRoute(inMaxConnectionsPerRoute);
		connectionMgr.setMaxTotal(inMaxConnections);

		HttpParams clientParams = new BasicHttpParams();
		
		HttpConnectionParams.setConnectionTimeout(clientParams, inConnectionTimeout);
		
		clientParams.setParameter(ClientPNames.HANDLE_REDIRECTS, false);
		
		clientParams.setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY);
		
		return new DefaultHttpClient(connectionMgr, clientParams);
	}

}
