package com.hds.custom.hci;

import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hds.custom.apihelpers.HCPUtils;
import com.hds.custom.utils.HCIToken;

public class HCILogin {

	private String mUserName;
	private String mPassword;
	private boolean bIsInitialized;

	private static HttpClient mHttpClient = null;
	private static Object HttpClientLock = new Object();

	public HCILogin(String userName, String userPass) {
		this.mUserName = userName;
		this.mPassword = userPass;
	}

	public void initialize() throws Exception {
		if (!bIsInitialized) {
			// mHttpClient = inHttpClient;

			// Make this thread safe for this static HTTP client.
			synchronized (HttpClientLock) {
				if (null == mHttpClient) {
					// Get our http client. NOTE: All threads will use the same
					// instance of httpClient since it
					// is thread safe and will help with pooling between all
					// threads.
					mHttpClient = HCPUtils.initHttpClient();
				}
			}
			bIsInitialized = true;
		}
	}

	public String login() {

		HttpResponse httpResponse = null;
		ArrayList<NameValuePair> postParameters;
		ObjectMapper mapper = new ObjectMapper();

		try {
			/*
			 * Setup the POST request to start the query and to get the input
			 * stream of the response that will be processed by the SAX parser.
			 */
			HttpPost httpRequest = new HttpPost("https://192.168.1.15:8000/auth/oauth/");

			postParameters = new ArrayList<NameValuePair>();
			postParameters.add(new BasicNameValuePair("grant_type", "password"));
			postParameters.add(new BasicNameValuePair("username", this.mUserName.trim()));
			postParameters.add(new BasicNameValuePair("password", this.mPassword.trim()));
			postParameters.add(new BasicNameValuePair("scope", "*"));
			postParameters.add(new BasicNameValuePair("client_secret", "hci-client"));
			postParameters.add(new BasicNameValuePair("client_id", "hci-client"));
			//postParameters.add(new BasicNameValuePair("realm", "local"));

			httpRequest.setEntity(new UrlEncodedFormEntity(postParameters));

			/*
			 * Now execute the POST request.
			 */
			// httpRequest.removeHeaders(arg0);
			httpResponse = mHttpClient.execute(httpRequest);

			// For debugging purposes, dump out the HTTP Response.

			HCPUtils.dumpHttpResponse(httpResponse);

			// If the return code is anything BUT 200 range indicating
			// success, we have to throw an exception.
			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {

				// Clean up after ourselves and release the HTTP connection
				// to the connection manager.
				//EntityUtils.consume(httpResponse.getEntity());
				System.out.println(httpResponse.getStatusLine().getReasonPhrase());
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}
			
			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8");
			
			HCIToken hciToken = mapper.readValue(jsonResponseString, HCIToken.class);
			EntityUtils.consume(httpResponse.getEntity());
			
			return hciToken.getAccess_token();

		} catch (Exception e) {
			//do some logging

		} finally {
		}
		return "";
	}

	
}
