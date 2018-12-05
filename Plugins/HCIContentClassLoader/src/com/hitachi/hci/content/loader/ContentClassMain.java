package com.hitachi.hci.content.loader;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hitachi.hci.content.utils.ContentClassRequest;
import com.hitachi.hci.content.utils.ContentProperties;
import com.hitachi.hci.content.utils.HCIToken;
import com.hitachi.hci.content.utils.HCPUtils;

public class ContentClassMain {

	private boolean bIsInitialized;
	private static boolean bIsDebugEnabled;

	private static final String AUTH_URI = "auth/oauth";
	private static final String CONTENTCLASS_URI = "api/admin/contentclasses";
	private static final String HCI_PORT = "8000";
	private static final String SCHEME = "https";
	private static final String convertToLowerCase = "(?i)";
	private static final String[] FORMAT = { "xlsx", "xls" };

	private static final String HTTP_SEPERATOR = "/";

	private static HttpClient mHttpClient = null;
	private static Object HttpClientLock = new Object();
	private String mAccessToken;

	private static Workbook mWorkbook;

	private static String mFilePath;
	private static String mHCIUserName;
	private static String mHCIPassword;
	private static String mHCIDNSName;

	private HashMap<String, String> classMap;

	public String getHCIAuthURL() {
		return getHCIBaseURL() + AUTH_URI + HTTP_SEPERATOR;

	}

	public String getHCIBaseURL() {
		return SCHEME + ":" + HTTP_SEPERATOR + HTTP_SEPERATOR + mHCIDNSName + ":" + HCI_PORT + HTTP_SEPERATOR;

	}

	public String getHCIContentClassURL() {
		return getHCIBaseURL() + CONTENTCLASS_URI;

	}

	private String getAccessToken() {
		HttpResponse httpResponse = null;
		ArrayList<NameValuePair> postParameters;
		ObjectMapper mapper = new ObjectMapper();
		HCIToken hciToken = null;

		try {
			/*
			 * Setup the POST request
			 */
			HttpPost httpRequest = new HttpPost(getHCIAuthURL());
			postParameters = new ArrayList<NameValuePair>();
			postParameters.add(new BasicNameValuePair("grant_type", "password"));
			postParameters.add(new BasicNameValuePair("username", mHCIUserName));
			postParameters.add(new BasicNameValuePair("password", mHCIPassword));
			postParameters.add(new BasicNameValuePair("scope", "*"));
			postParameters.add(new BasicNameValuePair("client_secret", "hci-client"));
			postParameters.add(new BasicNameValuePair("client_id", "hci-client"));

			httpRequest.setEntity(new UrlEncodedFormEntity(postParameters));

			/*
			 * Now execute the POST request.
			 */

			httpResponse = mHttpClient.execute(httpRequest);

			// For debugging purposes, dump out the HTTP Response.
			if (bIsDebugEnabled) {
				HCPUtils.dumpHttpResponse(httpResponse);
			}

			// If the return code is anything BUT 200 range indicating
			// success, we have to throw an exception.
			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {

				// Clean up after ourselves and release the HTTP connection
				// to the connection manager.
				// EntityUtils.consume(httpResponse.getEntity());
				System.out.println(httpResponse.getStatusLine().getReasonPhrase());
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}

			String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8");
			if (bIsDebugEnabled) {
				System.out.println(jsonResponseString);
			}
			hciToken = mapper.readValue(jsonResponseString, HCIToken.class);
			EntityUtils.consume(httpResponse.getEntity());
			System.out.println("INFO: Refreshed Access Token.");
		} catch (NoRouteToHostException nrhe) {
			System.out.println("ERROR: Unable to reach the specified host : " + mHCIDNSName + ".");
			System.out.println("INFO: Exiting program.");
			System.exit(1);
		}
		catch (Exception e) {
			System.out.println("ERROR: Unable to fetch the access token");
			System.out.println("INFO: Exiting program.");
			System.exit(1);

		}
		return hciToken.getAccess_token();
	}

	private void initialize() throws Exception {

		if (!bIsInitialized) {

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
			classMap = new HashMap<String, String>();
			setAccessToken(getAccessToken());
			setContentMap();
			File file = new File(mFilePath);
			if (file.exists()) {
				if (ArrayUtils.contains(FORMAT, getExtension(file.getAbsolutePath()))) {
					mWorkbook = WorkbookFactory.create(new File(mFilePath));
				} else {
					System.out.println("ERROR: Invalid File Format specified. Expected: (xlsx or xls). Found: "
							+ file.getName() + ".");
					System.out.println("INFO: Exiting program.");
					System.exit(1);
				}
			} else {
				System.out.println("ERROR: Unable to locate Input File: " + file.getName() + ".");
				System.out.println("INFO: Exiting program.");
				System.exit(1);
			}
			bIsInitialized = true;
		}

		System.out.println("INFO: Initialized Client.");

	}

	private String getExtension(String absolutePath) {
		String extension = "";

		int i = absolutePath.lastIndexOf('.');
		int p = Math.max(absolutePath.lastIndexOf('/'), absolutePath.lastIndexOf('\\'));

		if (i > p) {
			extension = absolutePath.substring(i + 1);
		}
		return extension;
	}

	private void setContentMap() throws Exception {

		ObjectMapper responseMapper = new ObjectMapper();
		ObjectMapper requestMapper = new ObjectMapper();
		responseMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		requestMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		ContentClassRequest[] cc;

		HttpGet httpRequest = new HttpGet(getHCIContentClassURL());

		httpRequest.setHeader("Accept", "application/json");
		httpRequest.setHeader("Authorization", "Bearer " + mAccessToken);

		/*
		 * Now execute the POST request.
		 */
		// httpRequest.removeHeaders(arg0);
		HttpResponse httpResponse = mHttpClient.execute(httpRequest);

		// For debugging purposes, dump out the HTTP Response.
		if (bIsDebugEnabled) {
			HCPUtils.dumpHttpResponse(httpResponse);
		}
		// If the return code is anything BUT 200 range indicating
		// success, we have to throw an exception.
		if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {

			// Clean up after ourselves and release the HTTP connection
			// to the connection manager.
			// EntityUtils.consume(httpResponse.getEntity());
			System.out.println(httpResponse.getStatusLine().getReasonPhrase());
			throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
					"Unexpected status returned from " + httpRequest.getMethod() + " ("
							+ httpResponse.getStatusLine().getStatusCode() + ": "
							+ httpResponse.getStatusLine().getReasonPhrase() + ")");

		}

		String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8");
		cc = responseMapper.readValue(jsonResponseString, ContentClassRequest[].class);
		EntityUtils.consume(httpResponse.getEntity());

		for (int i = 0; i < cc.length; i++) {
			classMap.put(cc[i].getName(), cc[i].getUuid());
		}

		System.out.println("INFO: Refreshed the HCI Content Class Map");

	}

	private void loadContentClass() throws Exception {

		initialize();

		ObjectMapper mapper = new ObjectMapper();

		Iterator<Sheet> sheetIterator = mWorkbook.sheetIterator();

		while (sheetIterator.hasNext()) {
			Sheet sheet = sheetIterator.next();

			ContentClassRequest ccRequest = new ContentClassRequest();
			ArrayList<ContentProperties> propList = new ArrayList<ContentProperties>();
			ccRequest.setModelVersion("1.1.0");
			ccRequest.setName(sheet.getSheetName());
			ccRequest.setDescription("");
			ccRequest.setUuid("");

			DataFormatter dataFormatter = new DataFormatter();

			Iterator<Row> rowIterator = sheet.rowIterator();
			rowIterator.next(); // skip the header
			
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				String cellvalue = "";
				ContentProperties cProps = new ContentProperties();
				Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					String cellValue = dataFormatter.formatCellValue(cell).trim();
					cellvalue = cellvalue + "\t" + cellValue;
				}
				String[] cellvalues = cellvalue.split("\t");
				if (cellvalues.length > 1) {
					cProps.setModelVersion("1.1.0");
					cProps.setName(cellvalues[2]);
					cProps.setType("PATTERN");
					if (cellvalues.length >= 5 && cellvalues[4].contains(convertToLowerCase)) {
						cProps.setExpression(cellvalues[4] + cellvalues[3]);
					} else {
						cProps.setExpression(cellvalues[3]);
					}
					propList.add(cProps);
				}

			}
			ccRequest.setContentProperties(propList);
			String jsonBody = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ccRequest);
			if (bIsDebugEnabled) {
				System.out.println(jsonBody);
			}
			HttpPost httpRequest = new HttpPost(getHCIContentClassURL());

			httpRequest.setHeader("Content-Type", "application/json");
			httpRequest.setHeader("Accept", "application/json");
			httpRequest.setHeader("Authorization", "Bearer " + mAccessToken);

			httpRequest.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

			/*
			 * Now execute the POST request.
			 */
			// httpRequest.removeHeaders(arg0);
			HttpResponse httpResponse = mHttpClient.execute(httpRequest);

			if (httpResponse.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
				EntityUtils.consume(httpResponse.getEntity());
				System.out.println(
						"INFO: Content Class (" + sheet.getSheetName() + ") already Exists. Initiating Update.");
				String uuid = classMap.get(ccRequest.getName());

				ccRequest.setUuid(uuid);
				String jsonEditBody = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ccRequest);

				HttpPut httpPutRequest = new HttpPut(getHCIContentClassURL() + HTTP_SEPERATOR + uuid);

				httpPutRequest.setHeader("Content-Type", "application/json");
				httpPutRequest.setHeader("Accept", "application/json");
				httpPutRequest.setHeader("Authorization", "Bearer " + mAccessToken);

				httpPutRequest.setEntity(new StringEntity(jsonEditBody, StandardCharsets.UTF_8));

				/*
				 * Now execute the PUT request.
				 */
				httpResponse = mHttpClient.execute(httpPutRequest);
			}

			// For debugging purposes, dump out the HTTP Response.
			if (bIsDebugEnabled) {
				HCPUtils.dumpHttpResponse(httpResponse);
			}

			// If the return code is anything BUT 200 range indicating
			// success, we have to throw an exception.
			if (2 != (int) (httpResponse.getStatusLine().getStatusCode() / 100)) {

				// Clean up after ourselves and release the HTTP connection
				// to the connection manager.
				// EntityUtils.consume(httpResponse.getEntity());
				System.out.println(httpResponse.getStatusLine().getReasonPhrase());
				throw new HttpResponseException(httpResponse.getStatusLine().getStatusCode(),
						"Unexpected status returned from " + httpRequest.getMethod() + " ("
								+ httpResponse.getStatusLine().getStatusCode() + ": "
								+ httpResponse.getStatusLine().getReasonPhrase() + ")");

			}
			if (bIsDebugEnabled) {
				String jsonResponseString = IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8");
				System.out.println(jsonResponseString);
			}
			EntityUtils.consume(httpResponse.getEntity());
			System.out.println("INFO: Successfully Loaded Content Class: " + sheet.getSheetName() + ".");

		}
	}

	public static void main(String[] aArgs) throws Exception {

		Options options = new Options();

		Option excelFilePath = new Option("i", "excelFilePath", true, "Input-File-Path");
		excelFilePath.setRequired(true);
		options.addOption(excelFilePath);

		Option hciUserName = new Option("u", "hciUser", true, "HCI-UserName");
		hciUserName.setRequired(true);
		options.addOption(hciUserName);

		Option hciDNS = new Option("h", "hciHost", true, "HCI-HostName");
		hciDNS.setRequired(true);
		options.addOption(hciDNS);

		Option hciPassword = new Option("p", "hciPassword", true, "HCI-Password");
		hciPassword.setRequired(false);
		options.addOption(hciPassword);

		Option debugFlag = new Option("d", "debug", false, "debug");
		debugFlag.setRequired(false);
		options.addOption(debugFlag);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, aArgs);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("ContentClassLoader", options);

			System.exit(1);
		}

		mFilePath = cmd.getOptionValue("excelFilePath");
		mHCIUserName = cmd.getOptionValue("hciUser");

		char[] password = {};
		if (cmd.hasOption("hciUser")) {
			if (cmd.hasOption("hciPassword")) {
				password = cmd.getOptionValue("hciPassword").toCharArray();
			} else {
				password = System.console().readPassword("Enter password for HCI User %s : ", mHCIUserName);
			}
		}
		mHCIPassword = String.valueOf(password);
		mHCIDNSName = cmd.getOptionValue("hciHost");

		try {
			InetAddress.getByName(mHCIDNSName);
		} catch (UnknownHostException uhe) {
			System.out.println("ERROR: Unknown Host specified: " + mHCIDNSName + ".");
			System.out.println("INFO: Exiting program.");
			System.exit(1);
		}

		if (cmd.hasOption("debug")) {
			bIsDebugEnabled = true;
		} else {
			bIsDebugEnabled = false;
		}

		ContentClassMain mainObject = new ContentClassMain();

		System.out.println("INFO: Loading Content Class");

		mainObject.loadContentClass();

		System.out.println("INFO: All Done!");

	}

	/**
	 * @param mAccessToken
	 *            the mAccessToken to set
	 */
	public void setAccessToken(String mAccessToken) {
		this.mAccessToken = mAccessToken;
	}

}
