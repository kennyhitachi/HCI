/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.hds.ensemble.sdk.model.Document;
import com.hds.ensemble.sdk.model.DocumentBuilder;
import com.hds.ensemble.sdk.model.StandardFields;
import com.hds.ensemble.sdk.model.StringDocumentFieldValue;
import com.hds.ensemble.sdk.plugin.PluginCallback;

public class WebCrawler {

	private int crawlDepth; // depth to crawl
	private List<Document> documentList; // container to hold all the documents
											// crawled.
	private PluginCallback callback; // handle to the plugin callback.
	private String url; // url to crawl
	private HashSet<String> crawledUrls; // container to hold a list of crawled
											// urls to avoid circular
											// dependencies.

	public WebCrawler(String inUrl, int inDepth, PluginCallback inCallback) {
		this.crawlDepth = inDepth;
		this.documentList = new ArrayList<Document>();
		this.callback = inCallback;
		this.url = inUrl;
		this.crawledUrls = new HashSet<>();
	}

	/**
	 * @param documentList
	 */
	public void setDocumentList(List<Document> documentList) {
		this.documentList = documentList;
	}

	/**
	 * @return documentList
	 */
	public List<Document> getDocumentList() {
		return documentList;
	}

	/**
	 * @return crawlDepth
	 */
	public int getCrawlDepth() {
		return crawlDepth;
	}

	/**
	 * @return Size
	 */
	public int getCrawledListSize() {
		return crawledUrls.size();
	}

	/**
	 * @return Connection
	 */
	public Connection getConnection() {
		return Jsoup.connect(this.getUrl());
	}

	/**
	 * @param i
	 */
	public void setCrawlDepth(int i) {
		this.crawlDepth = i;
	}

	/**
	 * @return
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @param inUrl
	 * @return a string representation of the content
	 * 
	 *         This method gets the content of a given url and returns the
	 *         content as a string.
	 */
	public String getContentAsString(String inUrl) {
		URL url;
		BufferedReader in = null;
		StringBuilder str = new StringBuilder();
		String line = null;
		try {
			url = new URL(inUrl);
			in = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));

			while ((line = in.readLine()) != null) {
				str.append(line);
			}
		} catch (Exception e) {
			// Eat the exception
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					// Eat the exception
				}
			}
		}

		return str.toString();
	}

	/**
	 * @param None
	 * @return None
	 * 
	 *         This method crawls starting from a base url upto a configured
	 *         depth and constructs a Document of basic HCI metadata.
	 * 
	 */
	public void crawl() {
		org.jsoup.nodes.Document doc;

		if ((!this.crawledUrls.contains(this.getUrl()) && (this.getCrawlDepth() > 0))) {
			try {
				doc = this.getConnection().get();

				Elements links = doc.select("a");
				String title = doc.title();

				// Maintain a list of crawled urls
				this.crawledUrls.add(this.getUrl());

				// container to hold all the metadata documents for crawled urls
				if (!title.isEmpty()) {
					documentList.add(getDocument(url.toString(), false, title));
				}

				for (Element link : links) {
					String absHref = link.attr("abs:href");
					if (!absHref.trim().isEmpty()) {

						this.setUrl(absHref);
						// crawl recursively
						this.crawl();
					}
				}
				this.setCrawlDepth(this.getCrawlDepth() - 1);
			} catch (UnsupportedMimeTypeException e) {
				// Eat the exception and move on
			} catch (IOException e) {
				// Eat the exception and move on
			}
		}
	}

	/**
	 * @param None
	 * @return None
	 * 
	 *         This method returns the HCI document metadata for a given url
	 * 
	 */
	public Document getDocumentMetadata(String inUrl) {
		try {
			org.jsoup.nodes.Document doc = this.getConnection().get();
			String title = doc.title();
			return getDocument(inUrl, false, title);
		} catch (IOException e) {
			return null;
		}

	}

	// Obtain an example document with the given name suffix
	private Document getDocument(String name, Boolean isContainer, String title) throws IOException {

		HashMap<String, String> contentStreamMetadata = new HashMap<>();

		// Optionally add metadata about this stream (e.g. it's size, etc.)
		contentStreamMetadata.put("HCI_crawlerMetadata", "default");

		DocumentBuilder builder = this.callback.documentBuilder();

		if (isContainer) {
			builder.setIsContainer(true).setHasContent(false);
		} else {
			builder.setStreamMetadata(StandardFields.CONTENT, contentStreamMetadata);
		}
		builder.addMetadata(StandardFields.ID, StringDocumentFieldValue.builder().setString(name).build());
		builder.addMetadata(StandardFields.URI, StringDocumentFieldValue.builder().setString(name).build());
		builder.addMetadata(StandardFields.DISPLAY_NAME, StringDocumentFieldValue.builder().setString(title).build());
		builder.addMetadata(StandardFields.VERSION, StringDocumentFieldValue.builder().setString("1").build());

		return builder.build();

	}

	// For example purposes, return a root Document
	public Document getRootDocument(String inName, String title) throws IOException {
		return getDocument(inName, true, title);
	}

}
