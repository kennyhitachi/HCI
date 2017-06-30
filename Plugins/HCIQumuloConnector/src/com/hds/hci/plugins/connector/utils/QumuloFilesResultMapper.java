/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.utils;

import java.util.List;

public class QumuloFilesResultMapper {
	
	private String path;
	private String id;
	private int child_count;
	private List<QumuloFile>  files;
	private QumuloPaging paging;
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getChild_count() {
		return child_count;
	}
	public void setChild_count(int child_count) {
		this.child_count = child_count;
	}
	public List<QumuloFile> getFiles() {
		return files;
	}
	public void setFiles(List<QumuloFile> files) {
		this.files = files;
	}
	/**
	 * @return the paging
	 */
	public QumuloPaging getPaging() {
		return paging;
	}
	/**
	 * @param paging the paging to set
	 */
	public void setPaging(QumuloPaging paging) {
		this.paging = paging;
	}
}
