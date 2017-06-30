/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.utils;

public class QumuloGroupDetails {
	
	private String id_type;
    private String id_value;
    
	public String getId_type() {
		return id_type;
	}
	public void setId_type(String id_type) {
		this.id_type = id_type;
	}
	public String getId_value() {
		return id_value;
	}
	public void setId_value(String id_value) {
		this.id_value = id_value;
	}
}
