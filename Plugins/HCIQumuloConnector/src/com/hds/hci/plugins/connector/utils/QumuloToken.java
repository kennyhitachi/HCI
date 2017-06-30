/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.utils;


public class QumuloToken {
	
	private String key_id;
	private String key;
	private String algorithm;
	private String bearer_token;
	/**
	 * @return the algorithm
	 */
	public String getAlgorithm() {
		return algorithm;
	}
	/**
	 * @param algorithm the algorithm to set
	 */
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	/**
	 * @param key the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}
	/**
	 * @return the bearer_token
	 */
	public String getBearer_token() {
		return bearer_token;
	}
	/**
	 * @param bearer_token the bearer_token to set
	 */
	public void setBearer_token(String bearer_token) {
		this.bearer_token = bearer_token;
	}
	/**
	 * @return the key_id
	 */
	public String getKey_id() {
		return key_id;
	}
	/**
	 * @param key_id the key_id to set
	 */
	public void setKey_id(String key_id) {
		this.key_id = key_id;
	}
	
	
	

}
