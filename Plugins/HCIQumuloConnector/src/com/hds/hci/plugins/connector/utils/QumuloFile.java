/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.utils;

/***
 * 
 * @author kkancherla
 * Sample json
 *{
    "file_number": "2", 
    "major_minor_numbers": {
        "major": 0, 
        "minor": 0
    }, 
    "num_links": 6, 
    "creation_time": "2017-02-27T21:15:13.805254983Z", 
    "owner": "500", 
    "id": "2", 
    "size": "8192", 
    "group": "513", 
    "extended_attributes": {
        "read_only": false, 
        "temporary": false, 
        "system": false, 
        "compressed": false, 
        "not_content_indexed": false, 
        "hidden": false, 
        "archive": false
    }, 
    "owner_details": {
        "id_type": "LOCAL_USER", 
        "id_value": "admin"
    }, 
    "metablocks": "1", 
    "child_count": 16, 
    "group_details": {
        "id_type": "LOCAL_GROUP", 
        "id_value": "Users"
    }, 
    "type": "FS_FILE_TYPE_DIRECTORY", 
    "datablocks": "0", 
    "blocks": "1", 
    "modification_time": "2017-08-22T23:17:39.671470385Z", 
    "path": "/", 
    "name": "", 
    "change_time": "2017-08-22T23:17:39.671470385Z", 
    "mode": "0777", 
    "symlink_target_type": "FS_FILE_TYPE_UNKNOWN"
}
 */

public class QumuloFile {
	private String path;
    private String name;
    private int num_links;
    private String type;
    private String symlink_target_type;
    private String file_number;
    private QumuloMajorMinor major_minor_numbers;
    private String id;
    private String mode;
    private String owner;
    private QumuloOwnerDetails owner_details;
      
  
    private String group;
    private QumuloGroupDetails group_details;
      
  
    private String blocks;
    private String datablocks;
    private String metablocks;
    private String size;
    private String modification_time;
    private String change_time;
    private String creation_time;
    private int child_count;
    private QumuloExtendedAttributes extended_attributes;
    
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getNum_links() {
		return num_links;
	}
	public void setNum_links(int num_links) {
		this.num_links = num_links;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSymlink_target_type() {
		return symlink_target_type;
	}
	public void setSymlink_target_type(String symlink_target_type) {
		this.symlink_target_type = symlink_target_type;
	}
	public String getFile_number() {
		return file_number;
	}
	public void setFile_number(String file_number) {
		this.file_number = file_number;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public QumuloOwnerDetails getOwner_details() {
		return owner_details;
	}
	public void setOwner_details(QumuloOwnerDetails owner_details) {
		this.owner_details = owner_details;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public QumuloGroupDetails getGroup_details() {
		return group_details;
	}
	public void setGroup_details(QumuloGroupDetails group_details) {
		this.group_details = group_details;
	}
	public String getBlocks() {
		return blocks;
	}
	public void setBlocks(String blocks) {
		this.blocks = blocks;
	}
	public String getDatablocks() {
		return datablocks;
	}
	public void setDatablocks(String datablocks) {
		this.datablocks = datablocks;
	}
	public String getMetablocks() {
		return metablocks;
	}
	public void setMetablocks(String metablocks) {
		this.metablocks = metablocks;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getModification_time() {
		return modification_time;
	}
	public void setModification_time(String modification_time) {
		this.modification_time = modification_time;
	}
	public String getChange_time() {
		return change_time;
	}
	public void setChange_time(String change_time) {
		this.change_time = change_time;
	}
	public String getCreation_time() {
		return creation_time;
	}
	public void setCreation_time(String creation_time) {
		this.creation_time = creation_time;
	}
	public int getChild_count() {
		return child_count;
	}
	public void setChild_count(int child_count) {
		this.child_count = child_count;
	}
	public QumuloExtendedAttributes getExtended_attributes() {
		return extended_attributes;
	}
	public void setExtended_attributes(QumuloExtendedAttributes extended_attributes) {
		this.extended_attributes = extended_attributes;
	}
	/**
	 * @return the major_minor_numbers
	 */
	public QumuloMajorMinor getMajor_minor_numbers() {
		return major_minor_numbers;
	}
	/**
	 * @param major_minor_numbers the major_minor_numbers to set
	 */
	public void setMajor_minor_numbers(QumuloMajorMinor major_minor_numbers) {
		this.major_minor_numbers = major_minor_numbers;
	}
      
}
