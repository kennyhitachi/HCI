/*
 * ========================================================================
 *
 * Copyright (c) by Hitachi Data Systems, 2017. All rights reserved.
 *
 * ========================================================================
 */

package com.hds.hci.plugins.connector.utils;

public class QumuloExtendedAttributes {
	
	private boolean read_only;
	private boolean hidden;
	private boolean system;
	private boolean archive;
	private boolean temporary;
	private boolean compressed;
	private boolean not_content_indexed;
	
	public boolean isRead_only() {
		return read_only;
	}
	public void setRead_only(boolean read_only) {
		this.read_only = read_only;
	}
	public boolean isHidden() {
		return hidden;
	}
	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}
	public boolean isSystem() {
		return system;
	}
	public void setSystem(boolean system) {
		this.system = system;
	}
	public boolean isArchive() {
		return archive;
	}
	public void setArchive(boolean archive) {
		this.archive = archive;
	}
	public boolean isTemporary() {
		return temporary;
	}
	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}
	public boolean isCompressed() {
		return compressed;
	}
	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}
	public boolean isNot_content_indexed() {
		return not_content_indexed;
	}
	public void setNot_content_indexed(boolean not_content_indexed) {
		this.not_content_indexed = not_content_indexed;
	}
}
