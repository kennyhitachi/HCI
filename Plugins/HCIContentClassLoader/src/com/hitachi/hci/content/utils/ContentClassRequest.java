package com.hitachi.hci.content.utils;

import java.util.List;

public class ContentClassRequest {
   public String getModelVersion() {
		return modelVersion;
	}
	public void setModelVersion(String modelVersion) {
		this.modelVersion = modelVersion;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public List<ContentProperties> getContentProperties() {
		return contentProperties;
	}
	public void setContentProperties(List<ContentProperties> list) {
		this.contentProperties = list;
	}
public String modelVersion;
   public String name;
   public String description;
   public String uuid;
   public List<ContentProperties> contentProperties;
}
