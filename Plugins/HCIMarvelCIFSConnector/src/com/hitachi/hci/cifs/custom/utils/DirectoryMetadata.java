package com.hitachi.hci.cifs.custom.utils;

//FRANCHISE	ITEM NUMBER	ITEM_DESCRIPTION	IMPRINT	MAIN_TITLE	NAMING_CONVENTION	ITEM_TYPE	EDITOR_NAME	DIRECT_ONSALE_DATE	ISBN	eBook ISBN-13	UPC	EAN

public class DirectoryMetadata {
	public DirectoryMetadata(String franchise, String itemNumber, String itemDescription, String imprint,
			String mainTitle, String namingConvention, String itemType, String editorName, String directOnsaleDate,
			String isbn, String ebookIsbn13, String upc, String ean) {
		super();
		this.franchise = franchise;
		this.itemNumber = itemNumber;
		this.itemDescription = itemDescription;
		this.imprint = imprint;
		this.mainTitle = mainTitle;
		this.namingConvention = namingConvention;
		this.itemType = itemType;
		this.editorName = editorName;
		this.directOnsaleDate = directOnsaleDate;
		this.isbn = isbn;
		this.ebookIsbn = ebookIsbn13;
		//this.isbn13 = isbn13;
		this.upc = upc;
		this.ean = ean;
	}
	private String franchise;
	private String itemNumber;
	private String itemDescription;
	private String imprint;
	private String mainTitle;
	private String namingConvention;
	private String itemType;
	private String editorName;
	private String directOnsaleDate;
	private String isbn;
	private String ebookIsbn;
	private String upc;
	private String ean;
	public String getFranchise() {
		return franchise;
	}
	public void setFranchise(String franchise) {
		this.franchise = franchise;
	}
	public String getItemNumber() {
		return itemNumber;
	}
	public void setItemNumber(String itemNumber) {
		this.itemNumber = itemNumber;
	}
	public String getItemDescription() {
		return itemDescription;
	}
	public void setItemDescription(String itemDescription) {
		this.itemDescription = itemDescription;
	}
	public String getImprint() {
		return imprint;
	}
	public void setImprint(String imprint) {
		this.imprint = imprint;
	}
	public String getMainTitle() {
		return mainTitle;
	}
	public void setMainTitle(String mainTitle) {
		this.mainTitle = mainTitle;
	}
	public String getNamingConvention() {
		return namingConvention;
	}
	public void setNamingConvention(String namingConvention) {
		this.namingConvention = namingConvention;
	}
	public String getItemType() {
		return itemType;
	}
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	public String getEditorName() {
		return editorName;
	}
	public void setEditorName(String editorName) {
		this.editorName = editorName;
	}
	public String getDirectOnsaleDate() {
		return directOnsaleDate;
	}
	public void setDirectOnsaleDate(String directOnsaleDate) {
		this.directOnsaleDate = directOnsaleDate;
	}
	public String getIsbn() {
		return isbn;
	}
	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}
	
	public String getEbookIsbn() {
		return ebookIsbn;
	}
	public void setEbookIsbn(String ebookIsbn) {
		this.ebookIsbn = ebookIsbn;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getEan() {
		return ean;
	}
	public void setEan(String ean) {
		this.ean = ean;
	}

}
