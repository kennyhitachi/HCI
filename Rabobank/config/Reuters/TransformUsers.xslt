<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>
<xsl:output method="xml" indent="yes" omit-xml-declaration="yes"/>

    <!-- this template is applied by default to all nodes and attributes -->
    <xsl:template match="@*|node()">
        <xsl:apply-templates select="@*|node()"/>
    </xsl:template>

    <xsl:template match="Users">
        <xsl:element name="Users">
        <xsl:apply-templates select="@*|node()"/>
        </xsl:element>
    </xsl:template>

    <!-- this template is applied to UserInfo node to add new FullName field -->
    <xsl:template match="UserInfo">
        <xsl:element name="UserInfo">
          <xsl:element name="FullName">
	    <xsl:value-of select="concat(FirstName, ' ', LastName)" />
          </xsl:element>
          <xsl:element name="Identifier">
	    <xsl:value-of select="Identifier"/>
          </xsl:element>
          <xsl:element name="Email">
	    <xsl:value-of select="Email"/>
          </xsl:element>
          <xsl:element name="FirstName">
	    <xsl:value-of select="FirstName"/>
          </xsl:element>
          <xsl:element name="LastName">
	    <xsl:value-of select="LastName"/>
          </xsl:element>
          <xsl:element name="SiteLocation">
	      <xsl:value-of select="SiteLocation/Name"/>
          </xsl:element>
        </xsl:element>
    </xsl:template>
</xsl:stylesheet>
