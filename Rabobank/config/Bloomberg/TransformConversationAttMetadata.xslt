<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" version="2.0" exclude-result-prefixes="xs">
<xsl:output method="xml" omit-xml-declaration="yes" indent="yes"/>

   <xsl:template match="@* | node()">
     <xsl:apply-templates/>
   </xsl:template>

   <xsl:template match="/Conversation">
     <Attachments>
     <xsl:apply-templates/>
     </Attachments>
   </xsl:template>

   <xsl:template match="Attachment">
      <Attachment>
      <HCPReference>attachments/<xsl:value-of select="FileID"/></HCPReference>
      <xsl:choose>
        <xsl:when test="FileName">
          <FileName><xsl:value-of select="FileName"/></FileName>
        </xsl:when>
        <xsl:otherwise>
          <FileName><xsl:value-of select="Reference"/></FileName>
        </xsl:otherwise>
      </xsl:choose>
      <FileSize dataType="number"><xsl:value-of select="FileSize"/></FileSize>
      </Attachment>
   </xsl:template>
</xsl:stylesheet>

