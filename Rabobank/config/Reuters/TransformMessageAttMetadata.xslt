<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" version="2.0" exclude-result-prefixes="xs">
<xsl:output method="xml" omit-xml-declaration="yes" indent="yes"/>
   <xsl:param name="fileSize"/>
   <xsl:template match="/">
            <Attachment>
              <HCPReference>attachments/<xsl:value-of select="Message/FileAttachment/Reference"/></HCPReference>
              <FileName><xsl:value-of select="Message/FileAttachment/Name"/></FileName>
              <FileSize dataType="number"><xsl:value-of select="$fileSize"/></FileSize>
            </Attachment>
   </xsl:template>
</xsl:stylesheet>

