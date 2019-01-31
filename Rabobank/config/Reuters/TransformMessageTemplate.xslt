<?xml version="1.0" encoding="utf-8"?>

<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>

    <xsl:output method="xml" omit-xml-declaration="yes" indent="yes"/>
    <xsl:param name="hcpReference"/>
    <xsl:param name="referenceFilter"/>

    <!-- this template is applied by default to all nodes and attributes -->
    <xsl:template match="@*|node()">
        <!-- just copy all my attributes and child nodes, except if there's a better template for some of them -->
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="FileAttachment">
        <FileAttachment>
        <xsl:apply-templates select="@*|node()"/>
        </FileAttachment>
    </xsl:template>

    <!-- this template is applied to Message node to add new FullName field -->
    <xsl:template match="Message">
<xsl:text>
</xsl:text>
      <Message>
        <HCPReference>instant_messages/<xsl:value-of select="$hcpReference"/></HCPReference>
        <MessageID><xsl:value-of select="MessageID"/></MessageID>
        <User><xsl:value-of select="User"/></User>
        <xsl:apply-templates select="FileAttachment[Reference=$referenceFilter]"/>
      </Message>
    </xsl:template>
</xsl:stylesheet>
