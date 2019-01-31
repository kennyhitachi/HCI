<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>

    <!-- this template is applied by default to all nodes and attributes -->
    <xsl:template match="@*|node()">
        <!-- just copy all my attributes and child nodes, except if there's a better template for some of them -->
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- this template is applied to MsgTime node to add dataType -->
    <xsl:template match="MsgTime">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <MsgTime dataType="date">
            <xsl:apply-templates select="@*|node()"/>
        </MsgTime>
    </xsl:template>

    <!-- this template is applied to MsgTimeUTC node to add dataType -->
    <xsl:template match="MsgTimeUTC">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <MsgTimeUTC dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </MsgTimeUTC>
    </xsl:template>

    <!-- this template is applied to FileSize node to add dataType -->
    <xsl:template match="FileSize">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <FileSize dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </FileSize>
    </xsl:template>

    <!-- this template is applied to User node to add new FullName field -->
    <xsl:template match="UserInfo">
        <UserInfo><xsl:text>
      </xsl:text>
        <FullName><xsl:value-of select="concat(FirstName, ' ', LastName)" /></FullName>
        <xsl:apply-templates select="@*|node()"/>
        <xsl:text>    </xsl:text></UserInfo>
    </xsl:template>
</xsl:stylesheet>
