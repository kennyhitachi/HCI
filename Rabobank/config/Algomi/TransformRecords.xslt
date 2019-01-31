<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>

    <!-- this template is applied by default to all nodes and attributes -->
    <xsl:template match="@*|node()">
        <!-- just copy all my attributes and child nodes, except if there's a better template for some of them -->
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- this template is applied to DateTime node to add dataType -->
    <xsl:template match="createdAt">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <createdAt dataType="date">
            <xsl:apply-templates select="@*|node()"/>
        </createdAt>
    </xsl:template>

    <!-- this template is applied to DateTimeUTC node to add dataType -->
    <xsl:template match="quantity">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <quantity dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </quantity>
    </xsl:template>

</xsl:stylesheet>
