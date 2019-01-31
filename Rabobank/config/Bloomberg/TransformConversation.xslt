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
    <xsl:template match="DateTime">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <DateTime dataType="date">
            <xsl:apply-templates select="@*|node()"/>
        </DateTime>
    </xsl:template>

    <!-- this template is applied to DateTimeUTC node to add dataType -->
    <xsl:template match="DateTimeUTC">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <DateTimeUTC dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </DateTimeUTC>
    </xsl:template>

    <!-- this template is applied to StartDateTime node to add dataType -->
    <xsl:template match="StartTime">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <StartTime dataType="date">
            <xsl:apply-templates select="@*|node()"/>
        </StartTime>
    </xsl:template>

    <!-- this template is applied to StartDateTimeUTC node to add dataType -->
    <xsl:template match="StartTimeUTC">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <StartTimeUTC dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </StartTimeUTC>
    </xsl:template>

    <!-- this template is applied to EndDateTime node to add dataType -->
    <xsl:template match="EndTime">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <EndTime dataType="date">
            <xsl:apply-templates select="@*|node()"/>
        </EndTime>
    </xsl:template>

    <!-- this template is applied to EndDateTimeUTC node to add dataType -->
    <xsl:template match="EndTimeUTC">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <EndTimeUTC dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </EndTimeUTC>
    </xsl:template>

    <!-- this template is applied to FileSize node to add dataType -->
    <xsl:template match="FileSize">
        <!-- copy me and my attributes and my subnodes, applying templates as necessary, and add a dataType attribute -->
        <FileSize dataType="number">
            <xsl:apply-templates select="@*|node()"/>
        </FileSize>
    </xsl:template>

    <!-- this template is applied to User node to add new FullName field -->
    <xsl:template match="User">
        <User><xsl:text>
      </xsl:text>
	<FullName><xsl:value-of select="concat(FirstName, ' ', LastName)" /></FullName>
        <xsl:apply-templates select="@*|node()"/>
        <xsl:text>    </xsl:text>
        </User>
    </xsl:template>

    <!-- this template is applied to Inviter node to add new FullName field -->
    <xsl:template match="Inviter">
        <Inviter><xsl:text>
      </xsl:text>
	<FullName><xsl:value-of select="concat(FirstName, ' ', LastName)" /></FullName>
        <xsl:apply-templates select="@*|node()"/>
        <xsl:text>    </xsl:text>
	</Inviter>
    </xsl:template>

    <!-- this template is applied to Invitee node to add new FullName field -->
    <xsl:template match="Invitee">
        <Invitee><xsl:text>
      </xsl:text>
	<FullName><xsl:value-of select="concat(FirstName, ' ', LastName)" /></FullName>
        <xsl:apply-templates select="@*|node()"/>
        <xsl:text>    </xsl:text>
	</Invitee>
    </xsl:template>
</xsl:stylesheet>
