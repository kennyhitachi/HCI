<xsl:stylesheet version="1.0"
       xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="@* | node()">
      <xsl:choose>
        <xsl:when test="Key">
          <xsl:element name="DictionaryEntry_{translate(./Key, ' ', '')}">
             <xsl:value-of select="./Value"/>
          </xsl:element>
        </xsl:when>
        <xsl:otherwise>
          <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
          </xsl:copy>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
