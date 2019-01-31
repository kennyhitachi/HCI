<xsl:stylesheet version="1.0"
       xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="@* | node()">
      <xsl:choose>
        <xsl:when test="Key">
          <xsl:choose>
             <xsl:when test="./Key = 'CD1'">
	          <xsl:element name="DictionaryEntry_RecorderReference">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:when>
             <xsl:when test="./Key = 'CD2'">
	          <xsl:element name="DictionaryEntry_TraderIdentifier">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:when>
             <xsl:when test="./Key = 'CD3'">
	          <xsl:element name="DictionaryEntry_TraderName">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:when>
             <xsl:when test="./Key = 'CD4'">
	          <xsl:element name="DictionaryEntry_DeviceIdentifier">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:when>
             <xsl:when test="./Key = 'CD5'">
	          <xsl:element name="DictionaryEntry_DeviceType">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:when>
             <xsl:otherwise>
	          <xsl:element name="DictionaryEntry_{translate(./Key, ' ', '')}">
	             <xsl:value-of select="./Value"/>
	          </xsl:element>
             </xsl:otherwise>
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>
          <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
          </xsl:copy>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
