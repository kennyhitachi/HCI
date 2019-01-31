<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output indent="yes" cdata-section-elements="preview" />
    <xsl:template match="/Message" >
      <Participants>
      <xsl:attribute name="count">
       <xsl:value-of select="count(distinct-values(*/UserInfo/BloombergEmailAddress))"/>
      </xsl:attribute>
      <xsl:for-each-group select="*/UserInfo" group-by="./BloombergEmailAddress">
          <Participant>
              <BloombergEmailAddress>
                <xsl:value-of select="BloombergEmailAddress"/>
              </BloombergEmailAddress>
              <CorporateEmailAddress>
                <xsl:value-of select="CorporateEmailAddress"/>
              </CorporateEmailAddress>
          </Participant>
      </xsl:for-each-group>
      </Participants>
    </xsl:template>
</xsl:stylesheet>
