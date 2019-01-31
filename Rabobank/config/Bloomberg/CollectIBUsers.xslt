<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output indent="yes" cdata-section-elements="preview" />
    <xsl:template match="/Conversation" >
      <Participants>
      <xsl:attribute name="count">
       <xsl:value-of select="count(distinct-values(*/User/EmailAddress | */Invitee/EmailAddress | */Inviter/EmailAddress))"/>
      </xsl:attribute>
      <xsl:for-each-group select="*/User | */Invitee | */Inviter" group-by="./EmailAddress">
          <Participant>
              <EmailAddress>
                <xsl:value-of select="EmailAddress"/>
              </EmailAddress>
              <CorporateEmailAddress>
                <xsl:value-of select="CorporateEmailAddress"/>
              </CorporateEmailAddress>
          </Participant>
      </xsl:for-each-group>
      </Participants>
    </xsl:template>
</xsl:stylesheet>
