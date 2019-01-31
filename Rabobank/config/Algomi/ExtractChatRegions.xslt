<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='2.0'>

    <!-- this template is applied to DateTime node to add dataType -->
    <xsl:template match="/ChatRecords/ChatRecord">
        <ChatRecord>
          <xsl:for-each select="ChatMembers/ChatMember/countryCode">
              <countryCode>
                 <xsl:value-of select="." />
              </countryCode>
          </xsl:for-each>
        </ChatRecord>
    </xsl:template>

</xsl:stylesheet>
