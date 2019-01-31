<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" version="2.0" exclude-result-prefixes="xs">
   <xsl:template match="/">
      <ChatRecord>
         <chatType>
            <xsl:value-of select="ChatRecord/chatType" />
         </chatType>
         <orderId>
            <xsl:value-of select="ChatRecord/orderId" />
         </orderId>
         <isinCode>
            <xsl:value-of select="ChatRecord/isin/code" />
         </isinCode>
         <isinDescription>
            <xsl:value-of select="ChatRecord/isin/description" />
         </isinDescription>
         <clientName>
            <xsl:value-of select="ChatRecord/client/clientName" />
         </clientName>
         <clientId>
            <xsl:value-of select="ChatRecord/client/clientId" />
         </clientId>
         <quantity>
            <xsl:value-of select="ChatRecord/quantity" />
         </quantity>
         <ChatMembers>
            <xsl:attribute name="count">
               <xsl:value-of select="count(//ChatMember)" />
            </xsl:attribute>
            <xsl:for-each select="ChatRecord/ChatMembers/ChatMember">
               <userId>
                  <xsl:value-of select="userId" />
               </userId>
            </xsl:for-each>
         </ChatMembers>
         <ChatMessages>
            <xsl:attribute name="count">
               <xsl:value-of select="count(//ChatMessage)" />
            </xsl:attribute>
            <startDateTime>
               <xsl:value-of select="min(//createdAt/xs:dateTime(.))" />
            </startDateTime>
            <endDateTime>
               <xsl:value-of select="max(//createdAt/xs:dateTime(.))" />
            </endDateTime>
         </ChatMessages>
      </ChatRecord>
   </xsl:template>
</xsl:stylesheet>
