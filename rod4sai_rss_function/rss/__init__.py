import logging
import os
import re
from datetime import datetime, timezone
import pymongo
from feedgen.feed import FeedGenerator
import azure.functions as func
import xml.dom.minidom

def xmlPretty(string):
    #string=string.encode("utf-8").decode('utf-8','strict')
    xml_s = xml.dom.minidom.parseString(string)
    xml_pretty_str = xml_s.toprettyxml()
    return xml_pretty_str


def get_connstring():
    return os.getenv('CONNSTRING_MONGO', "mongodb://localhost:27017/")

def mongo_collection(col="anforanden"):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb[col]
    return mycol

def formatera_text_n(txt):
    """ Fixa texten till mer läsbart format men byter p-taggar till radbryt och behåller andra radbryt"""
    if txt is not None:
        txt=txt.replace('</p>','\n').replace('<p>','').replace('</em>','').replace('<em>','').replace('STYLEREF Kantrubrik \* MERGEFORMAT','').replace('-\n','')\
            .replace("&", "&amp;").replace("<","&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;")
    return txt.encode("utf-8").decode('utf-8','strict')

def get_cursor(search_string, authority, maxhits):
    # Fetch search string for abreviated authorities with complex search strings
    if authority:
        if len(authority)<5:
            authority=authority.upper()
            auth=mongo_collection('authorities').find({'Entitet':authority})
            authority_search=auth[0]['Söksträng']
        else:
            authority=authority.title()
            auth=mongo_collection('authorities').find({'Entitet':authority})
            authority_search=auth[0]['Söksträng']
    else:
        authority_search=''

    # Depending on parameter input in the function this if-statement chooses how the Mongo query should be specified
    if search_string:
        search_string = search_string.replace("%20"," ")
        if len(search_string) > 3:
            if not authority:
                        filter=   {
                        "$text" : { "$search": search_string }
                    }
                    
            else:
                filter=   {
                            "$text" : { "$search": search_string },
                            "authorities": authority
                        }
        else:
            if not authority:
                        filter=   {}
            else:
                filter=   {
                            "authorities": authority
                        }
    else:
        if not authority:
                    filter=   {}
        else:
            filter=   {
                        "authorities": authority
                    }

    # Pass the query to Mongo
    cursor=mongo_collection().find(sort=[("dok_datum", pymongo.DESCENDING),("anforande_nummer",pymongo.DESCENDING)],limit=maxhits, filter=filter)

    return cursor, authority_search


# Start looping in items to the rss 
def rss_feedgen(search_string, authority, maxhits):
    cursor, authority_search = get_cursor(search_string, authority, maxhits)
    # Create the rss template
    rss_head="""<?xml version="1.0" encoding="UTF-8"?><rss xmlns:atom="http://www.w3.org/2005/Atom" version="2.0"><channel><title>RiR Datalab RSS feed</title><link>http://rod4sai.azurewebsites.net/</link><description>Anföranden i Riksdagen</description><language>sv</language><copyright>RiR Datalab 2020</copyright><atom:link href="http://rod4sai.azurewebsites.net" rel="self" type="application/rss+xml"/>"""

    reg_fallback=re.compile(f'((?<=<p>)(?:(?!<\/p>).)*.*(?:(?!<\/p>).)*(?=<\/p>))')
    reg_str=re.compile(f'((?<=<p>)(?:(?!<\/p>).)*{authority_search}(?:(?!<\/p>).)*(?=<\/p>))')
    item=''
    for row in cursor:
        if authority:
            try:
                summary = reg_str.search(row['anforandetext']).group(1)
            except:  
                if len(row['anforandetext'])>400:
                    summary = f"{row['anforandetext'][:400]}..."
                else:
                    summary = row['anforandetext']
        else:
            try:
                summary = reg_fallback.search(row['anforandetext']).group(1)
            except Exception as ex:
                if len(row['anforandetext'])>400:
                    summary = f"{row['anforandetext'][:400]}..."
                else:
                    summary = row['anforandetext']

        link=f"<link>{row['protokoll_url_www']}</link>"
        title=f"<title>{row['avsnittsrubrik']}</title>"
        description=f"<description>{formatera_text_n(summary)}</description>"
        date=row['dok_datum'].replace(tzinfo=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
        pubDate=f"<pubDate>{date}</pubDate>"
        guid=f"""<guid isPermaLink="false">{row['anforande_url_html']} </guid>"""
        category=f"<category>{row['topic']}</category>"
        source_url=f"""<source url="{row['protokoll_url_www']}"> {row['talare']} </source>"""

        item+=f"<item>{link}{title}{description}{pubDate}{guid}{category}{source_url}</item>"
    return rss_head+item+"</channel></rss>"



def main(req: func.HttpRequest) -> func.HttpResponse:
    #logging.info('Python HTTP trigger function processed a request.')

    searchstring = req.params.get("searchstring")
    authority = req.params.get("authority")
    maxhits = req.params.get("maxhits")

    if not searchstring:
        searchstring=""

    if maxhits:
        maxhits=int(maxhits)
    else:
        maxhits=25
    try:
        rss_string = rss_feedgen(searchstring, authority, maxhits)
        pass
    except Exception as ex:
        return func.HttpResponse(ex,status_code=200)

    return func.HttpResponse(rss_string,status_code=200)