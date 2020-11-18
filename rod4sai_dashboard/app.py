import streamlit as st
import freetextpage
#import applausepage_plotly as applausepage
import applausepage
import mentionspage
import sentimentpage
import text_trends
#import compound_hist
import compound_hist_plotly as compound_hist
import topicspage
import wordcloudpage
import rsspage
import pymongo
from datareader import  mongo_count, mongo_collection, mongo_find, get_wc_from_mongo

def load_homepage():
    st.title('ROD4SAI')
    for i in range(3):
        st.write(" ")
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/d/d0/Riksdag.svg/800px-Riksdag.svg.png",use_column_width=True)
    st.markdown("> A Dashboard for eavesdropping on the Swedish Parliament")
    st.markdown("...with data from <a href='https://data.riksdagen.se/' target='_blank'>Riksdagens öppna data.</a>", unsafe_allow_html=True)
    st.markdown("<div align='center'><br>"
                "<img src='https://img.shields.io/badge/MADE%20WITH-PYTHON%20-red?style=for-the-badge'"
                "alt='API stability' height='25'/>"
                "<img src='https://img.shields.io/badge/DASHBOARD%20BY-Streamlit-turquoise?style=for-the-badge'"
                "alt='API stability' height='25'/>"
                "<img src='https://img.shields.io/badge/DATA%20FROM-Riksdagens Öppna data%20-yellow?style=for-the-badge'"
                "alt='API stability' height='25'/>"
                "<img src='https://img.shields.io/badge/STORED%20IN-Mongo Atlas-brightgreen?style=for-the-badge'"
                "alt='API stability' height='25'/>"
                "<img src='https://img.shields.io/badge/HOSTED%20IN-Azure-blue?style=for-the-badge'"
                "alt='API stability' height='25'/>"
                "</div>", unsafe_allow_html=True)

    for i in range(3):
        st.write(" ")
    st.header("The Application")
    st.write("This application is a Streamlit dashboard that can be used to explore "
             "the speeches of the chamber of the Swedish Parliament.")
    st.markdown('___')             
    st.write(f"Currently, there is {mongo_count(filter={}):,.0f} speeches distributed on {len(mongo_collection().distinct('dok_id')):,.0f} debates in this database.")

    latest=mongo_find(sort=[("dok_datum", pymongo.DESCENDING),("anforande_nummer",pymongo.ASCENDING)],filter={},limit=1)
    st.write(f"__An example:__ The first speech from the latest debate held on {latest.loc[0,'dok_datum']:%Y-%m-%d}")

    # Beware! This code chunk only works during the period where paragraphs are denoted by html-tags: <p> and </p>
    latest.loc[0,'First paragraph']=latest.loc[0,'anforandetext'].split('</p>')[0].replace('<p>','').replace('<em>','').replace('</em>','')
    latest.loc[0,'Second paragraph']=latest.loc[0,'anforandetext'].split('</p>')[1].replace('<p>','').replace('<em>','').replace('</em>','')
    latest['dok_datum']=latest['dok_datum'].dt.strftime("%Y-%m-%d") # Format date output

    st.table(latest[['talare','dok_datum','avsnittsrubrik','topic','First paragraph','Second paragraph']].set_index('talare').rename(columns={'dok_datum':'Date', 'avsnittsrubrik':'Title','topic':'Data generated topic'}).transpose() )

    st.header("Wordcloud from the 1000 latest speeches")
    st.image(get_wc_from_mongo(),use_column_width=True)

def create_layout():
    "Hide hamburger menu"
    hide_menu_style = "<style>#MainMenu {visibility: hidden;}</style>"
    st.markdown(hide_menu_style, unsafe_allow_html=True)

    st.sidebar.title("Menu")
    app_mode = st.sidebar.selectbox("Please select a page", ["Homepage","Freetext search","Plot text trends","Topics","Applause","Mentions","Sentiment","Compound sentiment distribution","WordCloud","RSS Feed"]) 
    if app_mode == 'Homepage':
        load_homepage()
    elif app_mode == "Freetext search":
        freetextpage.showpage()
    elif app_mode == "Plot text trends":
        text_trends.showpage()    
    elif app_mode == "Topics":
        topicspage.showpage()        
    elif app_mode == "Applause":
        applausepage.showpage()
    elif app_mode == "Mentions":
        mentionspage.showpage()
    elif app_mode == "Sentiment":
        sentimentpage.showpage()
    elif app_mode == "Compound sentiment distribution":
        compound_hist.showpage()
    elif app_mode == "WordCloud":
        wordcloudpage.showpage()
    elif app_mode == "RSS Feed":
        rsspage.showpage()


def main():
    create_layout()

if __name__ == "__main__":
    main()
