import streamlit as st
import datareader as dr

def showpage():
    #TODO: Fix for multiple search terms, multiple authorities (probably in both rss-generator and this page.)
    st.title("RSS Feed")
    st.markdown("Use the parameters below to create an RSS-feed.")
    searchstring = st.text_input("Search string (Optional)")

    auths=dr.get_entities()
    selectauth = " [none] "
    auths.insert(0, selectauth)

    authority = st.selectbox("Mentioned authority (Optional)",auths) #har medvetet gjort så att koden kraschar om man väljer flera auths, det borde den väl klara?
    
    maxhits = st.slider(label="Max hits", step=20, min_value=1, max_value=500,value=(100))

    searchstring=searchstring.replace(' ',r'%20')

    link=f'https://rod4sai-rss.azurewebsites.net/api/rss?searchstring={searchstring}&authority={authority.replace(selectauth,"")}&maxhits={maxhits}'
    url_link=f'[rss_feed]({link})'
    st.markdown(url_link, unsafe_allow_html=True)
