import pandas as pd
import datareader as dr
import streamlit as st
import plotly.express as px
from datetime import datetime

def showpage():
    st.title("Compound sentiment distribution by speech mentioning any listed authority")
    st.markdown("The compound sentiment measure")
    st.markdown("-1=most negative, +1=most positive, 0=most neutral")

    st.sidebar.markdown("--------")
    maxy=datetime.today().year
    yearspan = st.sidebar.slider(label="Range (year)", min_value=2003, max_value=maxy,value=(maxy-3,maxy))
    authorities = st.sidebar.multiselect("Select authorities",dr.get_entities(),default=['Arbetsförmedlingen','Riksrevisionen','Statskontoret','Försäkringskassan','Skatteverket','Tillväxtanalys'])

    start = datetime(yearspan[0],1,1)
    end = datetime(yearspan[1],12,31)

    projection= {'authorities': 1, 'sent_com': 1,'sent_neg': 1,'sent_pos': 1, 'applause':1,'avsnittsrubrik':1,'underrubrik':1,'topic':1,'talare':1,'anforande_nummer':1}
    partylist = ["S","M", "SD", "L","C","KD","V","MP"]

    criteria={"dok_datum": {'$lt': end, '$gte': start},
            'authorities':{'$in':authorities }
    }

    pipeline=[
        {'$match': criteria },
        {'$unwind':'$authorities'},
        {'$match': criteria },
        {'$project':projection}
    ]


    df=pd.DataFrame(list(dr.mongo_collection().aggregate(pipeline))).reset_index().drop('_id', axis=1)
    df.rename(columns={'avsnittsrubrik':'Title', 'talare':'Speaker','topic':'Topic','anforande_nummer':'Speech number'}, inplace=True)


    how={'sent_com':False,
        'authorities':False,
        'Title':True,
        'Speaker':True,
        'Topic':True,
        'Speech number':True}

    fig = px.box(df, x="sent_com", y="authorities", hover_data=how)
    fig.update_traces(quartilemethod="exclusive") # or "inclusive", or "linear" by default
    fig.update_layout(yaxis_title='Authoritites', xaxis_title='Speech sentiment',hoverlabel=dict(
            bgcolor="white", 
            font_size=12, 
            font_family="Rockwell"
        ))

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("__Note:__ Compound sentiment refers to the speech itself, not necessary the authority.  Sentiment computed by VADER sentiment tool.")
    st.markdown("Article describing the elemets of a box plot: https://towardsdatascience.com/understanding-boxplots-5e2df7bcbd51")
    st.markdown("__Source:__ Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.")