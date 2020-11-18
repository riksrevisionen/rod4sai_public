import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import altair as alt
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from rod_utils import agg_to_df

import streamlit as st
import datareader as dr

def showpage():
    st.title("Compound sentiment distribution")
    st.markdown("The compound sentiment measure")
    st.markdown("-1=most negative, +1=most positive, 0=most neutral")

    st.sidebar.markdown("--------")
 
    yearspan = st.sidebar.slider(label="Range (year)", min_value=2003, max_value=2020,value=(2017,2020))
    authorities = st.sidebar.multiselect("Select authorities",dr.get_entities(),default=['Arbetsförmedlingen','Riksrevisionen','Statskontoret','Försäkringskassan','Skatteverket','Tillväxtanalys'])

    start = datetime(yearspan[0],1,1)
    end = datetime(yearspan[1],12,31)

    projection= {'authorities': 1, 'sent_com': 1,'sent_neg': 1,'sent_pos': 1, 'applause':1}
    partylist = ["S","M", "SD", "L","C","KD","V","MP"]

    criteria=   {
            "dok_datum": {'$lt': end, '$gte': start},
            'authorities':{'$in':authorities }
    }

    pipeline=[
        {'$match': criteria },
        {'$unwind':'$authorities'},
        {'$match': criteria },
        {'$project':projection}
    ]


    df=pd.DataFrame(list(dr.mongo_collection().aggregate(pipeline))).reset_index().drop('_id', axis=1)
    df['mean_label']='Mean of sentiment'
    box_plot=alt.Chart(df).mark_boxplot(color='burlywood').encode(
    y='authorities:N',
    x=alt.X('sent_com:Q')
    ).properties(height=400)

    circle=alt.Chart(df).mark_circle().encode(
    y='authorities:N',
    x=alt.X('average(sent_com):Q'),
    color=alt.Color('mean_label:N', title=''),
    size='mean(applause):Q'
    )
    st.altair_chart(box_plot + circle, use_container_width=True)    