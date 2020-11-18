import datareader as dr
import streamlit as st
import plotly.express as px
from rod_utils import tabstat_2way

def showpage():
    st.title("Applause per speech")

    criteria={
                "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }
            }

    df=tabstat_2way(criteria,'dok_rm','parti','avg','applause')

    pipeline=[
        {'$match':criteria},
        { "$sort": { "applause": -1, "dok_datum":-1, 'anforande_nummer': -1 } },
    { 
        "$group": {
            "_id": {'dok_rm':"$dok_rm",'parti':'$parti'},
            "max_applause": { "$first": "$applause" },
            "Speaker": { "$first": "$talare" },
            "Title": { "$first": "$avsnittsrubrik" },
            "Subtitle": { "$first": "$underrubrik" },
            "dok_datum": { "$first": "$dok_datum" },
            "Topic": { "$first": "$topic" },
            "Speech_html": {'$first':'$anforande_url_html'},
            "Sentiment": {'$first':'$sent_com'}
        }
    }
    ]

    df_meta=dr.mongo_aggregate(pipeline,allowDiskUse=True)

    df['rm_slider']=df['dok_rm'].str.slice(0,4).astype(int)
    df_meta['rm_slider']=df_meta['dok_rm'].str.slice(0,4).astype(int)

    st.sidebar.markdown("--------")

    party_colors={'V':'#ff9896','S':'#d62728','M':'#1f77b4','C':'#2ca02c','L':'#aec7e8','MP':'#98df8a','KD':'#9467bd','SD':'#ffd92f'}
    line = px.line(df.sort_values('rm_slider'), x="dok_rm", y="avg_applause", color='parti',color_discrete_map=party_colors, 
    hover_data={'parti':False,'avg_applause':':.2f'})
    line.update_layout(title='Average number of applause per speech', yaxis_title='Applause', xaxis_title='Parliament year',legend_title_text='Party')

    how={'dok_rm':False,
    'Speaker':True,
    'Date':df_meta.dok_datum.astype(str),
    'Title':True,
    'Topic':True,
    'Sentiment':True,
    }
    bar = px.bar(df_meta, x='dok_rm', y='max_applause', color='parti',color_discrete_map=party_colors, hover_data=how)

    bar.update_layout(title='Maximum number of applause in a speech', yaxis_title='No. Applause', xaxis_title='Parliament year',
                    barmode='stack', xaxis={'categoryorder':'category ascending'}, legend_title_text='Party',
                    hoverlabel=dict(
            bgcolor="white", 
            font_size=12, 
            font_family="Rockwell"
        ))

    st.plotly_chart(line)
    st.plotly_chart(bar)