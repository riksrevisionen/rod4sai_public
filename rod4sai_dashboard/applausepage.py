import datareader as dr
from rod_utils import tabstat_2way
import streamlit as st
import altair as alt


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
    y_min=int(df['rm_slider'].min())
    y_max=int(df['rm_slider'].max())
    yearspan = st.sidebar.slider(label="Range (start year of Parliament year)", min_value=y_min, max_value=y_max,value=(y_min,y_max))
    start = yearspan[0]
    end = yearspan[1]    
    rm_filter=(df.rm_slider <= end) & (df.rm_slider >= start)
    rm_filter_meta=(df_meta.rm_slider <= end) & (df_meta.rm_slider >= start)


    # Altair graph:
    #     
    # nearest for multi-line tooltip
    nearest = alt.selection(type='single', nearest=True, on='mouseover',
                            fields=['dok_rm'], empty='none')

    # Control party colors!
    domain = ["M","L", "SD", "S","C","KD","V","MP"]
    range_ = ['#1f77b4', '#aec7e8', '#ffd92f','#d62728', '#98df8a', '#9467bd','#ff9896','#2ca02c']
    party_colors = alt.Scale(domain=domain, range=range_)

    line = alt.Chart(df[rm_filter]).mark_line().encode(
        x='dok_rm:N',
        y=alt.Y('avg_applause:Q',axis=alt.Axis(title= 'Average number of applause')),
        color=alt.Color('parti:N', legend=alt.Legend(title="Party"), scale=party_colors ),
        order='dok_rm:O'
    )

    line.encoding.x.title = 'Parliament year'

    # Transparent selectors across the chart. This is what tells us
    # the x-value of the cursor
    selectors = alt.Chart(df[rm_filter], title='Average number of applause per speech').mark_point().encode(
        x='dok_rm:N',
        opacity=alt.value(0),
    ).add_selection(
        nearest
    )

    # Draw points on the line, and highlight based on selection
    points = line.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )

    # Draw text labels near the points, and highlight based on selection
    text = line.mark_text(align='left', dx=5, dy=-5).encode(
        text=alt.condition(nearest, 'avg_applause:Q', alt.value(' '),format='.2f')
    )

    # Draw a rule at the location of the selection
    rules = alt.Chart(df[rm_filter]).mark_rule(color='gray').encode(
        x='dok_rm:N',
    ).transform_filter(
        nearest
    )

    # Put the five layers into a chart and bind the data
    graph= alt.layer(
        line, selectors, points, rules, text
    ).properties(
        width=600, height=300
    )

    # maximum number of applause
    bars = alt.Chart(df_meta[rm_filter_meta], title='Maximum number of applause per speech').mark_bar().encode(
        x='dok_rm:O',
        y=alt.Y('max_applause:Q',axis=alt.Axis(title= 'No. applause')),
        color=alt.Color('parti:N', legend=alt.Legend(title="Party"), scale=party_colors ),
        tooltip=[
        alt.Tooltip('max_applause', title='Number of applause'),
        alt.Tooltip('dok_datum', title='Date'),    
        alt.Tooltip('Speaker', title='Speaker'),
        alt.Tooltip('Title', title='Title'),
        alt.Tooltip('Subtitle', title='Subtitle'),
        alt.Tooltip('Topic', title='Topic'),
        alt.Tooltip('Sentiment', title='Sentiment')
        ],
        href='Speech_html:N',
        ).properties(
        width=600, height=300
    )
    bars.encoding.x.title = 'Parliament year'

    st.altair_chart(graph, use_container_width=True)

    st.altair_chart(bars, use_container_width=True)