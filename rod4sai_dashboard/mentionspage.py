from rod_utils import agg_to_df
import streamlit as st
import datareader as dr
import altair as alt

@st.cache()
def get_data():
    partylist = ["S","M", "SD", "L","C","KD","V","MP"]
    criteria=   {
                "parti": {"$in": partylist }
        }

    # Mongo aggregation, 2 pieces
    pipeline= [
        {'$match':criteria},
        {'$unwind':'$authorities'},
        { "$group": {
            "_id": {
                "dok_rm": "$dok_rm",
                "authorities": "$authorities",
            },
            "Count": { "$sum": 1 }
        }}
    ]

    pipeline_TOT= [
        {'$match':criteria},
        { "$group": {
            "_id": {
                "dok_rm": "$dok_rm"
            },
            "Count_TOT": { "$sum": 1 }
        }}
    ]

    ag=agg_to_df(pipeline)
    ag_TOT=agg_to_df(pipeline_TOT)
    df = ag.merge(ag_TOT, on=['dok_rm'], how='outer')
    df['p_mention']=df['Count']/df['Count_TOT']
    df['rm_slider']=df['dok_rm'].str.slice(0,4).astype(int)

    return df
def showpage():    
    st.title("Mentions")
    st.markdown("Entities are identified using complex text search strings (regex)")
    st.sidebar.markdown("--------")

    authorities_list=dr.get_entities()
    authorities = st.sidebar.multiselect(f"Select among {len(authorities_list)} authorities",authorities_list, default=['Riksrevisionen','Arbetsförmedlingen','Försäkringskassan'])

    df=get_data()

    y_min=int(df['rm_slider'].min())
    y_max=int(df['rm_slider'].max())
    yearspan = st.sidebar.slider(label="Range (start year of Parliament year)", min_value=y_min, max_value=y_max,value=(y_min,y_max))

    start = yearspan[0]
    end = yearspan[1]
    end_rm=str(end)+"/"+str(end+1)[2:]
    rm_filter=((df.rm_slider <= end) & (df.rm_slider >= start)) & (df.authorities.isin(authorities))


    line = alt.Chart(df[rm_filter], title='Selected authorities in filter over time' ).mark_line().encode(
        x=alt.X('dok_rm:O', axis=alt.Axis(title='Parliament year')),
        y=alt.Y('Count:Q', axis=alt.Axis(title='Count of speeches with a mention')),
        color=alt.Color('authorities:N', legend=alt.Legend(title="Authority"), scale=alt.Scale(scheme='tableau10') )
        ).properties(width=600)

    line_p = alt.Chart(df[rm_filter]).mark_line().encode(
        x=alt.X('dok_rm:O', axis=alt.Axis(title='Parliament year')),
        y=alt.Y('p_mention:Q', axis=alt.Axis(title='Probability of a mention in a speach', format='.1%')),
        color=alt.Color('authorities:N', legend=alt.Legend(title="Authority")),
        ).properties(width=600)
    

    bars=alt.Chart(df[df.rm_slider == end],title=('All listed authorities',f'Mentions during Parliament year {end_rm} (the end point in the slider)')).mark_bar().encode(
        y=alt.Y('authorities:N',sort='-x'),
        x=alt.X('Count:Q', axis=alt.Axis(title='Count of speeches with a mention')),
        tooltip=[
            alt.Tooltip('p_mention:Q', title='Probability of a mention in a speach', format='.1%')
        ]
    ).properties(width=665)

    st.altair_chart(line & line_p, use_container_width=False)
    st.markdown('___')
    st.altair_chart(bars, use_container_width=False)
