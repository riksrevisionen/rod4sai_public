import altair as alt
import streamlit as st
from rod_utils import formatera_text_pandas
import datareader as dr

@st.cache(allow_output_mutation=True)
def getdata(riksmote, authorities=[]):
    
    projection= {'dok_hangar_id': 0, 'anforande_url_xml': 0, 'anforande_url_html': 0, 'topic_prob': 0}
    if len(authorities)==0:
        filter=   {
                "dok_rm": {"$in": riksmote},
                "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }
        }  
    else:        
        filter=   {
                "dok_rm": {"$in": riksmote},
                "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] },
                "authorities": {'$all': authorities}
        }   
    
    df = dr.mongo_find(filter=filter, projection=projection, limit=14000)
    
    no_speeches=dr.mongo_count(filter=filter)
    
    return df, no_speeches

def showpage():
    
    st.title("Sentiment")
    st.markdown("Scatter plot of speeches by positive and negative sentiment")
    st.markdown("")
    st.markdown("* Select party in the legend to filter by party, hold [shift] to select multiple parties")
    st.markdown("* Draw an area in the scatter plot to filter count of records in the bar chart")
    st.markdown("* Hover the mouse over an observation to get more data, click to access the protocol")
    st.markdown("* Use the dropdown to the left to filter on authority mentioned")
    st.markdown("* Use the dropdown to the left to select Parliament year")

    st.sidebar.markdown("--------")
    y=dr.last_value()

    y_max=int(y.get('dok_rm')[:4])
    y_min=2003
    yearspan = st.sidebar.slider(label="Range (start of Parliament year)", min_value=y_min, max_value=y_max,value=(y_max-1,y_max))
    riksmote=[]
    for year in range(yearspan[0],yearspan[1]+1):
        riksmote.append(str(year)+"/"+str(year+1)[2:])

    authorities = st.sidebar.selectbox(f"Select among {len(dr.get_entities())} authorities",dr.get_entities())

    df, no_speeches=getdata(riksmote, [authorities])
    st.markdown('___')
    if no_speeches>14000:
        st.markdown(f"___Warning!___ Number of speeches selected exceeds 14,000 (you selected {no_speeches:,.0f}), data is truncated to the last 14,000 speeches. Please select a more narrow filter because this scatter plot won't make sense anyway...")
    else:
        st.text(f"Number of speeches selected {no_speeches}.")

    df['anforandetext']=formatera_text_pandas(df['anforandetext'])

    df_varlist=['avsnittsrubrik','underrubrik','talare','sent_com','topic','anforandetext','kammaraktivitet']
    for var in df_varlist:
        if var not in df:
            df[var]=None
      # Gör en lista av valbara alternativ

    alt.data_transformers.disable_max_rows()

    # Brush for selection
    brush = alt.selection(type='interval')
    selection = alt.selection_multi(fields=['parti'], bind='legend')

    # Control party colors!
    domain = ["M","L", "SD", "S","C","KD","V","MP"]
    range_ = ['#1f77b4', '#aec7e8', '#ffd92f','#d62728', '#98df8a', '#9467bd','#ff9896','#2ca02c']

    # The chart
    points = alt.Chart(df).mark_point().encode(
        alt.X('sent_neg:Q', scale=alt.Scale(domain=(0, 0.35), clamp=True ) ),
        alt.Y('sent_pos:Q', scale=alt.Scale(domain=(0, 0.5), clamp=True ) ),
        color=alt.condition(brush, alt.Color('parti:N', legend=alt.Legend(title="Party"),
        scale=alt.Scale(domain=domain, range=range_) ), alt.value('grey') ),
        tooltip=[
        alt.Tooltip('dok_datum', title='Date'),    
        alt.Tooltip('talare', title='Speaker'),
        alt.Tooltip('anforande_nummer', title='Speach no.'),
        alt.Tooltip('kammaraktivitet', title='Activity'),
        alt.Tooltip('avsnittsrubrik', title='Title'),
        alt.Tooltip('underrubrik', title='Subtitle'),
        alt.Tooltip('authorities', title='Mentioned entities'),
        alt.Tooltip('topic', title='Topic'),
        alt.Tooltip('applause', title='No. applause'),
        alt.Tooltip('anforandetext', title='Speech')
        ],
        href='protokoll_url_www:N',
        opacity=alt.condition(selection, alt.value(1), alt.value(0.01))
    ).add_selection(brush, selection)

    points.encoding.x.title = 'Negative sentiment'
    points.encoding.y.title = 'Positive sentiment'

    # Build bars with counts of party
    bars = alt.Chart(df).mark_bar().encode(
        y='parti:N',
        color=alt.Color('parti:N',legend=None),
        x='count(parti):Q'
    ).transform_filter(
        selection
    ).transform_filter(
        brush
    )
    bars.encoding.y.title = 'Party'

    # Apply drop-down filter to points and bars
    filtered_points = points.properties(title="Speach sentiment")

    filtered_bars = bars.properties(height=150)

    points_topics = alt.Chart(df).mark_point().encode(
        alt.X('sent_neg:Q', scale=alt.Scale(domain=(0, 0.35), clamp=True ) ),
        alt.Y('sent_pos:Q', scale=alt.Scale(domain=(0, 0.5), clamp=True ) ),
        color=alt.condition(brush, alt.Color('parti:N', legend=alt.Legend(title="Party"),
        scale=alt.Scale(domain=domain, range=range_) ), alt.value('grey') ),
        shape=alt.Shape('topic:N'),
        tooltip=[
        alt.Tooltip('dok_datum', title='Date'),    
        alt.Tooltip('talare', title='Speaker'),
        alt.Tooltip('anforande_nummer', title='Speach no.'),
        alt.Tooltip('kammaraktivitet', title='Activity'),
        alt.Tooltip('avsnittsrubrik', title='Title'),
        alt.Tooltip('underrubrik', title='Subtitle'),
        alt.Tooltip('authorities', title='Mentioned entities'),
        alt.Tooltip('applause', title='Applause'),
        alt.Tooltip('anforandetext', title='Speech')
        ],
        href='protokoll_url_www:N',
        opacity=alt.condition(selection, alt.value(1), alt.value(0.01))
    ).add_selection(brush, selection)

    points_topics.encoding.x.title = 'Negative sentiment'
    points_topics.encoding.y.title = 'Positive sentiment'

    # Build bars with counts of party
    bars = alt.Chart(df).mark_bar().encode(
        y='parti:N',
        color=alt.Color('parti:N',legend=None),
        x='count(parti):Q'
    ).transform_filter(
        selection
    ).transform_filter(
        brush
    )
    bars.encoding.y.title = 'Party'

    # Apply drop-down filter to points and bars
    filtered_points_topics = points_topics.properties(title="Speach sentiment")

    filtered_bars = bars.properties(height=150)

    # Build chart
    if st.sidebar.checkbox('Include topics i scatter plot as shapes'):
        st.altair_chart(filtered_points_topics & filtered_bars, use_container_width=True)
    else:
        st.altair_chart(filtered_points & filtered_bars, use_container_width=True)
