import altair as alt
import streamlit as st
from rod_utils import agg_to_df

import datareader as dr


def showpage():

    st.title("Topics by LDA")
    st.markdown("Speeches by party filtered by topic and Parliament year")
    st.markdown("Click on topic bar to show relative importance of topic by parties.")

    # Fetch entities from other Mongo collection
    # But right now have do manual labour...
    entities = st.sidebar.multiselect("Named entity",dr.get_entities())
    if len(entities) ==0 :
        st.markdown("All")
        pipeline=[ 
            {'$match': 
                    {
                        "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }
                    }
            },
            {'$group': {
                    '_id':{'topic':'$topic', 'parti':'$parti', 'dok_rm':'$dok_rm'},
                    'count': {'$sum':1},
                    'sent_com': {'$avg':'$sent_com'},
                    'applause': {'$avg':'$applause'}
                    } 
            }
        ] 

        pipeline_topic =[
            {'$match':
                {
                    "parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] }
                } 
            },
                {'$group': {
                    '_id': {'topic':'$topic'},
                        'Count':{'$sum':1},
                        'sent_com': {'$avg':'$sent_com'},
                        'applause': {'$sum':'$applause'}
                }
            }
        ]
    else:
        pipeline=[ 
            {'$match': 
                    {
                        "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] },
                        "authorities": {"$in": entities }
                    }
            },
            {'$group': {
                    '_id':{'topic':'$topic', 'parti':'$parti', 'dok_rm':'$dok_rm'},
                    'count': {'$sum':1},
                    'sent_com': {'$avg':'$sent_com'},
                    'applause': {'$avg':'$applause'}
                    } 
            }
        ] 

        pipeline_topic =[
            {
                '$match':
                {
                    #"$text" : { "$search": 'virus' } ,
                    #"dok_datum": {'$lt': Dates[0]['end'], '$gte': Dates[0]['start']},
                    "parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] },
                    "authorities": {"$in": entities }
                } },
                {
                    '$group': {
                        '_id': {'topic':'$topic'},
                            'Count':{'$sum':1},
                            'sent_com': {'$avg':'$sent_com'},
                            'applause': {'$sum':'$applause'}

                    }
                }
        ]

    # Pandas prepp
    df=agg_to_df(pipeline)
    df.dropna(inplace=True)

    ag_topic=agg_to_df(pipeline_topic)

    # Selector list rm
    rm=df.dok_rm.unique().tolist()
    rm.sort(reverse=True)
    # Selector list topics
    topic=df.topic.unique().tolist()
    topic.sort(reverse=True)

    #sidebar
    riksmote = st.sidebar.selectbox("Parliament year :",rm)

    # Altair
    domain = ["M","L", "SD", "S","C","KD","V","MP"]
    range_ = ['#1f77b4', '#aec7e8', '#ffd92f','#d62728', '#98df8a', '#9467bd','#ff9896','#2ca02c']
    party_colors = alt.Scale(domain=domain, range=range_)

    # A dropdown filter
    #rm_dropdown = alt.binding_select(options=rm)
    #rm_select = alt.selection_single(fields=['dok_rm'], init={'dok_rm':'2019/20'}, bind=rm_dropdown, name="Parliament year")

    # Click selector: Choose topic display relative importance by party.
    selector = alt.selection(type='single', empty='none', encodings=['y'])

    # Define base
    base = alt.Chart(df[df['dok_rm']==riksmote]).transform_joinaggregate(
        # Normalize: Calculate total speeches by party
        TotalSpeeches='sum(count)',
        groupby=['parti', 'dok_rm']
    ).transform_calculate(
        PercentOfTotal="datum.count / datum.TotalSpeeches"
    ).add_selection(selector
    ).properties(width=200, height=300)

    #Define bars for topics
    topics = base.mark_bar().encode(
        alt.X('count:Q', axis=alt.Axis(title= 'Count of records')),
        alt.Y('topic:N', axis=alt.Axis(title= '', labelLimit=400), sort='-x'),
        color=alt.Color('parti:N',legend=None, scale=party_colors ),
        opacity=alt.condition(selector, alt.value(0.4), alt.value(1))
    )

    #Define bars for parties
    parties = base.mark_bar().encode(
        alt.X('PercentOfTotal:Q', axis=alt.Axis(title= 'Relative importance of topic')),
        alt.Y('parti:N', axis=alt.Axis(title= '', labelLimit=400), sort='-x'),
        color=alt.Color('parti:N',legend=None, scale=party_colors )

    ).transform_filter( selector )

    # Second altair plot
    circles = alt.Chart(ag_topic).mark_circle().encode(
    alt.X('sent_com:Q', axis=alt.Axis(title= 'Compound sentiment')),
    alt.Y('applause:Q', axis=alt.Axis(title= 'Number of applause')),
    alt.Size('Count:Q',legend=alt.Legend(title="Count of speeches") ),
    alt.Color('topic:N', legend=alt.Legend(title="Topics (by LDA topic modelling)", orient='bottom', columns=2, labelLimit=500), 
            scale=alt.Scale(scheme='tableau10'))
    ).properties(width=700)

    #Show p
    st.altair_chart(topics | parties)
    st.markdown("____")
    st.markdown("Speech by applause and sentiment aggregated by topics for entire period of 2003-2020")
    st.altair_chart(circles)

    st.markdown('__Note:__ LDA has been fitted using the Python package: _scikit-learn_. Learn more about topic modelling here: https://towardsdatascience.com/end-to-end-topic-modeling-in-python-latent-dirichlet-allocation-lda-35ce4ed6b3e0, https://www.ideals.illinois.edu/bitstream/handle/2142/46405/ParallelTopicModels.pdf?sequence=2&isAllowed=y')


