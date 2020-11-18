import streamlit as st
import datareader as dr
import rod_utils
import re
from datetime import datetime 
import altair as alt

def list_extract(sel_entities=[]):
    all_entities = dr.mongo_collection('authorities').find({'Entitet':{'$nin':sel_entities } })
    exclude_list=sel_entities.copy()
    all_entities_excl = dr.mongo_collection('authorities').find({'Entitet':{'$nin': exclude_list } } )

    return list(all_entities), list(all_entities_excl)

def showpage():
    st.title("Plot text trends")
    st.markdown("Plot the trend of your custom text search in absolute and relative terms")
    search_string = st.text_input("Search string")
    maxy=datetime.today().year
    yearspan = st.sidebar.slider(label="Range (year)", min_value=2003, max_value=maxy,value=(2003,maxy))
    start = datetime(yearspan[0],1,1)
    end = datetime(yearspan[1],12,31)
    sentiment = st.sidebar.slider(label="Compound sentiment", min_value=-1.0, max_value=1.0, step=0.005,  value=(-1.0,1.0))

    if len(search_string) > 3:
        # Number of speeches
        filter=   {
            "sent_com": {'$lte': sentiment[1], '$gte': sentiment[0]},
            "dok_datum": {'$lt': end, '$gte': start},
            "$text" : { "$search": search_string }
        }
        no_speeches=dr.mongo_count(filter=filter)

        #Abort function if 0 search results are returned
        if no_speeches==0:
            st.markdown("found 0 documents")
            return
    
    
        pipeline_tot =[ 
                    {'$match': 
                            {
                                "sent_com": {'$lte': sentiment[1], '$gte': sentiment[0]},
                                "dok_datum": {'$lte': end, '$gte': start},
                                "parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] } 
                            }
                    },
                    {'$group': {
                            '_id':{'Year': {'$year': '$dok_datum'}}, 
                            'count_TOT': {'$sum':1} } }
                    ] 


        pipeline =[ 
                    {'$match': 
                            {
                                "sent_com": {'$lte': sentiment[1], '$gte': sentiment[0]},
                                "$text" : { "$search": search_string } ,
                                "dok_datum": {'$lte': end, '$gte': start},
                                "parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] } 
                            }
                    },
                    {'$group': {
                            '_id':{'Year': {'$year': '$dok_datum'}}, 
                            'count': {'$sum':1} } }
                    ]   

        # Pandas preparations  
        # 
        ## Trend line
        ## Bars frequency
        ag=rod_utils.agg_to_df(pipeline)
        ag_TOT=rod_utils.agg_to_df(pipeline_tot)
        dfa = ag.merge(ag_TOT, on=['Year'], how='outer')
        dfa['relative_count']=dfa['count']/dfa['count_TOT']
        del ag
        dfa['rc']='Relative count'
        dfa['c']='Count'

        #list_strings=search_string.split(' ')
        #Ett avancerat sätt att dela strängar på för att hantera citationstecken
        PATTERN = re.compile(r'''((?:[^\s"']|"[^"]*"|'[^']*')+)''')
        list_strings=PATTERN.split(search_string)[1::2]

        if len(list_strings)>1:
            dfa['rc']='All search terms'
            df_multi=rod_utils.multi_agg(list_strings,sentiment[1],sentiment[0],start, end)
            df_multi = df_multi.merge(ag_TOT, on=['Year'], how='outer')
            df_multi['relative_count']=df_multi['count']/df_multi['count_TOT']

            multi_chart=alt.Chart(df_multi).mark_line().encode(
                x='Year:O',
                y=alt.Y('relative_count:Q', axis=alt.Axis(title= 'Relative count by year')),
                color='search_string:N'
            ).properties(
                width=180,
                height=120
            ).facet(
                facet='search_string:N',
                columns=3
            )
                #########################################################################################################################################        
        

        # End preparations
        #########################################################################################################################################

        #########################################################################################################################################
        ## Altair prepp

        # Altair graphs:

        # Line chart
        line = alt.Chart(dfa).mark_line().encode(
            x='Year:O',
            y='count:Q',
            color=alt.Color('c:N')
        )
        
        # Line chart party
        line_relative = alt.Chart(dfa).mark_line(color='red').encode(
            x='Year:O',
            y=alt.Y('relative_count:Q', axis=alt.Axis(title= 'Relative count by year')),
            color=alt.Color('rc:N', legend=alt.Legend(title=""))
        )
          
             
        comb=(line + line_relative).resolve_scale(y='independent').properties(width=800)
        
        #########################################################################################################################################
        

        st.markdown(f"Total number of hits: {no_speeches}")
        if len(list_strings)>1:
            st.altair_chart(line_relative.properties(width=500,height=100) & multi_chart, use_container_width=True)
        else:
            st.altair_chart(comb, use_container_width=True)

    elif len(search_string) in range(1,3):
        st.markdown("Search term to short. Minimum length is a 4 letter words.") 

    st.markdown("*__How to text search:__*")
    st.markdown(""" Write a single word or multiple search words separated by space.""", unsafe_allow_html=True)
    st.markdown("---")