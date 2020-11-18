import streamlit as st
import datareader as dr
import rod_utils
import re
from datetime import datetime 
import pymongo
import altair as alt

def list_extract(sel_entities=[]):
    all_entities = dr.mongo_collection('authorities').find({'Entitet':{'$nin':sel_entities } })
    exclude_list=sel_entities.copy()
    all_entities_excl = dr.mongo_collection('authorities').find({'Entitet':{'$nin': exclude_list } } )

    return list(all_entities), list(all_entities_excl)

def mark_data(text, search_string, tag_all=0, sel_entities=[], all_entities=[], all_entities_excl=[]):
    #Maybe add so that we catch whole words -> capture until next punctuation or whitespace?

    #applause
    text = re.sub(r"(?P<hit>\(Applåder\))",r"<mark data-entity='reaction'>\g<hit></mark>", text, flags=re.IGNORECASE)

    #entity 
    if tag_all==1:
        for entity in all_entities:
                text = re.sub(r"(?P<hit>" + entity['Söksträng'] + ")",r"<mark data-entity='authority'>\g<hit></mark>", text)
    elif tag_all==2:
        for entity in all_entities_excl:
            text = re.sub(r"(?P<hit>" + entity['Söksträng'] + ")",r"<mark data-entity='authority'>\g<hit></mark>", text)

    # Tag selected authorities
    if len(sel_entities) > 0:
        sel_auth = dr.mongo_collection('authorities').find({"Entitet": {"$in": sel_entities }})
        for auth in sel_auth:
            if auth['Entitet']=='Konjunkturinstitutet': # Special treatment for Konjunkturinstitutet since KI is an abreviation shared by both Karolinska institutet and Konjunkturinstitutet.
                text = re.sub(r"(?P<hit>" + auth['Söksträng'] + '|KI' + ")",r"<mark data-entity='sel_authority'>\g<hit></mark>", text)
            else:
                text = re.sub(r"(?P<hit>" + auth['Söksträng'] + ")",r"<mark data-entity='sel_authority'>\g<hit></mark>", text)

    #match
    for term in re.findall(r'(?<=")\w[\w\s-]*(?=")|\w+|"[\w\s-]*"', search_string):
        text = re.sub(r"(?P<hit>" + term.replace('"','') + ")",r"<mark data-entity='match'>\g<hit></mark>", text, flags=re.IGNORECASE)

    return text

def showpage():
    max_yrs=datetime.today().year
    st.title("Freetext search")
    search_string = st.text_input("Search string")
    maxhits = st.sidebar.slider(label="Max matches", min_value=1, max_value=100,  value=20)
    yearspan = st.sidebar.slider(label="Range (year)", min_value=2003, max_value=max_yrs,value=(max_yrs-5,max_yrs))
    entities = st.sidebar.multiselect("Select authorities",dr.get_entities())
    show_stat = st.sidebar.selectbox("Graphical visualisation of results", ('point','bars','heat','strip','bars by debate'))
    start = datetime(yearspan[0],1,1)
    end = datetime(yearspan[1],12,31)
    sentiment = st.sidebar.slider(label="Compound sentiment", min_value=-1.0, max_value=1.0, step=0.005,  value=(-1.0,1.0))

    # Generate lists
    all_entities, all_entities_excl = list_extract(entities)
    
    # Tag indicator for color taging
    TAG=0
    if st.sidebar.checkbox("Tag all listed authorities in dropdown menu"):
        TAG=1   

    if len(search_string) > 3:
        projection= {'dok_datum': 1, 'anforandetext': 1, 'talare': 1, 'protokoll_url_www': 1, 'avsnittsrubrik':1, 'sent_neg':1, 'sent_pos':1,'sent_com':1, 'topic':1, 'underrubrik':1,'kammaraktivitet':1}
        projection_meta= {'dok_datum': 1,'anforandetext': 1, 'parti':1, 'talare': 1,'anforande_nummer':1, 'anforande_url_html':1,'avsnittsrubrik':1,'topic':1, 'underrubrik':1,'kammaraktivitet':1}

        filter={
                    "sent_com": {'$lte': sentiment[1], '$gte': sentiment[0]},
                    "dok_datum": {'$lt': end, '$gte': start},
                    "$text" : { "$search": search_string }
                }
        if len(entities) is not 0:
            filter.update({"authorities": {"$all": entities }})

        filter_meta=filter.copy()
        # Color coding for parties
        filter_meta.update({"parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] } }) 
    
        # Number of speeches
        no_speeches=dr.mongo_count(filter=filter)

        #Abort function if 0 search results are returned
        if no_speeches==0:
            st.markdown("found 0 documents")
            return
    
        df = dr.mongo_find(sort=[("dok_datum", pymongo.DESCENDING),("anforande_nummer",pymongo.ASCENDING)],limit=maxhits, filter=filter,projection=projection)
        
        # Assert conformability to the printing output
        df_varlist=['dok_datum','avsnittsrubrik','underrubrik','talare','sent_com','topic','anforandetext','kammaraktivitet']
        for var in df_varlist:
            if var not in df:
                df[var]=None

        ### Frequency bars and relative frequencies

        pipeline_tot =[ 
                    {'$match': 
                            filter_meta
                    },
                    {'$group': {
                            '_id':{'parti':'$parti'}, 
                            'count_TOT': {'$sum':1} } }
                    ] 

        pipeline =[ 
                    {'$match': 
                            filter_meta
                    },
                    {'$group': {
                            '_id':{'parti':'$parti'}, 
                            'count': {'$sum':1} } }
                    ]  

        pipeline_heat =[ 
                    {'$match': 
                            filter_meta
                    },
                    {'$group': {
                            '_id':{
                            'Month':{'$month':'$dok_datum'},
                            'Year': {'$year': '$dok_datum'}
                            }, 
                            'count': {'$sum':1} } }
                    ]                     

        pipeline_debate =[
                    {
                        '$match':
                        filter_meta },
                        {
                            '$group': {
                                '_id': {'dok_id':'$dok_id'},
                                    'Section':{'$first':'$avsnittsrubrik'},
                                    'Activity':{'$first':'$kammaraktivitet'},
                                    'Date':{'$first':'$dok_datum'},
                                    'Number':{'$max':'$anforande_nummer'},
                                    'URL':{'$first':'$protokoll_url_www'},
                                    'sent_com': {'$avg':'$sent_com'},
                                    'applause': {'$sum':'$applause'}

                            }
                        },
                        {'$sort':{'Date':-1 }},
                        { '$limit' : maxhits }
                ]

        #########################################################################################################################################        
        # Pandas preparations        
        ## Bars_debate                
        ag_debate=rod_utils.agg_to_df(pipeline_debate)
        ag_varlist=['dok_datum','Section','Activity','Date','Number','URL','sent_com','applause']
        for var in ag_varlist:
            if var not in ag_debate:
                ag_debate[var]=None

        ## Bars frequency
        ag_TOT=rod_utils.agg_to_df(pipeline_tot)
        ag=rod_utils.agg_to_df(pipeline)
        dfa = ag.merge(ag_TOT, on=['parti'], how='outer')
        dfa['relative_count']=dfa['count']/dfa['count_TOT']
        del ag_TOT, ag

        ## Heat map
        meta_heat=rod_utils.agg_to_df(pipeline_heat)

        ## Data for points and strip charts
        meta = dr.mongo_find(sort=[('dok_datum', pymongo.DESCENDING)],limit=3000, filter=filter_meta,projection=projection_meta).drop(columns='_id')
        # Assert conformability to the printing output
        for var in df_varlist:
            if var not in meta:
                meta[var]=None

        # Variable to use in tooltip
        # Use <p> </p> to find first matching search string    
        # TODO Hantera minustecken framför ord, typ: " KI " -Karolinska -filial       
        sstring=search_string.replace(r'"','') # Removes citation-marks in order to properly regex search     
        meta['First_match']=meta['anforandetext'].str.extract(f'((?<=<p>)(?:(?!<\/p>).)*{sstring}(?:(?!<\/p>).)*(?=<\/p>))', re.IGNORECASE, expand=False)          
        # Fall-back code when <p>-tags are missing     
        meta_filter=meta['First_match'].isna()
        meta.loc[meta_filter,'First_match']=meta.loc[meta_filter,'anforandetext'].str.extract(f'([^.]*{sstring}[^.]*\.)', re.IGNORECASE, expand=False)
        # Fail protection
        if 'First_match' not in meta:
            meta['First_match']=None

        # End preparations
        #########################################################################################################################################

        #########################################################################################################################################
        ## Altair prepp

        meta['Year'] = meta['dok_datum'].dt.year
        meta['Month'] = meta['dok_datum'].dt.month
        meta['All_parties']='Mentions'

        # Altair graphs:
        # Control party colors!
        domain = ["M","L", "SD", "S","C","KD","V","MP",'-']
        range_ = ['#1f77b4', '#aec7e8', '#ffd92f','#d62728', '#98df8a', '#9467bd','#ff9896','#2ca02c','#cccccc']
        party_colors = alt.Scale(domain=domain, range=range_)

        # Heat map
        heat = alt.Chart(meta_heat).mark_rect().encode(
            x='Year:O',
            y='Month:O',
            color='count:Q'
        ).properties(
        width=200,
        height=200 )
          

        #Strip plot
        strip = alt.Chart(meta).mark_tick().encode(
            alt.X('dok_datum:T'),
            alt.Y('All_parties:N', title=''),
            color=alt.Color('parti:N', legend=None ),
            #color=alt.Color('parti:N', legend=alt.Legend(title="Party", columns=3))
        ).properties(
        width=200,
        height=80 )

        point = alt.Chart(meta).mark_point(opacity=0.6).encode(
            alt.X('dok_datum:T', axis=alt.Axis(title= 'Date')),
            alt.Y('parti:N', title=''),
            color=alt.Color('parti:N', legend=None, scale=party_colors ),
            tooltip=[
            alt.Tooltip('dok_datum', title='Date'),    
            alt.Tooltip('talare', title='Speaker'),
            alt.Tooltip('anforande_nummer', title='Speach no.'),
            alt.Tooltip('kammaraktivitet', title='Activity'),
            alt.Tooltip('avsnittsrubrik', title='Title'),
            alt.Tooltip('underrubrik', title='Subtitle'),
            #alt.Tooltip('Entities', title='Mentioned entities'),
            alt.Tooltip('topic', title='Topic'),
            alt.Tooltip('First_match', title='First matching paragraph')
            ],
            href='anforande_url_html:N'
        ).properties(
        width=200,
        height=200 )

        # Frequency bars
        bars = alt.Chart(dfa).mark_bar().encode(
            alt.X('count:Q', axis=alt.Axis(title= 'Count of records')),
            alt.Y('parti:N', axis=alt.Axis(title= 'Party')),
            color=alt.Color('parti:N',legend=None, scale=party_colors ),
        ).properties(width=300)

        bars_p = alt.Chart(dfa).mark_bar().encode(
            alt.X('relative_count:Q', axis=alt.Axis(title= 'Normalized count by total speeches by party')),
            alt.Y('parti:N', axis=alt.Axis(title= 'Party')),
            color=alt.Color('parti:N',legend=None, scale=party_colors ),
        ).properties(width=300)

        # By Chapter title
        bars_debate = alt.Chart(ag_debate,title=[f'Number of speeches aggregated by subject matter,', f'{maxhits} latest by search term']).mark_bar().encode(
            alt.X('Number:Q', axis=alt.Axis(title= 'Number of speeches in debate')),
            alt.Y('Section:N', axis=alt.Axis(title= '', labelLimit=400), sort='-x'),
            color=alt.Color('sent_com:Q',legend=alt.Legend(title="Sentiment") ),
            href='URL:N',
            tooltip=[
                    alt.Tooltip('Number', title='Number of speeches in debate'),
                    alt.Tooltip('Date', title='Date'),
                    alt.Tooltip('Activity', title='Chamber activity'),
                    alt.Tooltip('sent_com', title='Average sentiment'),    
                    alt.Tooltip('applause', title='Number of applause')
            ]
        ).properties(width=400)

        #########################################################################################################################################
        
        st.markdown(f"Total number of hits: {no_speeches}")
        if show_stat=='heat':       
            st.altair_chart(heat, use_container_width=True)
        elif show_stat=='strip':
            if no_speeches>3000:
                st.markdown(f"Warning! Text query covers over 3000 documents. Visual representation automatically switched to 'heat' ")
                st.altair_chart(heat, use_container_width=True)           
            else:
                st.altair_chart(strip, use_container_width=True)
        elif  show_stat=='point':
            if no_speeches>3000:
                st.markdown(f"Warning! Text query covers over 3000 documents. Visual representation automatically switched to 'heat' ")
                st.altair_chart(heat, use_container_width=True) 
            else:
                st.altair_chart(point, use_container_width=True)    
        elif  show_stat=='bars':
            st.altair_chart(bars | bars_p, use_container_width=True)    
        elif show_stat=='bars by debate':
            st.altair_chart(bars_debate, use_container_width=True)     
        #
        #st.table(df["anforandetext"])
        st.markdown("<style>"
        "[data-entity=person] {background:#f0f0f0;}"
        "[data-entity=entity] {background:#f0f0f0;}"
        "[data-entity=authority] {background:#f0f0f0;}"
        "[data-entity=sel_authority] {background:green;}"
        "[data-entity=tag] {background:#f0f0f0;}"
        "[data-entity=match] {background:cyan;}"
        "[data-entity=reaction] {background:red;}"
        "[data-entity] {padding: .2em .3em; margin: 0 .25em; line-height: 1; display: inline-block; border-radius: .25em;}"
        "[data-entity]:after {box-sizing: border-box;content: attr(data-entity);font-size: .55em;line-height: 1;padding: .35em .35em;border-radius: .35em;text-transform: uppercase;display: inline-block;vertical-align: middle;margin: 0 0 .15rem .5rem;background: #fff;font-weight: 700;}"
        "</style>",unsafe_allow_html=True)

        st.markdown(f"#### Found {no_speeches} documents containing \"{search_string}\". List restricted to {len(df.index)}  documents.")
        st.markdown("##")

        for row in df.itertuples():
            #replace ents with <mark data-entity="entity">entname<mark>
            st.markdown(f"#### Date: {str(row.dok_datum)[:10]}")
            #st.markdown(f"#### Chamber activity: {row.kammaraktivitet}")
            st.markdown(f"<h4><a href=\"{row.protokoll_url_www}\" target=\"_blank\">{row.avsnittsrubrik}</a></h4>",unsafe_allow_html=True)
            if type(row.underrubrik) is str and len(row.underrubrik)>0:
                st.markdown(f"#### Subtitle: {row.underrubrik}")
            st.markdown(f"#### {row.talare}")
            st.markdown(f"#### Sentiment: {row.sent_com}")
            st.markdown(f"#### Topic: {row.topic}")
            st.markdown(mark_data(row.anforandetext, search_string, tag_all=TAG, sel_entities=entities, all_entities=all_entities, all_entities_excl=all_entities_excl),unsafe_allow_html=True)
            st.markdown("-----------------")            
    
    elif len(search_string) in range(1,3):
        st.markdown("Search term to short. Minimum length is a 4 letter words.") 

    st.markdown("*__How to text search:__*")
    st.markdown(""" __OR__: Space between words </br> (*word<sub>1</sub>* *word<sub>2</sub>*). \n\n __AND__: Words encapsulated in quotation marks with space in between </br> (*"word<sub>1</sub>"* *"word<sub>2</sub>"*). \n\n __PHRASE__: Encapsulate two or more words in quotation marks to search for the phrase </br> (*"word<sub>1</sub> word<sub>2</sub> word<sub>n</sub>"*) \n\n __EXCLUDE__: Prefix a word with a minus sign to exclude word from search results </br> (*word<sub>1</sub> __-__word<sub>2</sub>*)""", unsafe_allow_html=True)
    st.markdown("---")