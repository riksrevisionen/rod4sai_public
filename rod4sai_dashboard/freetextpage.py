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

@st.cache(allow_output_mutation=True)
def visualisations(filter,maxhits,projection_meta,df_varlist,search_string):

    filter_meta=filter.copy()
    # Color coding for parties
    filter_meta.update({"parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] } }) 

    filter_total=filter_meta.copy()
    del filter_total['$text']

    ### Frequency bars and relative frequencies
    pipeline_timeline_tot =[ 
                {'$match': 
                        filter_total
                },
                {'$group': {
                        '_id':{'Year': {'$year': '$dok_datum'}}, 
                        'count_TOT': {'$sum':1} } }
                ] 
    pipeline_timeline =[ 
                {'$match': 
                        filter_meta
                },
                {'$group': {
                        '_id':{'Year': {'$year': '$dok_datum'}}, 
                        'count': {'$sum':1} } }
                ] 

    pipeline_tot =[ 
                {'$match': 
                        filter_total
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

    ## Trend line
    ## Bars frequency
    ag=rod_utils.agg_to_df(pipeline_timeline)
    ag_TOT=rod_utils.agg_to_df(pipeline_timeline_tot)
    dft = ag.merge(ag_TOT, on=['Year'], how='outer')
    dft['relative_count']=dft['count']/dft['count_TOT']
    del ag, ag_TOT
    dft['rc']='Relative count'
    dft['c']='Count'

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
    meta = dr.mongo_find(sort=[('dok_datum', pymongo.DESCENDING)],limit=2000, filter=filter_meta,projection=projection_meta).drop(columns='_id')
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
