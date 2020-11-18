import codecs
import pandas as pd
import pymongo
from datareader import get_connstring
from pandas import json_normalize


def formatera_text(txt):
    """ Fixa texten till mer läsbart format """
    if txt is not None:
        txt=txt.replace('</p>',''
        ).replace('<p>',''
        ).replace('styleref', ''
        ).replace('kantrubrik',''
        ).replace('mergeformat',''
        ).replace('-\n',''
        ).replace('\n',' ')
    return txt 

def formatera_text_n(txt):
    """ Fixa texten till mer läsbart format men byter p-taggar till radbryt och behåller andra radbryt"""
    if txt is not None:
        txt=txt.replace('</p>','\n').replace('<p>','').replace('</em>','').replace('<em>','').replace('STYLEREF Kantrubrik \* MERGEFORMAT','').replace('-\n','')
    return txt 

def formatera_text_pandas(series):
    """ Fixa texten till mer läsbart format men byter p-taggar till radbryt och behåller andra radbryt"""
    series=series.str.replace('</p>','\n', regex=False).str.replace('<p>','', regex=False).str.replace('STYLEREF Kantrubrik \* MERGEFORMAT','', regex=False)
    #.str.replace('-\n','')
    return series 

def stop_words(stop_word_list=r'rir_stopwords.txt'):
        
    stopwords = []
    with codecs.open(stop_word_list,'r',"utf-8") as f:
        for i,line in enumerate (f):
            stopwords.append(line.lower().strip().replace('\n', '').replace('\r', '').replace('\t',''))

    return frozenset(stopwords)

def talare_clean(series):   
    """ Cleaning the speaker names, caps, party abreviation and title stripping """     
    dd=series.str.title().str.replace(
    "(Mp)","(MP)",case=True, regex=False).str.replace(
    "(Fp)","(L)",case=True, regex=False).str.replace(
    "(FP)","(L)",case=True, regex=False).str.replace(
    "(Sd)","(SD)",case=True, regex=False).str.replace(
    "(Kd)","(KD)",case=True, regex=False).str.replace(
    " Replik","",case=False, regex=False).str.replace(
    " (Replik)","",case=False, regex=False).str.replace(
    "Statsrådet ", "",case=True, regex=False).str.replace(
    "^.*\s*.*\s*.*inister[n]* ", "",case=True, regex=True)
    return dd  

def multi_agg(list_strings,sent_pos,sent_neg,start, end,):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    listan=[]
    for string in list_strings:
        pipeline =[ 
            {'$match': 
                    {
                        "sent_com": {'$lte': sent_pos, '$gte': sent_neg},
                        "$text" : { "$search": string } ,
                        "dok_datum": {'$lte': end, '$gte': start},
                        "parti": {"$in": ["M","L", "SD", "S","C","KD","V","MP"] } 
                    }
            },
            {'$group': {
                    '_id':{'Year': {'$year': '$dok_datum'}}, 
                    'count': {'$sum':1} }
                    },
                       {
                    '$addFields':
                    {'search_string' : { '$literal': string }
                    }
                }
            ] 
        #    
        agg = list(mycol.aggregate(pipeline))
        listan.extend(agg)
    df=pd.DataFrame(listan)
    df_con=pd.concat([json_normalize(df['_id']), df.iloc[:,1:] ], axis=1)
    return df_con

def agg_to_df(pipeline):
    """ Aggregate pipeline to DataFrame """

    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    agg = mycol.aggregate(pipeline)
    df=pd.DataFrame(list(agg))
    df_con=pd.concat([json_normalize(df['_id']), df.iloc[:,1:] ], axis=1)
    return df_con

def tabstat_2way(criteria,group1,group2,statistic='avg',variable1=None,variable2=None,variable3=None,variable4=None):

    """ Calculates a statistic of one to four variables over two groups using pymongo. Always gives a frequency count. Outputs a Pandas DataFrame.
    requires: pandas, pymongo, json_normalize from pandas.io.json
    """

    # Mongo-pipeline
    if variable2 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            'count': {'$sum':1}  
            } }
        ]
    elif variable3 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            'count': {'$sum':1}   
            } 
            }
        ]
    elif variable4 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},
            'count': {'$sum':1}      
            } 
            }
        ]
    else:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},    
            statistic+'_'+variable4:{'$'+statistic:'$'+variable4},
            'count': {'$sum':1}      
            } 
            }
        ]

    return agg_to_df(pipeline)


def tabstat_2way_first(criteria,group1,group2,first,statistic='avg',variable1=None,variable2=None,variable3=None,variable4=None):

    """ Calculates a statistic of one to four variables over two groups using pymongo. Always gives a frequency count and always takes a 'first'-argument (first observation of the within group). Outputs a Pandas DataFrame.
    requires: pandas, pymongo, json_normalize from pandas.io.json
    """
    # Mongo-pipeline
    if variable2 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            'count': {'$sum':1}  
            } }
        ]
    elif variable3 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            'count': {'$sum':1}   
            } 
            }
        ]
    elif variable4 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},
            'count': {'$sum':1}      
            } 
            }
        ]
    else:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},    
            statistic+'_'+variable4:{'$'+statistic:'$'+variable4},
            'count': {'$sum':1}      
            } 
            }
        ]
    return agg_to_df(pipeline)
    #cursor = collection.aggregate(pipeline)
    #df=pd.DataFrame(list(cursor))
    #df_con=pd.concat([json_normalize(df['_id']), df.iloc[:,1:] ], axis=1)
    #return df_con

    
def tabstat_3way_first(criteria,group1,group2,group3,first,statistic='avg',variable1=None,variable2=None,variable3=None,variable4=None):

    """ Calculates a statistic of one to four variables over two groups using pymongo. Always gives a frequency count and always takes a 'first'-argument (first observation of the within group). Outputs a Pandas DataFrame.
    requires: pandas, pymongo, json_normalize from pandas.io.json
    """
    # Mongo-pipeline
    if variable2 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2, group3:'$'+group3}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            'count': {'$sum':1}  
            } }
        ]
    elif variable3 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2, group3:'$'+group3}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            'count': {'$sum':1}   
            } 
            }
        ]
    elif variable4 is None:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2, group3:'$'+group3}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},
            'count': {'$sum':1}      
            } 
            }
        ]
    else:
        pipeline = [ 
            {'$match': criteria },
            {'$group': {'_id':{group1:'$'+group1, group2:'$'+group2, group3:'$'+group3}, 
            first:{'$first':'$'+first},
            statistic+'_'+variable1:{'$'+statistic:'$'+variable1},
            statistic+'_'+variable2:{'$'+statistic:'$'+variable2},
            statistic+'_'+variable3:{'$'+statistic:'$'+variable3},    
            statistic+'_'+variable4:{'$'+statistic:'$'+variable4},
            'count': {'$sum':1}      
            } 
            }
        ]
    return agg_to_df(pipeline)