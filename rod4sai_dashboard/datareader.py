import pandas as pd
import pymongo
import os
from pandas import json_normalize
from PIL import Image
from io import BytesIO

def last_value():
    one=mongo_collection().find_one(projection={'dok_rm':1,'dok_datum':1},sort=([("dok_datum", -1)]))    
    return one


def get_connstring():
    return os.getenv('CONNSTRING_MONGO', "mongodb://localhost:27017/")

def get_sentiment_datas():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    cursor = mycol.find({}, 
    { 
        "_id" : "$_id", 
        "dok_datum" : "$dok_datum", 
        "talare" : "$talare", 
        "parti" : "$parti", 
        "sent_pos" : "$sent_pos", 
        "sent_neg" : "$sent_neg", 
        "authorities" : "$authorities", 
        "applause" : "$applause"
    }
    ).sort([("dok_datum", pymongo.DESCENDING)])

    df = pd.DataFrame(list(cursor))
    return df

def mongo_find(sort=[("dok_datum", pymongo.DESCENDING)], limit=0, **kwargs):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    cursor = mycol.find(**kwargs).sort(sort).limit(limit)
    
    df = pd.DataFrame(list(cursor))
    return df
 
def mongo_collection(col="anforanden"):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb[col]
    return mycol

def mongo_aggregate(pipeline,**kwargs):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    aggregation = mycol.aggregate(pipeline,**kwargs)
    df=pd.DataFrame(list(aggregation))
    df_con=pd.concat([json_normalize(df['_id']), df.iloc[:,1:] ], axis=1)
    return df_con

def mongo_count(**kwargs):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    return mycol.count_documents(**kwargs)
    
def get_entities(col="authorities"):
    return list(mongo_collection(col).distinct("Entitet") )

def wc_stop():
    list_of_dict=list(mongo_collection('stopwords').find({}))
    stopwords_set=set(list_of_dict[0]['stopwords'])
    stopwords_set.update(['ledamöter','ledamöter','fru','talman','det','är','regeringens','se','ju','styleref kantrubrik','kantrubrik mergeformat','samtidigt','trots','tillbaka','dessutom','låt','em'])   
    return stopwords_set 

def get_wc_png():
    coll_bilder=mongo_collection(col='bilder')
    bild=coll_bilder.find_one()
    return bild['WC']

def get_wc_from_mongo():
    coll_bilder=mongo_collection(col='bilder')
    bild=coll_bilder.find_one({'namn':'WC'})
    stream = BytesIO(bild['bild'])
    image = Image.open(stream).convert("RGBA")
    stream.close()
    return image