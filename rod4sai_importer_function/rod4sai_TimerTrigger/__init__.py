import datetime
import logging
import azure.functions as func
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobBlock
from io import StringIO
from io import BytesIO
import json
import requests
import pymongo
import os
import sys
import codecs
import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import joblib
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from nltk import sent_tokenize, word_tokenize
from nltk.stem import SnowballStemmer
import nltk
from wordcloud import WordCloud

# Define own LemmaTokenizer class used in vectorizer in function add_missing_topics_to_db()
class LemmaTokenizer:
    """ Tokenize, remove punctuation and numbers (t.isalpha() and lemmatize using SnowballStemmer) """
    def __init__(self):
        self.ss = SnowballStemmer("swedish") # SnowballStemmer kinda sucks but it is the only lemmatizer we got...

    def __call__(self, doc):
        return [self.ss.stem(t) for t in word_tokenize(doc, language='swedish') if t.isalpha()]
        

########################### DATABASE STUFF ###########################################
def get_connstring():
    """Gets a connectionstring from Azure ENV-variable CONNSTRING_MONGO or returns a localhost
    
    Returns:
        string -- a mongodb connectionstring 
    """
    return os.getenv('CONNSTRING_MONGO', "mongodb://localhost:27017/")

def log(message):
    logging.info(f"{datetime.datetime.now().isoformat()} : {message}")

    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["log"]
    logdoc = {'time': datetime.datetime.now().isoformat(), 'message': message}
    mycol.insert_one(logdoc)


def getblob(blobname):
    secret = os.getenv("AZURE_BLOB_STORAGE_SECRET","")
    container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME","")
    #https://datalabbstorage.blob.core.windows.net/rod4sai-import-function-files/entitites.csv
    blob = BlobClient(account_url="https://datalabbstorage.blob.core.windows.net",container_name=container_name,blob_name=blobname,credential=secret)
    data = blob.download_blob().readall()
    return data

####################### Formatting and converting ####################################
def format_dates_in_anfs(document_list):
    for doc in document_list:
        doc["dok_datum"] = datetime.datetime.strptime(doc["dok_datum"][:10],"%Y-%m-%d")
    return document_list

def format_parties_in_anfs(document_list):
    for doc in document_list:
        doc["parti"] = doc["parti"].upper()
    return document_list

def format_text(txt):
    """ Fixa texten till mer läsbart format """
    if txt is not None:
        txt=txt.replace('</p>','').replace('<p>','').replace('STYLEREF Kantrubrik \* MERGEFORMAT','').replace('-\n','').replace('\n',' ')
        txt=txt.replace('</p>',' ').replace('<p>','').replace('</em>','').replace('<em>','').replace('STYLEREF Kantrubrik \* MERGEFORMAT','').replace('-\n','').replace('\n',' ')
    return txt 

####################### ROD STUFF #####################################################
#Get data from riksdagens öppna data (ROD) input:date YYYY-mm-dd, output json 
def get_anfs_from_rod(from_date):
    """Gets all "anföradnden" as JSON from ROD since "from_date"
    
    Arguments:
        from_date {datetime} 
    
    Returns:
        json
    """
    #Catch errors!!
    from_date_string = from_date.strftime("%Y-%m-%d")
    response = requests.get(f"http://data.riksdagen.se/anforandelista/?rm=&anftyp=&d={from_date_string}&ts=&parti=&iid=&sz=99999&utformat=json")

    response_json = json.loads(response.text.replace("None","'None'"))

    new_anfs = response_json["anforandelista"]["@antal"]
    log(f"Got {new_anfs} new anfs from ROD")
    #Refactor. Check for zero new documents..
    
    #strip response of extra headers aso..
    response_json = response_json["anforandelista"]["anforande"]
    return response_json

#Get anforandetext from url
def get_anf_text_from_rod(url):
    #Catch errors!!
    response = requests.get(url)
    response_json = json.loads(response.text)
    return response_json["anforande"]["anforandetext"]

def populate_anfs_with_text(documents):
    for doc in documents:
        anftext = get_anf_text_from_rod(doc["anforande_url_xml"] + "/json")
        doc["anforandetext"] = anftext
    return documents


def add_missing_topics_to_db():
    # 0. Establish connection
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    # 1. Get data
    criteria={ 'topic_num':{'$exists': False} }
    cursor=mycol.find(criteria,{'anforandetext':1} )
    #print(f"Performing operations on {mycol.count_documents(criteria)} speeches")
    
    c_list = []
    id_list = []
    for anf in cursor:    
        if anf['anforandetext'] is not None: # Only non-empty speeches
            if len(anf['anforandetext'])>20: # Only speeches with more than 20 characters
                c_list.append(format_text(anf['anforandetext']))
                id_list.append(anf['_id'])

    #ugly way to cop out if no new anfs to get topics for 
    if len(c_list) == 0:
        return

    # 2. Get stopwords and use stemmer
    stemmer = SnowballStemmer("swedish")
    stopwords = []
    with StringIO(getblob("rir_stopwords.txt").decode("utf-8")) as f:
        for i,line in enumerate (f):
            stopwords.append(stemmer.stem(line.lower().strip().replace('\n', '').replace('\r', '').replace('\t','')))

    stop_words = frozenset(stopwords)

    # 3. Load model with attributes and Vocabulary ####
    # Instantiate an LDA
    lda_model = LatentDirichletAllocation()
    # Loading Vocabulary and model attributes
    dic, lda_model.components_, lda_model.exp_dirichlet_component_, lda_model.doc_topic_prior_ = joblib.load(BytesIO(getblob("final_topic_model.pkl")))
    #### End ####

    # 4.  Instantiate a count vectorizer with cleaned text
    # The vectorizer object will:
        # a. Clean, tokenize and lemmatize text using custom function: LemmaTokenizer()
        # b. Remove stopwords
        # c. Use dic as vocabulary
    vectorizer = CountVectorizer(vocabulary=dic, 
                                tokenizer=LemmaTokenizer(), 
                                min_df=5, max_df=0.9,
                                lowercase=True,
                                #token_pattern=r'\b[a-zåäöA-ZÅÄÖ\-]{3,}\b', 
                                stop_words=stop_words)

    # 5. Transform text-data from cleaned, tokenized and lemmatized list of speeches to a sparse dtm-matrix
    tf = vectorizer.transform(c_list)

    # 6. Apply the fitted LDA-model to transformed data
    lda_output = lda_model.transform(tf)

    # 7. Infere topics and establish dominant topic
    # column names
    topicnames = ['Topic' + str(i) for i in range(len(lda_model.components_))]
    # index names
    docnames = ['Doc' + str(i) for i in range(len(c_list))]
    # Make the pandas dataframe
    df_document_topic = pd.DataFrame(np.round(lda_output, 2), columns=topicnames, index=id_list)
    # Get dominant topic for each document
    dominant_topic = np.argmax(df_document_topic.values, axis=1)
    df_document_topic['dominant_topic'] = dominant_topic

    # 8. Update each doc in mongo
    for doc in df_document_topic.itertuples():
        mycol.update_one({'_id':doc[0] },                       # doc[0]='_id'
                            {'$set': {'topic_num': doc[-1] }    # doc[-1]='dominant_topic'
                                    }, 
                            upsert=False )

    # 9. Set human interpretation of kewords from topics
    # But keep in mind: Changing the topic name requires us to do so on all records without any retriction.

    mycol.update_many({'topic_num':0,'topic':{'$exists':False} }, {'$set': {'topic':'Infrastructure, long term investments, jobs, regional policy'}            }, upsert=False)
    mycol.update_many({'topic_num':1,'topic':{'$exists':False} }, {'$set': {'topic':'Internal affairs, Police, Criminality'}                                   }, upsert=False)
    mycol.update_many({'topic_num':2,'topic':{'$exists':False} }, {'$set': {'topic':'Health care system, Geriatric care'}                                      }, upsert=False)
    mycol.update_many({'topic_num':3,'topic':{'$exists':False} }, {'$set': {'topic':'Policy for rural areas, Culture and sports'}                              }, upsert=False)
    mycol.update_many({'topic_num':4,'topic':{'$exists':False} }, {'$set': {'topic':'Foreign policy, EU, Defence policy'}                                      }, upsert=False)
    mycol.update_many({'topic_num':5,'topic':{'$exists':False} }, {'$set': {'topic':'Economical affairs, Business, Energy, Welfare, Growth, Fiscal Budget'}    }, upsert=False)
    mycol.update_many({'topic_num':6,'topic':{'$exists':False} }, {'$set': {'topic':'Labour market, Social policy and insurance'}                              }, upsert=False)
    mycol.update_many({'topic_num':7,'topic':{'$exists':False} }, {'$set': {'topic':'Education, Higher education, Housing policy'}                             }, upsert=False)
    mycol.update_many({'topic_num':8,'topic':{'$exists':False} }, {'$set': {'topic':'Internal protocol, General political debate, Parliamentary politics'}     }, upsert=False)
    mycol.update_many({'topic_num':9,'topic':{'$exists':False} }, {'$set': {'topic':'Legislation, Formal administration and protocol, Investigations'}         }, upsert=False)    

def add_missing_sentiments_to_db():
    # Ladda in sentimentanalyseraren
    analyzer = SentimentIntensityAnalyzer()

    ### Utför beräkningar per anförande (Python)

    # 1. Välj ut de anföranden som är nya genom att leta rätt på de som saknar exempelvis negativ sentiment score
    # 2. Applicera sentimentanalysen
    # 3. Spara tillbaka till MongoDB

    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    ### 1. Väljer ut de anföranden som saknar topic eller sentiment
    criteria={'sent_com':{'$exists': False} } 
    cursor=mycol.find(criteria,{'anforandetext':1} )

    for anf in cursor:    
            if anf['anforandetext'] is not None: # Tar bara icke-tomma anföranden
                if len(anf['anforandetext'])>20: # Tar bara anföranden längre än 20 tecken
                    elem = format_text(anf['anforandetext'])      
        
                    ### 2. Sentiment ###
                    vs = analyzer.polarity_scores(elem)
                                
                    ### 3. Push to MongoDB ###
                    mycol.update_one({'_id':anf['_id']}, 
                                            {'$set': {'sent_neg': vs['neg'],
                                                    'sent_neu': vs['neu'],
                                                    'sent_pos': vs['pos'],
                                                    'sent_com': vs['compound']}
                                                    }, 
                                            upsert=False )

def add_missing_applause_to_db():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    
    # Choose correct criteria_app! Choose empty if we need to rewrite everything. Choose the other when in functioning production

    criteria_app={'applause':{'$exists':False} } 
    #criteria_app={}
    
    pipeline=[
        {'$set':{
            'applause':{    # Each match is an element in the array, thus number of elements = number of matches
                '$regexFindAll':{
                    'input':'$anforandetext', 'regex': '\(Applåder\)'}
                                }
                            }
                    },
        {'$set':{
            'applause':{ # Convert 'applause' to number of applauses by counting the elements
                '$size':'$applause'} 
                    } 
                }
    ]

    mycol.update_many(criteria_app,pipeline)

def add_missing_entities_to_db():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    aut_col = mydb["authorities"]

    # List of authorities
    authorities = aut_col.find({})

    # Tag records with null 'authorities', i.e not only [] but missing completely
    mycol.update_many({'authorities':{'$exists':False}}, {'$set': {'aut_missing':1 } } )
    log(f"Searching through {mycol.count_documents({'aut_missing':1} ) } documents")

    # Only perform if any records have missing authorities array
    if mycol.count_documents({'aut_missing':1})>0:
        for aut in authorities:
            criteria_f = {'aut_missing':1, 'anforandetext': {'$regex': aut['Söksträng']} }
            
            # Unmask and use instead of criteria_f if new authorities has been added to the collection with authorities
            # criteria_addition = {'anforandetext': {'$regex': aut['Söksträng']} }

            x = mycol.update_many(criteria_f,{'$addToSet': {'authorities': aut['Entitet'] } } )

        # Special addition for Konjunkturinstitutet (KI). In order to discriminate between when the common and shared acronym 'KI' is used to refer to Konjunkturinstitutet and not to Karolinska Institutet. To do this we use the topics 0,5,6,8. These topics have no overlap but e.g. 9 has some even though it seems reasonable to include.
        ki_string=' KI(-|\s)'
        criteria_f = {'aut_missing':1,'topic_num':{'$in':[0,5,6,8]}, 'anforandetext': {'$regex': ki_string} }
        mycol.update_many(criteria_f,{'$addToSet': {'authorities': 'Konjunkturinstitutet' } } )

        # Add empty array for scanned documents (for easy inspection)
        mycol.update_many({'authorities':{'$exists':False}}, {'$set': {'authorities':[] } } )
        # Tag records that has been scanned for authorities
        mycol.update_many({}, {'$unset': {'aut_missing':1 } } )
    else: 
        log('No documents missing entities') 

def wc_stop():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["stopwords"]
    list_of_dict=list(mycol.find({}))
    stopwords_set=set(list_of_dict[0]['stopwords'])
    stopwords_set.update(['ledamöter','ledamöter','fru','talman','det','är','regeringens','se','ju','styleref kantrubrik','kantrubrik mergeformat','samtidigt','trots','tillbaka','dessutom','låt','em'])   
    return stopwords_set 

def make_wc_png():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]

    #Lista anföranden
    criteria={ "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }   }
    cursor=mycol.find(sort=[("dok_datum", pymongo.DESCENDING),("anforande_nummer",pymongo.DESCENDING)],filter=criteria,projection={'anforandetext':1},limit=1000)

    c_list = list(cursor)
    text = " ".join(format_text(str(anf['anforandetext']).lower()) for anf in c_list)
    
    stopwords_set=wc_stop()
    # Create and generate a word cloud image:
    wordcloud = WordCloud(stopwords=stopwords_set, background_color='white', width=1600, height=800, collocations=False).generate(text)

    return wordcloud.to_image()

def add_wc_to_mongo():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol_bilder = mydb["bilder"]

    bild=make_wc_png()

    byteIO = BytesIO()
    bild.save(byteIO, format='PNG')
    byteArr = byteIO.getvalue()

    page_info_dict = {
        "Senaste datum": get_last_date_from_db(),
        "namn":'WC',
        "bild": byteArr}

    mycol_bilder.update_one({'namne':'WC'},{'$set':page_info_dict}, upsert=True)

def save_to_mongo(documents):
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    x = mycol.insert_many(documents)
    return x.inserted_ids

def get_last_date_from_db():
    myclient = pymongo.MongoClient(get_connstring())
    mydb = myclient["rod"]
    mycol = mydb["anforanden"]
    cursor = mycol.find().sort("dok_datum",pymongo.DESCENDING).limit(1)
    return cursor[0]["dok_datum"]

def start_import():

    try:
        #Get JSON from Riksdagen for all speeches since last entry in DB. If the database is empty the API will time out..
        last_date = get_last_date_from_db()
        log(f"Looking for new anfs since {last_date}")
        rod_json = get_anfs_from_rod(last_date)
        log(f"Got new anfs from ROD since {last_date}")
        #Populate every speech/document with the corresponding transcrip (different API...)
        log("Populating new anfs with text..")
        document_list = populate_anfs_with_text(rod_json)
        log("Populated new anfs with text..")
        log("Formatting dates as dates..")
        document_list = format_dates_in_anfs(document_list)
        log("Formatted dates as dates..")
        log("Formatting parties correctly..")
        document_list = format_parties_in_anfs(document_list)
        log("Formatted parties correctly..")

        #Save all documents to database
        log("Saving new anfs to database")
        save_to_mongo(document_list)
        log("Saved new anfs to database..")
    except KeyError:
        log("No new anfs from ROD or broken API?")
        log("Exiting")
        return
    except Exception as ex:
        logging.info(f"{datetime.datetime.now().isoformat()} : {(sys.exc_info()[0])}")
        #report error to some magic azure service..
        log(f"Error: {ex}")
        #return


    try:
        #do stuff after saved to mongo..
        #Populate every speech/document with entities, sentiment, topics, tags, applause
        log("Pre-downloading nltk.punkt")
        nltk.download("punkt")
        log("download done")
        log("Adding missing sentiments to database..")
        add_missing_sentiments_to_db()
        log("Added missing sentiments to database..")
        log("Adding missing topics to database..")
        add_missing_topics_to_db()
        log("Added missing topics to database..")
        log("Adding missing applause to database..")
        add_missing_applause_to_db()
        log("Added missing applause to database..")
        log("Adding missing entities to database..")
        add_missing_entities_to_db()
        log("Added missing entities to database..")
        log("Refreshing WC picture...")
        add_wc_to_mongo()
        log("Done with WC picture!")

    except Exception as ex:
        logging.info(f"{datetime.datetime.now().isoformat()} : {(sys.exc_info()[0])}")
        #report error to some magic azure service..
        log(f"Error: {ex}")

        return

def main(mytimer: func.TimerRequest) -> None:
    log("Import started")
    start_import()
    log("Import ended")