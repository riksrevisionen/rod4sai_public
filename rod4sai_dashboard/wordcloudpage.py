import matplotlib.pyplot as plt
import datetime

import datareader as dr
from rod_utils import formatera_text
import streamlit as st
from wordcloud import WordCloud


dt = datetime.datetime.today()

@st.cache()
def getdata(size,authority='_Any_speech',topic='_Any_topic',end=dt, start=dt-datetime.timedelta(days=365)):
    #Lista anföranden
    if (authority=='_Any_speech' and topic=='_Any_topic'):
        criteria={"dok_datum": {'$gte': start, '$lte': end},
            "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] } }
    elif (authority!='_Any_speech' and topic=='_Any_topic'):
        criteria={ "dok_datum": {'$gte': start, '$lte': end},
        "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }, 
        "authorities":authority
        }
    elif (authority=='_Any_speech' and topic!='_Any_topic'):
        criteria={ "dok_datum": {'$gte': start, '$lte': end},
        "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }, 
        "topic":topic
        }
    elif (authority!='_Any_speech' and authority!='_Any_topic'):
        criteria={ "dok_datum": {'$gte': start, '$lte': end},
        "parti": {"$in": ["S","M", "SD", "L","C","KD","V","MP"] }, 
        "authorities":authority,
        "topic":topic
        }

    cursor = dr.mongo_collection().aggregate([
    {'$match':criteria},
    {'$sample':{'size':size}},
    {'$project':{'anforandetext':1,'_id':0} }
    ])            

    total_request=dr.mongo_count(filter=criteria)

    c_list = list(cursor)
    no_speech=len(c_list)
    text = " ".join(formatera_text(str(anf['anforandetext']).lower()) for anf in c_list)
    
    return text, no_speech, total_request

def showpage():

    st.title("WordCloud")
    st.markdown("Top word frequencies in word cloud")

    # Date filter
    today = datetime.date.today()
    four_months_ago = today - datetime.timedelta(days=180)
    start = st.sidebar.date_input('Start date', four_months_ago)
    end = st.sidebar.date_input('End date', today)

    start=datetime.datetime.combine(start, datetime.time.min) # Convert to datetime
    end=datetime.datetime.combine(end, datetime.time.min) # Convert to datetime

    # Authority
    list_auth=dr.get_entities()
    list_auth.append('_Any_speech')
    authority = st.sidebar.selectbox(f"Select among {len(list_auth)-1} authorities",list_auth,index=(len(list_auth)-1))

    # Topic
    list_topics=dr.mongo_collection().distinct('topic')
    list_topics.append('_Any_topic')
    topic = st.sidebar.selectbox(f"Select among {len(list_topics)-1} topics",list_topics,index=(len(list_topics)-1))

    # Sample
    y_min=2000
    y_max=10000
    sample=st.sidebar.slider(label="Maximum size of sample", value=y_min, step=1000, max_value=y_max)

    if end < start:
        st.error("End date has got to be larger than the start date.")
    else:        
        # Get data
        text, no_speech, total_request =getdata(sample,authority,topic,end,start)

        st.markdown(f"Returned {no_speech:,.0f} speeches out of {total_request:,.0f} requested.")

        if no_speech>19:

            # Display the generated image:
            stopwords_set=dr.wc_stop()
            # Create and generate a word cloud image:
            with st.spinner('Generating WordCloud'):
                wordcloud = WordCloud(stopwords=stopwords_set, background_color='white', width=1600, height=800, collocations=False).generate(text)
                fig = plt.figure( figsize=(20,10), facecolor='k' )
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis("off")
                plt.tight_layout(pad=0)
            st.pyplot(fig)
        else:
            st.error("Your query resulted in less than 20 speeches. This Wordcloud algorithm needs at least 20 speeches. Adjust your query accordingly.")