import seaborn as sns
import streamlit as st
from rod_utils import formatera_text_pandas
import datareader as dr

riksmote='2018/19'

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
    
    #riksmote_list=[]
    #for i in range(2019,2003,-1):
    #    riksmote_list.append(f"{str(i)}/{str(i+1)[2:]}")
    #riksmote_tuple = tuple(riksmote_list)
    #riksmote = st.sidebar.multiselect("Parliament year :",riksmote_tuple, default=['2019/20'])
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

    g = sns.FacetGrid(df, #the dataframe to pull from
                  row="parti", #define the column for each subplot row to be differentiated by
                  hue="parti", #define the column for each subplot color to be differentiated by
                  aspect=10, #aspect * height = width
                  height=1.5, #height of each subplot
                  palette=['#d62728','#1f77b4', '#9467bd','#aec7e8', '#ffd92f','#ff9896','#2ca02c','#98df8a'] #färger
                 )

    #shade: True/False, shade area under curve or not
    #alpha: transparency, lw: line width, bw: kernel shape specification

    g.map(sns.kdeplot, "sent_com", shade=True, alpha=1, lw=1.5, bw=0.2)
    g.map(sns.kdeplot, "sent_com", lw=4, bw=0.2)
    g.map(plt.axhline, y=0, lw=4)
    
    def label(x, color, label):
        ax = plt.gca() #get the axes of the current object
        ax.text(0, .2, #location of text
                label, #text label
                fontweight="bold", color=color, size=20, #text attributes
                ha="left", va="center", #alignment specifications
                transform=ax.transAxes) #specify axes of transformation

    g.map(label, "sent_com") #the function counts as a plotting object!
    #prevent overlapping issues by 'removing' axis face color
    sns.set(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
    g.fig.subplots_adjust(hspace= -.05)
    g.set_titles("") #set title to blank
    g.set(yticks=[]) #set y ticks to blank
    g.despine(bottom=True, left=True) #remove 'spines'
    st.pyplot(g)