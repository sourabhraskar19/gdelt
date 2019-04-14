#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 28 11:37:35 2019

@author: iquantela
"""

import numpy as np
import pandas as pd
import gdelt as gd
import re
import pytz
import datetime
from afinn import Afinn
import platform
import requests
from newspaper import Article
from bs4 import BeautifulSoup
from readability.readability import Document as Paper
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
from nltk import word_tokenize          
from nltk.stem import WordNetLemmatizer,PorterStemmer,SnowballStemmer
import pickle,os,json,random,nltk,re,io
affn=Afinn(emoticons=True)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
done = {}

def get_data(date=['2019 Feb 22','2019 Feb 27'], converge=True, normcols=True):
    c=gd.gdelt()
    return c.Search(date)
    
def get_gnisid(place="Pune"):
    gnis=pd.read_csv("gnis_data.csv")
    gnisid=[]
    df=gnis[gnis.location.str.contains(place,case=False, regex=True)]
    if (df.shape[0] < 5):
        gnisid.append(gnis[gnis.location.str.contains(place,case=False, regex=True)]['featureid'].iloc[0])
    else:
        for i in range(df.shape[0]):
            gnisid.append(gnis[gnis.location.str.contains(place,case=False, regex=True)]['featureid'].iloc[i])
    return gnisid

def get_locationwise_data1(gnisid,data):
    df=pd.DataFrame()
    for i in range(len(gnisid)):
    	if (len(data[data.ActionGeo_FeatureID==gnisid[i]]) != 0):
            df=df.append(data[data.ActionGeo_FeatureID==gnisid[i]])
    return df

def get_locationwise_data(gnisid,data):
    if (len(gnisid) < 2):
        return(data[data.ActionGeo_FeatureID==gnisid[0]])
    else:
        df=pd.DataFrame()
        for i in range(len(gnisid)):
            if (len(data[data.ActionGeo_FeatureID==gnisid[i]]) != 0):
                df=df.append(data[data.ActionGeo_FeatureID==gnisid[i]])
        return df
        
def get_data_countrywise(countryname,date):
    countryid=pd.read_csv("countrycode.csv")
    #countrycode=
    c=gd.gdelt()
    data=c.Search(date=date)
    #0print(countryid[countryid.ContryName.str.contains(countryname,case=False,regex=True)].CountryCode.values[0])
    return data[data['ActionGeo_CountryCode']==countryid[countryid.ContryName.str.contains(countryname,case=False,regex=True)].CountryCode.values[0]]

def get_text(url):
    global done
    TAGS = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'p', 'li']
    s = re.compile('(http://|https://)([A-Za-z0-9_\.-]+)')
    u = re.compile("(http://|https://)(www.)?(.*)(\.[A-Za-z0-9]{1,4})$")
    if s.search(url):
        site = u.search(s.search(url).group()).group(3)
    else:
        site = None
    answer = {}
    if s.search(url):
        if url in done.keys():
            yield done[url]
            pass
        try:
            r = requests.get(url, headers = {'User-Agent': 'FooBar-Spider 1.0'})
        except:
            done[url] = "Unable to reach website."
            answer['author'] = None
            answer['base'] = s.search(url).group()
            answer['provider']=site
            answer['published_date']=None
            answer['text'] = "Unable to reach website."
            answer['title'] = None
            answer['top_image'] = None
            answer['url'] = url
            answer['keywords']=None
            answer['summary']=None
            yield answer
        if r.status_code != 200:
            done[url] = "Unable to reach website."
            answer['author'] = None
            answer['base'] = s.search(url).group()
            answer['provider']=site
            answer['published_date']=None
            answer['text'] = "Unable to reach website."
            answer['title'] = None
            answer['top_image'] = None
            answer['url'] = url
            answer['keywords']=None
            answer['summary']=None

        if len(r.content)>500:
            article = Article(url)
            if int(platform.python_version_tuple()[0])==3:
                article.download(input_html=r.content)
            elif int(platform.python_version_tuple()[0])==2:
                article.download(html=r.content)
            article.parse()
            article.nlp()
            if len(article.text) >= 200:
                answer['author'] = ", ".join(article.authors)
                answer['base'] = s.search(url).group()
                answer['provider']=site
                answer['published_date'] = article.publish_date
                #answer['keywords']=article.keywords
                answer['summary']=article.summary
                if isinstance(article.publish_date,datetime.datetime):
                    try:
                        answer['published_date']=article.publish_date.astimezone(pytz.utc).isoformat()
                    except:
                        answer['published_date']=article.publish_date.isoformat()
                

                answer['text'] = article.text
                answer['title'] = article.title
                answer['top_image'] = article.top_image
                answer['url'] = url
             
            else:
                doc = Paper(r.content)
                data = doc.summary()
                title = doc.title()
                soup = BeautifulSoup(data, 'lxml')
                newstext = " ".join([l.text for l in soup.find_all(TAGS)])


                if len(newstext) > 200:
                    answer['author'] = None
                    answer['base'] = s.search(url).group()
                    answer['provider']=site
                    answer['published_date']=None
                    answer['text'] = newstext
                    answer['title'] = title
                    answer['top_image'] = None
                    answer['url'] = url
                    answer['keywords']=None
                    answer['summary']=None
      
                else:
                    newstext = " ".join([
                        l.text
                        for l in soup.find_all(
                            'div', class_='field-item even')
                    ])
                    done[url] = newstext
                    answer['author'] = None
                    answer['base'] = s.search(url).group()
                    answer['provider']=site
                    answer['published_date']=None
                    answer['text'] = newstext
                    answer['title'] = title
                    answer['top_image'] = None
                    answer['url'] = url
                    answer['keywords']=None
                    answer['summary']=None

        else:
            answer['author'] = None
            answer['base'] = s.search(url).group()
            answer['provider']=site
            answer['published_date']=None
            answer['text'] = 'No text returned'
            answer['title'] = None
            answer['top_image'] = None
            answer['url'] = url
            answer['keywords']=None
            answer['summary']=None
            yield answer
        yield answer

    else:
        answer['author'] = None
        answer['base'] = s.search(url).group()
        answer['provider']=site
        answer['published_date']=None
        answer['text'] = 'This is not a proper url'
        answer['title'] = None
        answer['top_image'] = None
        answer['url'] = url
        answer['keywords']=None
        answer['summary']=None
    yield answer
    
def get_news_summary_keywords(data):
    vect = np.vectorize(get_text)
    cc = vect(data['SOURCEURL'].values)
    q=list(next(l) for  l in cc)
    #print(q)
    return pd.DataFrame(q,columns=['author', 'summary', 'provider', 'title', 'base', 'url', 'published_date', 'top_image', 'text'])

def get_sentiment_affn(text):
    """
    return vector of sentiments
    text : english text for which sentiment have to be defined
    """
        # tokenizing into sentences
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    tokenized_text=tokenizer.tokenize(text)
    sentiments_vec=[affn.score(i) for i in tokenized_text]
    
    return sentiments_vec

filter_positive=lambda x: [i for i in x if i>0]
filter_positive.__name__='filter_positive'

filter_negative=lambda x: [i for i in x if i<0]
filter_negative.__name__='filter_negative'

filter_neutral=lambda x: [i for i in x if i==0]
filter_neutral.__name__='filter_neutral'

def get_sentiment_stat(text,method='affin'):
    """
    return statistics of sentiment 
    text : english text for which sentiment have to be defined
    method : affin or dnn
    """
    if method=='affin':
        sentiments_list=get_sentiment_affn(text)
    else:
        print("error in correct input")
        return 
    #sentiments_posts.append(sentiments_list)
    #print(summarize(post['text']))
    #print("overall sentiment median(+ve frequency) = "+str(np.median(sentiments_list)),\
            #"overall sentiment mean(average) = "+str(np.mean(sentiments_list)),\
            #"overall sentiment sum(Strongess) = "+str(np.sum(sentiments_list)))
    #display(text2sentiment_heatmap(post['text'],sentiments_list))
    #time.sleep(30)
    return {"overall sentiment median(+ve frequency)" : np.median(sentiments_list),
            "overall sentiment mean(average)":np.mean(sentiments_list),
            "overall sentiment sum(Strongess)":np.sum(sentiments_list),
            "sentiments_list":sentiments_list,
            "total_positive_sentiment_text":len(filter_positive(sentiments_list)),
            "total_negative_sentiment_text":len(filter_negative(sentiments_list)),
            "total_neutral_sentiment_text":len(filter_neutral(sentiments_list))
           }


def get_sentiment_blog(op_all):
    """
	develop sentiment analytics in webhose data 
	op_all : output dataframe from webhose
    keywords : keywords on which search has been made
    source : blogs/news/discussions
    take_all_data : if you want to use all data
	"""
    sentiments_stat=op_all['text'].apply(lambda x: get_sentiment_stat(x))
    op_all['published']=pd.DatetimeIndex(op_all['published_date'])
    op_all['stats']=pd.DataFrame(sentiments_stat)   
    op_all['median_sentiment_level']=op_all['stats'].apply(lambda x :x['overall sentiment median(+ve frequency)'])
    op_all['date']=op_all['published'].apply(lambda x: x.date())
    return op_all

def sentiment_tag(sentiment_val):
    """
    sentiment_val : returns sentiment_tag based on sentiment value
    """
    if sentiment_val<0:
        return 'Negative'
    elif sentiment_val>0:
        return 'Positive'
    else:
        return 'Neutral'

def get_sentiment_news(news,gdelt):
    data_sentiment=get_sentiment_blog(news)
    data_sentiment['lat']=gdelt.ActionGeo_Lat.reset_index().drop(columns=["index"])
    data_sentiment['lon']=gdelt.ActionGeo_Long.reset_index().drop(columns=["index"])
    data_sentiment['sentiment_tag']=data_sentiment['median_sentiment_level'].apply(lambda x:sentiment_tag(x))
    return data_sentiment.drop(columns=['author','summary','base', 'url','published_date', 'top_image','text', 'published', 'stats','median_sentiment_level', 'date']).drop_duplicates()



    

    
    





