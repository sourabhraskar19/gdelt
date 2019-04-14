#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 14:29:36 2019

@author: iquantela
"""

import numpy as np
import pandas as pd
import gdelt as gd
import re
import pytz
import datetime
import platform
import requests
from newspaper import Article
from bs4 import BeautifulSoup
from readability.readability import Document as Paper
from requests.packages.urllib3.exceptions import InsecureRequestWarning


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

c=gd.gdelt()


#data=c.Search(
 #               date=['2019 Feb 10','2019 Feb 15'],
  #                coverage=True,
   #               normcols=True)

gnis=pd.read_csv("/home/iquantela/gnis_data.csv")
done = {}

def filter_location(place,data):
    gnisid=get_gnisid(place,gnis)
   # s=gnisid[gnis.location.str.contains(place,case=False, regex=True)]['featureid'].iloc[0]
    return (data[data.actiongeofeatureid==gnisid.iloc[0]])

def get_gnisid(place, gnis):
    s=gnis[gnis.location.str.contains(place,case=False, regex=True)]['featureid']
    return (s)



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
                answer['keywords']=article.keywords
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


#vect = np.vectorize(get_text)
#cc = vect(data['SOURCEURL'].values)
#dd = list(next(l) for  l in cc)
