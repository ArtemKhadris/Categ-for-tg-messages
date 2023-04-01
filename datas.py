import time
TIME1 = time.perf_counter()

import re
import pandas as pd
import numpy as np
import math
import json
from urllib.parse import unquote, urlparse
import pymorphy2
from pymorphy2 import tokenizers
morph = pymorphy2.MorphAnalyzer()


#full table display
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 3000)


#opening js file, dont forget to uncomment and change directory
"""
df = pd.read_json('C:\\Users\\Lenovo\\Desktop\\fl\\2\\dark_classified_messages.jl', lines=True)

other_cat = df[df.label == 'other']

other_cat.to_json('C:\\Users\\Lenovo\\Desktop\\fl\\2\\dark_classified_messages_other.jl', force_ascii=False, orient='records', lines=True)


#mf_cat = df[df.category == 'money_fraud']
#db_cat = df[df.category == 'database']

"""

#creating text files with the most frequent words in categories
#dont forget to change directory
def init_keywords():
    df = pd.read_csv('C:\\Users\\Lenovo\\Desktop\\fl\\2\\dark_msgs_w_kwrds1.csv')
    wd = df.keywords.unique()
    wd1=[]
    for words in range (len(wd)-1):
        tokens = tokenizers.simple_word_tokenize(str(wd[words]))
        for word in tokens:
            if len(word) > 3:
                wd1.append(word)

    counter_w = {}
    for words in wd1:
        counter_w[words] = counter_w.get(words, 0) + 1
    doubles = {element: count for element, count in counter_w.items() if count > 0}
    wd = dict(sorted(doubles.items(), reverse = True, key=lambda x: x[1]))


    with open('C:\\Users\\Lenovo\\Desktop\\fl\\2\\keywords.txt','w',encoding='utf-8') as out:
        for key,val in wd.items():
            out.write('{}:{}\n'.format(key,val))
    
    return 0

#assigning a new category
#dont forget to change directory
def cat_w_key_words_fraud():
    with open('C:\\Users\\Lenovo\\Desktop\\fl\\2\\keywords_fraud.txt','r',encoding='utf-8') as f:
        top_key_words = f.read().splitlines()
    key_words = []
    for word in top_key_words:
        res = word.split(':', 1)[0]
        key_words.append(res)

    df = pd.read_csv('C:\\Users\\Lenovo\\Desktop\\fl\\2\\dark_msgs_w_kwrds1.csv')
    
    with open('C:\\Users\\Lenovo\\Desktop\\fl\\2\\outfile_fraud.csv', 'w'):
        pass

    df = df.reindex(df.columns.to_list(), axis=1)
    quant_str = 1
    count = 0
    start = int(time.time())
    other_cat1 = df.head(0)
    other_cat1.to_csv('./outfile.csv', mode = 'a', index = False)
    for strings in range (1, len(df), quant_str):
        count += 1
        if count % 1000 == 0:
                now = int(time.time())
                print(count, now - start)
                start = int(time.time())
        other_cat_cycle = pd.concat([df.head(0), df[strings:strings+quant_str]], ignore_index = True)
        
        txt=[]
        for a in other_cat_cycle.keywords:
            token_ttl = tokenizers.simple_word_tokenize(str(a))
            txt.append(token_ttl)
        
        new_txt=[]
        for words in txt:
            for word in words:
                if len(word) > 1:
                    new_txt.append(word)
        '''
        txt1 = []
        for a in txt:
            for j in a:
                txt1.append(j.lower())
        txt2=[]
        for a in txt1:
            a = re.sub("</?.*?>"," <> ", a)
            a = re.sub("(\\d|\\W)+", " " , a)
            for j in a:
                for k in a:
                    if j in 'qwertyuiopasdfghjklzxcvbnm -' and k in "йцукенгшщзхъфывапролджэячсмитьбю":
                        a = a.replace(j, '')
            if len(a)>3:
                txt2.append(a)
        txt3 = []
        for a in txt2:
            txt3.append(morph.parse(a)[0].normal_form)
        '''
        
        flag = True
        for word in new_txt:
            if word in key_words:
                other_cat_cycle.to_csv('C:\\Users\\Lenovo\\Desktop\\fl\\2\\outfile_fraud.csv', mode = 'a', index = False, header = False)
                flag = False
            if flag == False:
                break
        
    return 0

#assigning a new category
#dont forget to change directory
def cat_w_key_words_leaks():
    with open('C:\\Users\\Lenovo\\Desktop\\fl\\2\\keywords_leaks.txt','r',encoding='utf-8') as f:
        top_key_words = f.read().splitlines()
    key_words = []
    for word in top_key_words:
        res = word.split(':', 1)[0]
        key_words.append(res)

    df = pd.read_csv('C:\\Users\\Lenovo\\Desktop\\fl\\2\\dark_msgs_w_kwrds1.csv')
    
    with open('C:\\Users\\Lenovo\\Desktop\\fl\\2\\outfile_leaks.csv', 'w'):
        pass

    df = df.reindex(df.columns.to_list(), axis=1)
    quant_str = 1
    count = 0
    start = int(time.time())
    other_cat1 = df.head(0)
    other_cat1.to_csv('./outfile.csv', mode = 'a', index = False)
    for strings in range (1, len(df), quant_str):
        count += 1
        if count % 1000 == 0:
                now = int(time.time())
                print(count, now - start)
                start = int(time.time())
        other_cat_cycle = pd.concat([df.head(0), df[strings:strings+quant_str]], ignore_index = True)
        
        txt=[]
        for a in other_cat_cycle.keywords:
            token_ttl = tokenizers.simple_word_tokenize(str(a))
            txt.append(token_ttl)
        
        new_txt=[]
        for words in txt:
            for word in words:
                if len(word) > 1:
                    new_txt.append(word)
        '''
        txt1 = []
        for a in txt:
            for j in a:
                txt1.append(j.lower())
        txt2=[]
        for a in txt1:
            a = re.sub("</?.*?>"," <> ", a)
            a = re.sub("(\\d|\\W)+", " " , a)
            for j in a:
                for k in a:
                    if j in 'qwertyuiopasdfghjklzxcvbnm -' and k in "йцукенгшщзхъфывапролджэячсмитьбю":
                        a = a.replace(j, '')
            if len(a)>3:
                txt2.append(a)
        txt3 = []
        for a in txt2:
            txt3.append(morph.parse(a)[0].normal_form)
        '''
        
        flag = True
        for word in new_txt:
            if word in key_words:
                other_cat_cycle.to_csv('C:\\Users\\Lenovo\\Desktop\\fl\\2\\outfile_leaks.csv', mode = 'a', index = False, header = False)
                flag = False
            if flag == False:
                break
        
    return 0


#dond forget to uncommment function calling
#init_keywords()
#cat_w_key_words_fraud()
#cat_w_key_words_leaks()


TIME2 = time.perf_counter()
print('\n\n', round((TIME2 - TIME1), 3), ' seconds', sep = '')