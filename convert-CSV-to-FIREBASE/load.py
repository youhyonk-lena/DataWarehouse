#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 23:14:58 2020

@author: lenakim
"""


import json
import requests

def load_csv (files):
    db = {}
    for file in files:
        data = []  
        with open(file, 'r', encoding = 'latin -1') as f:
            keys = f.readline().split(",")
            keys = [clean(x) for x in keys]
            for lines in f:
                value = lines.split(",")
                dictionary = {}
                for i in range(0, len(value)):
                    try:
                        dictionary[keys[i]] = clean(value[i])
                    except:
                        dictionary[''] = clean(value[i])
                      
                data.append(dictionary)
            name = file.split(".")[0]
            db[name] = data
    return db

def clean (item):
    item = item.strip().replace("'", '')
    item = num_to_num (item)
    item = remove_punct(item)
    return item

def num_to_num (item):
    try:
        if("." in item):
            item = float(item)
        else:
            item = int(item)
    except:
        pass
    return item

def remove_punct (string):
    punc_list = ["!","\"", "\#", "$", "%", "&", "\\", "\'", "(", ")","*", "+", ",", "-", ".",
                 "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~"]
    new = ''
    if(type(string) == str):
        string = string.strip()
        for s in string:
            if s in punc_list:
                new += ' '
            else:
                new += s
        new = new.strip()
    else:
        new = string
    return new

def to_firebase (data, firebase, node = None): 
    for key in db.keys():
        url = firebase + key + '.json'    
        requests.put(url, json.dumps(db[key]))
   

def unique_words(db):
    words = set()
    for table, lists in db.items():
        for dic in lists:
            for key, value in dic.items():
                if (type(value) == str and len(value) > 0):
                    words.update(value.split())
    return words

def get_index(db, words):
    index = {}
    for word in words:
        where = []
        for table, lists in db.items():
            primary_key = None
            if (table == 'city'):
                primary_key = '# ID'
            elif (table == 'countryLanguage'):
                primary_key = '# CountryCode'
            else:
                primary_key = '# Code'
            
            for dic in lists:
                for key, value in dic.items():
                    if (type(value) == str and len(value) > 0):
                        if (word in value):
                            details = {'table': table, 'column' : key, 'primary_key': dic[primary_key]}
                            where.append(details)
                    else:
                        pass
        index[word] = where        
    return index
        
    
firebase = 'https://inf551-bb52b.firebaseio.com/'  
db = load_csv(["city.csv", "countryLanguage.csv", "country.csv"])
to_firebase(db, firebase)
words = unique_words(db)
index = get_index(db, words)
requests.put(firebase + 'index.json', json = json.dumps(index))


