#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 22:43:06 2020

@author: lenakim
"""

import os
import json
import pandas as pd
import numpy as np
import sys
import requests


def remove_punct (string):
    punc_list = ["!","\"", "\#", "$", "%", "&", "\\", "\'", "(", ")","*", "+", ",", "-", ".", "#",
                 "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~"]
    new = ''
    if(type(string) == str):
        string = string.strip()
        for s in string:
            if s in punc_list:
                new += ' '
            else:
                new += s
    else:
        new = string
    return new

def clean_words(words):
    words = remove_punct(words)
    words = words.strip().lower().split()
    return words

def get_database (firebase, table_names):
    database = {}
    for name in table_names:
        database[name] = json.loads(requests.get(firebase + name + '.json').json())
    return database
        


def search_key(word):
    words = clean_words(word)
    for w in words:
        for key, lists in index.items():
            if(w == key.lower()):
                occur = []
                count = {}
                tables = set()
                for dic in lists: 
                    table = dic['table']
                    tables.update(table.split())
                    pk = dic['primary_key']
                    occur.append({table:pk})
                    
                    if (pk in count.keys()):
                        count[pk] += 1
                    else:
                        count[pk] = 1
                    sorted_key = sorted(count.items(), key = lambda kv: kv[1], reverse = True)
                    sorted_key = [element[0] for element in sorted_key]
                
                result = {}
                for i in list(tables):
                    result[i] = []
                
                for o in occur:
                    for k, v in o.items():
                        result[k].append({v : 1})
                
                for tb, lis in result.items():
                    new_count = {}
                    for d in lis:
                        for pk, count in d.items():
                            
                            if pk in new_count.keys():
                                new_count[pk] += 1
                            else:
                                new_count[pk] = 1
                            sorted_key = sorted(new_count.items(), key = lambda kv: kv[1], reverse = True)
                            sorted_key = [element[0] for element in sorted_key]
                    result[tb] = sorted_key
    return result
 

        
firebase = 'https://inf551-bb52b.firebaseio.com/'
database = get_database(firebase, ['city', 'country', 'countryLanguage', 'index'])
index = database['index']
del database['index']                       

search_key("north")
                  
           
                
                        
                    
            
        
