#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 00:33:31 2020

@author: lenakim
"""

import os
import sys
import re
from lxml import etree
import json

#os.chdir("/Users/lenakim/Documents/USC/2020_Spring/INF_551/hw2")

def open_file (path):
    with open(path) as f:
        tree = etree.parse(f)
    return tree

def clean_query (query):
    query = query.split(".")[0]
    query = re.sub('-', ' ', query)
    query = query.lower()
    return query.split()


def get_possible_inum (index_path, query):
    
    index = open_file(index_path)
    candidates = set()
        
    for q in query:
        inums = set()
        path = '//postings[name="' + q + '"]'
        for posting in index.xpath(path):
            for p in posting:
                if (p.tag == 'inumber'):
                    inums.add(p.text)
                else:
                    pass
        if len(candidates) == 0:
            candidates = candidates.union(inums)
        else:
            candidates = candidates.intersection(inums)
                    
    return candidates

def get_directories (fsimage):
    
    directories = {}
    
    xpath =  '//INodeDirectorySection/directory'
    for directory in fsimage.xpath(xpath):
        parent = ''
        child = []
        for d in directory:
            if d.tag == 'parent':
                parent = str(d.text)
            elif d.tag == 'child':
                child.append(d.text)
            else:
                pass
        directories[parent] = child
   
    return directories
            
def get_parent (directories, inumber, fsimage):
    
    parent = None
   
    for key, values in directories.items():
        for v in values:
            if inumber == v:
                parent = key      
    
    return parent
      
def get_path (directories, inumber, fsimage):
    
    path = []
    
    parent = inumber
    
    while (parent != None):
        xpath = '//INodeSection/inode[id="' + parent +'"]'
        for nodes in fsimage.xpath(xpath):
            for n in nodes:
                if (n.tag == 'name' and n.text != None):
                    path.append(n.text)      
        parent = get_parent (directories, parent, fsimage)
        
    path.reverse()
    result = ''
    for p in path:
        result += '/'
        result += p
        
    return result

def get_metadata (fsimage, inumber):
    metadata = {}
    metadata["id"] = inumber
    xpath = '//INodeSection/inode[id="' + inumber +'"]'
    for nodes in fsimage.xpath(xpath):
        for n in nodes:
            if n.tag == 'type':
                metadata['type'] = n.text
            elif n.tag == 'mtime':
                metadata['mtime'] = n.text
            elif n.tag == 'blocks':
                for blocks in n:
                    for b in blocks:
                        if b.tag == 'id':
                            if 'blocks' in metadata.keys():
                                metadata['blocks'].append(b.text)
                            else:
                                metadata['blocks'] = [b.text]
    return metadata


    

if __name__ == "__main__":
    
    if(len(sys.argv) == 4):
        image_path = sys.argv[1]
        index_path = sys.argv[2]
        query = clean_query(sys.argv[3])
        
        fsimage = open_file(image_path)
        directories = get_directories (fsimage)
        candidates = get_possible_inum (index_path, query)
        
        for c in candidates:
            print (get_path(directories, c, fsimage))
            print (str(get_metadata (fsimage, c)))
        
    else:
        print("wrong arguments")
        exit()
        
        

