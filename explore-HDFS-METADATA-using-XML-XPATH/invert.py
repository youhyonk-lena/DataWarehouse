#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 22:59:00 2020

@author: lenakim
"""

import os
import sys
import re
from lxml import etree


#os.chdir("/Users/lenakim/Documents/USC/2020_Spring/INF_551/hw2")
os.listdir()

def open_file (path):
    with open(path) as f:
        tree = etree.parse(f)
    return tree

def tokenize (string):
    string = string.split(".")[0]
    string = re.sub('-', ' ', string)
    string = string.lower()
    return string.split()



def find_inumber (child, index):
    for inode in child.findall("inode"):
        name = inode.find("name").text
        if name == None:
            pass
        else:
            token = tokenize(name)
            for t in token:
                inumber = inode.find("id").text
                if t in index.keys():
                    index[t].append(inumber)
                else:
                    index[t] = [inumber]
    return index

def get_index (idict):
    root = etree.Element("index")
    for token in idict.keys():
        postings = etree.Element("postings")
        name = etree.Element("name")
        name.text = token
        postings.append(name)
        for inum in idict[token]:
            inumber = etree.Element("inumber")
            inumber.text = inum
            postings.append(inumber)
        root.append(postings)
    return root

def write_output (output_path, root):
    with open (output_path,'wb') as f:
        f.write(etree.tostring(root, pretty_print = True))
    
        
def create_index (input_path, output_path):
    file = open_file(input_path)
            
    idict = {}
    for child in file.getroot():
        if child.tag == 'INodeSection':
            find_inumber(child, idict)
             
    index = get_index(idict)
    write_output(output_path, index)

if __name__ == "__main__":
    
    if(len(sys.argv) == 3):
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        create_index (input_path, output_path)
    else:
        print("wrong arguments")
        exit()
   