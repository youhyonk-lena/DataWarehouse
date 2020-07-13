import os
import sys
import json
from collections import defaultdict
import csv
import operator

#O(n) where n = num rows
def load_csv(file):

    dict = defaultdict(list)
    reader = csv.reader(open(file, 'r'))
    header = next(reader)
    for row in reader:
       dict[row[0]].append(row[1:])

    return header, dict

country_header, country = load_csv(sys.argv[1]) # O(m)
cl_header, cl = load_csv(sys.argv[2]) # O(n)

#O(n)
official = defaultdict(list)
for k, v_list in cl.items():
    for v in v_list:
        is_official = v[cl_header.index("IsOfficial") - 1]
        if is_official == 'T':
            language = v[cl_header.index("Language") - 1]
            official[language].append(k)
        else:
            pass

#O(n)
top = {}
for language, countries in official.items():
    for c in countries:
        population = float(country[c][0][country_header.index("Population") - 1])
        if population >= 1000000:
            if language in top.keys():
                top[language] += population
            else:
                top[language] = population
        else:
            pass

# O(nlogn): Time sort
top_10 = sorted(top.items(), key = lambda x: x[1], reverse = True)[:10]
for t in top_10:
    print(t[0])

# TIme Complexity = O(m) + O(n) + O(n) + O(n) + O(nlogn) = O(m) + O(nlogn)
# One way to improve the algorithm is to merge two csv files into one pandas.dataframe based on country code, and looping through the data only once,
# which would give a slightly better time complexity
