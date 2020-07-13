from pyspark import SparkContext

sc = SparkContext(master = 'local[*]', appName = 'unofficial10')

country_language = sc.textFile('countrylanguage.csv').map(lambda x: x.split(',')).\
    map(lambda x: tuple([s.strip("' ") for s in x])).filter(lambda x: x[2] == 'F').\
    map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).\
    filter(lambda x: len(x[1]) >= 10).sortBy(lambda x: -1 * len(x[1])).keys().collect()

country = sc.textFile('country.csv').map(lambda x: x.split(',')).map(lambda x: (x[0].strip("' "), x[1].strip("' "))).\
    filter(lambda x: x[0] in country_language).collectAsMap()

for c in country_language:
    print(country[c])