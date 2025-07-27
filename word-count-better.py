import re
from pyspark import SparkConf, SparkContext

# W+ only words no puncutation ...
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/ml-100k/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y)
wordCountsSorted = wordCounts.map(lambda pair: (pair[1], pair[0])).sortByKey()


results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii' , 'ignore').decode()
    if word:
        print(f"{word}:\t\t{count}")
