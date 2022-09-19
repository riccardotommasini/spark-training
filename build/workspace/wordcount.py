from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab words count Solution")
sc = SparkContext(conf=conf)
conf.setMaster('spark://spark-master:7077')
conf.set('spark.driver.bindAddress', '0.0.0.0')

tokenized = sc.textFile("data/AliceInWonderLandPart1.txt").flatMap(lambda line: line.split(" "))
wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
list = wordCounts.collect()
print ('[%s]' % ', '.join(map(str, list)))

wordCounts.take(10)