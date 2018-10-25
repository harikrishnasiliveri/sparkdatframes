from pyspark import SparkConf, SparkContext

sc = SparkContext("local", "Spark Demo")

print(sc.textFile("c:\\Downloads\Hari Hadoop Developer").first())