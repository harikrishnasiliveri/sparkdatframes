from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import configparser as cp
import sys

props = cp.RawConfigParser()
props.read('venv/resources/application.properties')
env = sys.argv[1]

spark = SparkSession. \
    builder. \
    master(props.get(env, 'executionMode')). \
    appName("daily product revnue") .\
    getOrCreate()

inputBaseDir = props.get(env, 'input.base.dir')
outputBaseDir = props.get(env, 'output.base.dir')

ordersCSV = spark. \
    read. \
    csv( inputBaseDir + "/orders/part-00000") \
    .toDF('order_id', 'order_date', 'order_customer_id', 'order_status')


orderItemsCSV = spark. \
    read. \
    csv(inputBaseDir + "/order_items/part-00000") \
    .toDF('order_item_id', 'order_item_order_id', 'order_item_product_id', 'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

from pyspark.sql.types import IntegerType, FloatType

orders = ordersCSV. \
    withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))

orderItems = orderItemsCSV.\
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_items_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

dailyProductRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")'). \
join(orderItems, orders.order_id == orderItems.order_item_order_id). \
groupBy(orders.order_date, orderItems.order_item_product_id). \
agg(round(sum(orderItems.order_item_subtotal), 2).alias('revenue_total'))

spec = Window. \
    partitionBy(dailyProductRevenue.order_date). \
    orderBy(dailyProductRevenue.revenue_total.desc())

dailyProductRevenueRank = dailyProductRevenue.withColumn('rnk', rank().over(spec))

topN = int(sys.argv[2])

topDailyProducts = dailyProductRevenueRank. \
    where(dailyProductRevenueRank.rnk < topN). \
    orderBy(dailyProductRevenueRank.order_date, dailyProductRevenueRank.revenue_total.desc())

topDailyProducts.write.csv(outputBaseDir + "topDailyProductsNew")
#topDailyProducts.write.csv

#orderItems.printSchema()
#orderItems.show()

