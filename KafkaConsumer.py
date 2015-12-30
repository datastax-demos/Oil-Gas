from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from decimal import *
import datetime, uuid, sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def insert_oil_data(rdd):
    cluster = Cluster()
    session = cluster.connect("oilgas")
    reading = rdd[1].split(',')

    drill_reading = {
        "drill_id":uuid.UUID('{d7e607ad-67b6-48c8-ab39-d56b5fc4c97d}'),
        "td":Decimal(reading[0]),
        "bittd":Decimal(reading[1]),
        "wob":Decimal(reading[2]),
        "rpm":Decimal(reading[3]),
        "rop":Decimal(reading[4]),
        "flowin":Decimal(reading[5]),
        "flowout":Decimal(reading[6]),
        "plcirc":Decimal(reading[7]),
        "mwin":Decimal(reading[8]),
        "mwout":Decimal(reading[9]),
        "data_timestamp":datetime.datetime.fromtimestamp(int(reading[10])),
        "date":str(datetime.datetime.fromtimestamp(int(reading[10])).day),
    }
    insert_reading = session.prepare('''
        INSERT INTO oilgas.drill_data
            (drill_id, td, bittd, wob, rpm, rop, flowin, flowout, plcirc, mwin, mwout, data_timestamp, date)
        VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''')
    session.execute(insert_reading.bind(drill_reading))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>")
        exit(-1)

    sc = SparkContext(appName="OilGasDemo")
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    kvs.foreachRDD(lambda rdd: rdd.foreach(insert_oil_data))

    ssc.start()
    ssc.awaitTermination()