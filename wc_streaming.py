from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, expr
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName('Spark Streaming Word Count') \
            .master('yarn') \
            .config('spark.streaming.stopGracefullyOnShutdown', 'true') \
            .config('spark.sql.shuffle.partitions', 3) \
            .getOrCreate()

    logger = Log4J(spark)

    socket_line_df = spark.readStream \
                .format('socket') \
                .option('host', 'sardinia') \
                .option('port', '2220') \
                .load()

    #socket_line_df.printSchema()
    words_df = socket_line_df.select(expr("explode(split(value, ' ')) as words"))
    count_df = words_df.groupBy('words').count()
    out_wc_query = count_df.writeStream \
                    .format('console') \
                    .option('checkpointLocation', 'chk-point-dir') \
                    .queryName('Words from Socket') \
                    .outputMode('complete') \
                    .start()
    logger.info('Listening on port 2220')
    out_wc_query.awaitTermination()
