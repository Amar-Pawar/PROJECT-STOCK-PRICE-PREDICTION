'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-09-13
@Last Modified by: Amar Pawar
@Last Modified time: 2021-09-013
@Title : Consumer code to consume messages from producer
/**********************************************************************************
'''
import pandas as pd
import json
from kafka import KafkaConsumer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions
from logging_handler import logger
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Stock Price Prediction").getOrCreate()

try:
    ModelPath = "stock_model1"
    LoadModel = LinearRegressionModel.load(ModelPath)
except Exception as e:
    logger.info(e)

try:
    Consumer = KafkaConsumer('stock')
except Exception as e:
    logger.info(e)

def StockPricePrediction(LoadModel):
    try:
        for message in Consumer:
            res = json.loads(message.value.decode('utf-8'))
            dlist = list(res.values())
            df = pd.DataFrame([dlist], columns=['Open', 'High','Low','Close','Volume'])
            df = df.astype(float)
            spark_df = spark.createDataFrame(df)
            vectorAssembler = VectorAssembler(inputCols=['Open', 'High', 'Low'], outputCol='Features')
            df_vect = vectorAssembler.transform(spark_df)
            df_vect_features = df_vect.select(['Features', 'Close'])
            predictions = LoadModel.transform(df_vect_features)
            predictions.select("prediction", "Close", "Features").show()
            predict_value = predictions.select(functions.round(predictions["prediction"], 2).alias("prediction")).collect()[0].__getitem__("prediction")
            close_value = predictions.select('Close').collect()[0].__getitem__('Close')
            print(message.key)
            date_time = message.key.decode('utf-8')
            return predict_value, close_value, date_time
    except Exception as e:
        logger.info(e)

StockPricePrediction(LoadModel)