# PROJECT-STOCK-PRICE-PREDICTION USING LIVE DATA

## PROJECT SIMULATION ON STOCK PRICE PREDICTION USING Static DATA

### Prerequisites:-

* Hadoop 3.3.1
* Kafka 2.13
* Spark 3.1.2
* MLlib
* AWS EC2
* AWS S3
* Python 3.8
* Java

### Library Used:

* Alpha_Vantage Api: for getting  stock data.
* Flask: for rendering visualization of data in web browser
* Following is the workflow diagram or architecture for our project for predicting the stock price


* Here we will take data from alpha vantage API as it will work as data source for us,
* Then with kafka we will fetch that data from the above mentioned API.
* Then we will store it to any centralized storage, it can be HDFS or AWS S3.
* The data fetch will not be cleaned so we will clean it using pyspark and then we will store clean data in our data warehouse
* This cleaned data we will use for our analytical purpose i.e. for building ML model and predicting stock prices and accordingly plotting graphs

### Now Using Kafka Get the Stock data From Alphavantage and Store it Into Hdfs:
* Step1: Start hadoop daemons
** $ start-all.sh

### Step2: Start zookeeper services and kafka
* $ Apache/kafka/ sudo bin/zookeeper-server-start.sh config/zookeeper.properties
* Zookeeper started
* $ Apache/kafka/ sudo bin/kafka-server-start.sh config/server.properties
* Kafka-server /broker started

### Step3:
* Now with producer and consumer code fetch the data from alpha vantage API
* Start producer code in one terminal
* It will fetch data and send it to the kafka topic that is mentioned
* Run consumer code in other terminal
* It will consume messages from producers and consume the data and store in file that we provided, in our case its HDFS path


### Step4:
* Data we fetched with kafka and stored on storage system will not be cleaned so it needs to be processed to get clean data
* For this we are going to use spark for cleaning the data

### Step5: Preprocessing the data to clean it
* 1.Create a notebook and load create a spark session
* from pyspark.sql import SparkSession
* spark= SparkSession.builder.appName('Stock Data').getOrCreate()

### 2.Load the data we fetched in spark dataframe
* dataset=spark.read.csv("hdfs://localhost:9000/Sample/data.csv",inferSchema=True,header=True)

### 3.Check the dataframe columns
* dataset.columns

### 4. Clean the column as they are not in expected format for analysis
* dataset2=dataset.withColumnRenamed('["time"','time')\
* .withColumnRenamed(' "open"','open')\
* .withColumnRenamed(' "high"','high')\
* .withColumnRenamed(' "low"','low')\
* .withColumnRenamed(' "close"','close')\
* .withColumnRenamed(' "volume"]','volume')

### 5.Similarly clean the data by removing unnecessary quotes and brackets
* newDf = dataset2.withColumn('open', regexp_replace('open', '"', ''))\
* .withColumn('time', regexp_replace('time', '\\[', ''))\
* .withColumn('time', regexp_replace('time', '"', ''))\
* .withColumn('high', regexp_replace('high', '"', ''))\
* .withColumn('low', regexp_replace('low', '"', ''))\
* .withColumn('close', regexp_replace('close', '"', ''))\
* .withColumn('volume', regexp_replace('volume', '\\]', ''))\
* .withColumn('volume', regexp_replace('volume', '"', ''))

### 6. Check the final cleaned data

|       time        |   open  |  high   |  low    | close   |volume |
|-------------------|---------|---------|---------|---------|-------|
|2021-09-03 19:00:00| 139.55  | 139.55  | 139.55  | 139.55  | 2749  |
|2021-09-03 17:45:00| 139.65  | 139.65  | 139.65  | 139.65  | 150   |
|2021-09-03 16:15:00| 139.58  | 139.61  | 139.55  | 139.55  | 31003 |
|2021-09-03 16:00:00| 139.765 | 139.885 | 139.54  | 139.62  | 269779|
|2021-09-03 15:45:00| 139.69  | 139.769 | 139.635 | 139.769 | 79292 |
|2021-09-03 15:30:00| 139.8399| 139.86  | 139.655 | 139.69  | 49114 |
|2021-09-03 15:15:00| 139.76  | 139.88  | 139.75  | 139.83  | 50153 |
|2021-09-03 15:00:00| 139.78  | 139.84  | 139.72  | 139.74  | 38715 |
|2021-09-03 14:45:00| 139.61  | 139.78  | 139.59  | 139.78  | 31959 |
|2021-09-03 14:30:00| 139.6829| 139.6861| 139.57  | 139.6311| 31552 |
|2021-09-03 14:15:00| 139.63  | 139.72  | 139.57  | 139.69  | 34371 |
|2021-09-03 14:00:00| 139.76  | 139.76  | 139.63  | 139.64  | 36656 |
|2021-09-03 13:45:00| 139.69  | 139.78  | 139.65  | 139.76  | 28612 |
|2021-09-03 13:30:00| 139.65  | 139.73  | 139.629 | 139.67  | 30235 |
|2021-09-03 13:15:00| 139.49  | 139.65  | 139.48  | 139.64  | 35199 |
|2021-09-03 13:00:00| 139.46  | 139.515 | 139.4104| 139.47  | 32583 |
|2021-09-03 12:45:00| 139.45  | 139.47  | 139.35  | 139.46  | 34212 |
|2021-09-03 12:30:00| 139.47  | 139.5   | 139.33  | 139.43  | 34336 |
|2021-09-03 12:15:00| 139.4451| 139.54  | 139.39  | 139.52  | 35873 |
|2021-09-03 12:00:00| 139.5201| 139.56  | 139.43  | 139.48  | 32806 |


### 7.Convert data types of columns to desired format
* df2 = newDf.withColumn("open",col("open").cast("double"))\
* .withColumn("high",col("high").cast("double"))\
* .withColumn("low",col("low").cast("double"))\
* .withColumn("close",col("close").cast("double"))\
* .withColumn("volume",col("volume").cast("int"))

### 8. Check for null values by converting spark df to pandas df
* pandaDf=newDf.toPandas()
* pandaDf.isna().sum()


### Step6: CREATING A ML MODEL
* Import necessary things
* from pyspark.ml.linalg import Vectors
* from pyspark.ml.feature import VectorAssembler
* from pyspark.ml.regression import LinearRegression

### 2. Create features columns with vector assembler
* featureassembler=VectorAssembler(inputCols=["open","high","low"],outputCol="Features")

### 3. Trasform the data set accordingly
* output=featureassembler.transform(df2)

### 4. Sort data in ascending order
* finalized_data=output.select("time","Features","close").sort("time",ascending=True)

### 5. Split data into test and train data with window function
* df10=finalized_data.withColumn("rank",percent_rank().over(Window.partitionBy().orderBy("time")))
* train_df=df10.where("rank<=.8").drop("rank")
* test_df=df10.where("rank>.8").drop("rank")

### 6. Write test data to parquet file for further use
* test_df.write.parquet('testdata')

### 7. Create model with linear regression algoritham
* regressor=LinearRegression(featuresCol='Features', labelCol='close')
* lr_model=regressor.fit(train_df)

### 8.Check for coefficient and intercepts
* lr_model.coefficients
* lr_model.intercept

### 9. Make prediction by transforming data in model
* pred= lr_model.transform(test_df)
* pred.select("Features","close","prediction").show()
* Output:

| Features           |close | prediction       |
|--------------------| -----|------------------|
|[139.045,139.085,...|138.98| 139.0088189816052|
|[138.97,138.98,13...|138.97|138.98604284027746|
|[139.21,139.21,13...|139.21| 139.218743620076 |
|[138.81,138.81,13...|138.81|138.81953685610827|
|[138.81,139.05,13...|139.05|138.98329554207902|
|[139.4355,139.435...| 138.9|139.03487193320166|
|[139.25,139.25,13...|139.25|139.25866429647274|
|[138.97,138.97,13...|138.97|138.97921956169534|
|[139.43,139.59,13...|139.59|139.53984348571282|

### 10. Save model to HDFS

* lr_model.save(“stock_model”)


### Visualization using flask

* Create a flask application by writing python code and visualize the data by loading the model and test data
And also create index.html file for rendering visualization


#### After running the code we will see animated graph plotting data


