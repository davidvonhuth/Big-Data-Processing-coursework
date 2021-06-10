import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import length
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import *

sc = pyspark.SparkContext()
spark = SparkSession(sc)

def good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=6:
            return False

        float(fields[2])
        return True

    except:
        return False



lines = sc.textFile('./ETH_USD_2015-08-09_2020-11-01-CoinDesk.csv')
clean_lines = lines.filter(good_line)


#Extracting only dates and prices
df = clean_lines.map(lambda line: Row(Date = line.split(',')[1],
                                    Closing_price = line.split(',')[2])).toDF()

#Casting the price to FloatType
df = df.withColumn('Closing_price', df['Closing_price'].cast(FloatType()))
df = df.withColumn("timestamps", unix_timestamp('Date','yyyy-MM-dd'))
df = df.drop('Date')



#*********************DATA-CAMP TUTORIAL************************

input_data = df.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
df = spark.createDataFrame(input_data, ["Closing_price", "features"])
standardScaler = StandardScaler(inputCol="features", outputCol="dates_scaled")
scaler = standardScaler.fit(df)
scaled_df = scaler.transform(df)



df.show()
scaled_df.show()

# df.show()
df.printSchema()
df.describe().show()

#Separating data into train and test
train_data, test_data = scaled_df.randomSplit([0.8, 0.2],seed=2020)

lr = LinearRegression(labelCol='Closing_price', maxIter=100, regParam=0.3, elasticNetParam=0.8)
print(train_data)
print(type(train_data))
linearModel = lr.fit(train_data)
#
predicted = linearModel.transform(test_data)
# print(predicted[:10])
# print(df['Closing_price'][:10])

# # Extract the predictions and the "known" correct labels
predictions = predicted.select("prediction").rdd.map(lambda x: x[0])
labels = predicted.select("Closing_price").rdd.map(lambda x: x[0])
# #
# # # Zip `predictions` and `labels` into a list
predictionAndLabel = predictions.zip(labels).collect()
# #





# # # Print out first 5 instances of `predictionAndLabel`
print(predictionAndLabel[:5], '\n\n\n\n\n\n')

print('Coefficient: ', linearModel.coefficients)
print('Intercept: ', linearModel.intercept)

print('RMSE: ', linearModel.summary.rootMeanSquaredError)
print('R2-Score: ', linearModel.summary.r2)
print('\n\n\n\n\n\n\n')
print(linearModel.explainParams())

print(linearModel.evaluate(test_data))
