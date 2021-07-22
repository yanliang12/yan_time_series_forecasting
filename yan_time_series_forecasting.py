###########yan_time_series_forecasting.py#########
import numpy
import pandas

import keras
from keras.models import *
from keras.layers import *

import pyspark
from datetime import *
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.ml.feature import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SparkSession.builder.getOrCreate()


'''
transforming a csv file of data series to numpy arrays

example:


input_csv = 'QQQ.csv'
date_column_name = 'Date'
value_column_name = 'Close'
sliding_window_size = 30

date, y, x = time_series2feature_label_array(
	input_csv,
	date_column_name,
	value_column_name,
	x_npy = 'x.npy',
	y_npy = 'y.npy',
	date_npy = 'date.npy',
	)

'''

def time_series2feature_label_array(
	input_csv,
	date_column_name,
	value_column_name,
	x_npy = None,
	y_npy = None,
	date_npy = None,
	sliding_window_size = 30,
	):
	'''
	load the data
	'''
	data = sqlContext.read.option('header', True).csv(input_csv)
	data.registerTempTable("data")
	data1 = sqlContext.sql(u"""
		SELECT {} AS date, FLOAT({}) AS value,
		ROW_NUMBER() OVER (ORDER BY {} ASC) AS document_id
		FROM data
		""".format(date_column_name,
			value_column_name,
			date_column_name))
	data1.registerTempTable("data")
	'''
	attach the last sliding_window_size days value as the feature for each day
	'''
	cmd1 = ',\n'.join(["d%02d.value AS last_%02d_value"%(i,i) for i in range(1,sliding_window_size+1)])
	cmd2 = '\n'.join(["LEFT JOIN data AS d%02d ON d%02d.document_id = d.document_id - %02d"%(i,i,i) for i in range(1,sliding_window_size+1)])
	data2 = sqlContext.sql(u"""
		SELECT d.*,
		%s,
		d.value AS label
		FROM data AS d
		%s
		WHERE d.document_id > %d
		"""%(cmd1,cmd2,sliding_window_size))
	data2.write.mode("Overwrite").parquet("data2")
	data2 = sqlContext.read.parquet("data2")
	'''
	assemble the columns to one vector
	'''
	features_column = ["last_%02d_value"%(i) for i in range(1,sliding_window_size+1)]
	assembler = VectorAssembler(
		inputCols = features_column,
		outputCol = 'feature')
	data3 = assembler.transform(data2)
	data3.registerTempTable("data3")
	'''
	extract the numpy array of feature and label from the dataframe
	'''
	data4 = sqlContext.sql(u"""
		SELECT document_id, date, label, feature
		FROM data3
		""")
	data_pdf = data4.toPandas()
	label = numpy.array([numpy.array([r]) for r in data_pdf['label']])
	feature = numpy.array([numpy.array(r) for r in data_pdf['feature']])
	date = numpy.array([r for r in data_pdf['date']])
	if x_npy is not None:
		numpy.save(x_npy, feature)
	if y_npy is not None:
		numpy.save(y_npy, label)
	if date_npy is not None:
		numpy.save(date_npy, date)
	return date, label, feature


'''
building a model 

time_series_model = building_time_series_model(
	sliding_window_size = 30,
	drop_out_rate = 0.01,
	model_path = 'time_series.h5',
	)

'''
def building_time_series_model(
	sliding_window_size = 30,
	drop_out_rate = 0.01,
	model_path = None,
	):
	model = Sequential(
		[
			keras.layers.Dense(
				1000, activation="relu", 
				input_shape=(sliding_window_size,)
			),
			keras.layers.Dense(500, activation="relu"),
			keras.layers.Dropout(drop_out_rate),
			keras.layers.Dense(200, activation="relu"),
			keras.layers.Dropout(drop_out_rate),
			keras.layers.Dense(1),
		]
	)
	model.summary()
	model.compile(
		optimizer = "adam", 
		loss="mse",)
	if model_path is not None:
		model.save(model_path)
	return model


'''
train a model with x and y

time_series_model = training_time_series_model(
	x, y, 
	model_path = 'time_series.h5',
	)

'''

def training_time_series_model(
	x, y, data = None,
	x_npy = None,
	y_npy = None,
	date_npy = None,
	model_path = 'time_series.h5',
	max_iter = 100,
	batch_size = 500,
	drop_out_rate = 0.01,
	sliding_window_size = 30,
	):
	if x_npy is not None:
		x = numpy.load(x_npy)
	if y_npy is not None:
		y = numpy.load(y_npy)
	if date_npy is not None:
		date = numpy.load(date_npy)
	###
	sliding_window_size = x.shape[-1]
	try:
		#try to load an existing model
		model = keras.models.load_model(model_path)
	except:
		#build a new model
		model = building_time_series_model(
			sliding_window_size = sliding_window_size,
			drop_out_rate = drop_out_rate,
			model_path = model_path,
			)
	#try a model
	model.fit(
		x,
		y,
		epochs = max_iter,
		batch_size = batch_size,
		)
	model.save(model_path)
	return model

###########yan_time_series_forecasting.py#########