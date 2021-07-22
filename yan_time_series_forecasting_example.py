import yan_time_series_forecasting

input_csv = 'QQQ.csv'
date_column_name = 'Date'
value_column_name = 'Close'
sliding_window_size = 30

date, y, x = yan_time_series_forecasting.time_series2feature_label_array(
	input_csv,
	date_column_name,
	value_column_name,
	x_npy = 'x.npy',
	y_npy = 'y.npy',
	date_npy = 'date.npy',
	)

time_series_model = yan_time_series_forecasting.training_time_series_model(
	x, y, 
	model_path = 'time_series.h5',
	)


prediction = yan_time_series_forecasting.predict_time_series_from_model(
	x_npy = 'x.npy', 
	y_npy = 'y.npy', 
	date_npy = 'date.npy',
	model_path = 'time_series.h5',
	output_prediction_json = 'prediction.json',
	)


import pandas
import datetime
import matplotlib

prediction = pandas.read_json(
	'prediction.json',
	orient = 'records',
	lines = True,
	)
prediction = prediction.set_index('date')

prediction.plot()
