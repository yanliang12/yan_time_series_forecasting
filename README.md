# yan_time_series_forecasting

## start docker

```bash
docker pull yanliang12/yan_time_series_forecasting:1.0.1
```

## load the data and transform to numpy

```python
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
```

## train the model

```python
time_series_model = yan_time_series_forecasting.training_time_series_model(
	x, y, 
	model_path = 'time_series.h5',
	)
```

## predict 

```python
prediction = yan_time_series_forecasting.predict_time_series_from_model(
	x_npy = 'x.npy', 
	y_npy = 'y.npy', 
	date_npy = 'date.npy',
	model_path = 'time_series.h5',
	output_prediction_json = 'prediction.json',
	)
```

## start the jupyter notebook in docker

```bash
jupyter notebook \
--ip 0.0.0.0 \
--port 8674 \
--NotebookApp.token='' \
--no-browser \
--allow-root &
```

and access the jupyter notebook at 

http://0.0.0.0:8674/notebooks/matplotlib_example.ipynb


statsmodels

https://machinelearningmastery.com/arima-for-time-series-forecasting-with-python/

