import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import json
import yaml
import os


def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
            

    with open('models/fitted_model.pkl', 'rb') as fd:
        model = joblib.load(fd) 
    test_data = pd.read_csv('data/test_data.csv')
    target = test_data[params['target_col']]
    test_data.drop(columns=[params['target_col']], inplace=True)
        

    target_predicted = model.predict(test_data)

    mse=mean_squared_error(target, target_predicted) 
    rmse = np.sqrt(mse) 
    r2 = r2_score(target, target_predicted) 
        
    res = f'MSE: {mse}\nRMSE: {rmse}\nR^2: {r2}'

    os.makedirs('cv_results', exist_ok=True) 
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(res, fd)
    

if __name__ == '__main__':
	evaluate_model()