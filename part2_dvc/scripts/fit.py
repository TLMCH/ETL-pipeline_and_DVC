import pandas as pd
from sklearn.linear_model import LinearRegression
import yaml
import os
import joblib

# обучение модели
def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
        

    train_data = pd.read_csv('data/train_data.csv')
    target = train_data[params['target_col']]
    train_data.drop(columns=[params['target_col']], inplace=True)


    model = LinearRegression()
    model.fit(train_data, target) 
    

    os.makedirs('models', exist_ok=True) 
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(model, fd) 

if __name__ == '__main__':
	fit_model()