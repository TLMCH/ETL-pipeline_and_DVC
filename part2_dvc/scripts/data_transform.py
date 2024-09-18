import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
import yaml
import os
import joblib

def transform_data():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    
    data = pd.read_csv('data/initial_data.csv')
    

    binary_cat_features = data.select_dtypes(include='bool')
    num_features = data.select_dtypes(['float', 'int']).drop(columns=['id', 'flat_id', 'building_id', 'building_type_int', 'price'])
    

    preprocessor = ColumnTransformer(
        [
        ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
        ('cat', OneHotEncoder(), ['building_type_int']),
        ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    data_transformed = pd.DataFrame(preprocessor.fit_transform(data))
    data_transformed[params['target_col']] = data[params['target_col']]

    train_data, test_data =  train_test_split(data_transformed, test_size=0.2, random_state=42)


    train_data.to_csv('data/train_data.csv', index=None)
    test_data.to_csv('data/test_data.csv', index=None)
    os.makedirs('models', exist_ok=True) 
    with open('models/preprocessor.pkl', 'wb') as fd:
        joblib.dump(preprocessor, fd) 

if __name__ == '__main__':
	transform_data()