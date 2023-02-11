# -*- coding: utf-8 -*-
"""
Created on Sat Feb 12 18:57:46 2022

@author: aleex
"""

import pandas as pd
import prefect
from prefect import Flow,task,Parameter,config
from typing import Any, Dict, List
from prefect.engine.results import LocalResult 
from sklearn.model_selection import train_test_split
import pickle
import graphviz
import os


#ruta_dest = Parameter('ruta_dest',default='data/processed')

@task
def load_data(path: str):
    df = pd.read_csv(path,sep=',')
    return df


@task (result = LocalResult(dir='data/processed'))
def save_raw_data(data: pd.DataFrame,ruta_dest: str):
    data.to_excel(ruta_dest+'/data.xlsx',index=False)


@task
def get_classes(data: pd.DataFrame, target_col: str):
    return sorted(list(data[target_col].unique()))
    """Task for getting the classes from the Iris data set."""
    

@task
def encode_categorical_columns(data: pd.DataFrame, target_col: str):
    """Task for encoding the categorical columns in the Iris data set."""
    df= pd.get_dummies(data, columns=[target_col], prefix="", prefix_sep="")
    return df

@task(target="{task_name}_output-{today}", 
      result = LocalResult(dir='data/processed'))
def split_data(data: pd.DataFrame, test_data_ratio: float, classes: list):
    """Task for splitting the classical Iris data set into training and test
    sets, each split into features and labels.
    """
    X,y = data.drop(columns=classes),data[classes]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_data_ratio)
    return dict(
        train_x=X_train,
        train_y=y_train,
        test_x=X_test,
        test_y=y_test,
    )
    
with Flow('iris-prefect') as flow:
    ruta='data/processed'

    # Marcamos como parámetro el ratio train-test. Dentro de la app o en la UI podremos reconfigurarlo
    test_data_ratio = Parameter("test_data_ratio", default=0.2)
    ruta_dest = Parameter("ruta_dest", default=ruta)
    url = Parameter("url",default = 'https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv')
    target_col = 'species'
   
    data = load_data(path=url)
    classes = get_classes(data=data, target_col=target_col) 
    categorical_columns = encode_categorical_columns(data=data, target_col=target_col)
    train_test_dict = split_data(data=categorical_columns,test_data_ratio=test_data_ratio,
                            classes=classes)
    #save_raw_data(data=data,ruta_dest=ruta_dest)
    
flow.run()
flow.visualize()
#flow.register(project_name="Iris Project")