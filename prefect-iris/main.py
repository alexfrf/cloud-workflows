# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 00:22:05 2022

@author: aleex
"""

from prefect import Flow 
from prefect.tasks.prefect import StartFlowRun
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule



data_engineering_flow = StartFlowRun(flow_name="iris-prefect", project_name='Iris Project')
data_science_flow = StartFlowRun(flow_name="data-science", project_name='Iris Project')

with Flow("main-flow") as flow:
    data_science = data_science_flow(upstream_tasks=[data_engineering_flow])
    
flow.register(project_name="Iris Project")
flow.run()