# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 16:42:07 2019

@author: syue003
"""


import Reader
from surprise import Dataset
# Algorithms
from surprise import KNNBasic
from surprise.model_selection import LeaveOneOut

# Measure
from surprise.accuracy import rmse
from surprise import accuracy

from surprise.model_selection import train_test_split

#def f_rs_cr_sim_matrix(df_name, user_base, mtx_file):

    
    """
      Module name: f_rs_cr_sim_matrix.py
      """
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from collections import Counter
from collections import defaultdict
import heapq
from operator import itemgetter
import itertools

"""

from surprise i
      Purpose: Create similarity matrix
      
      Parameters:
          df_name: Surprise dataset to create the similarity matrix
          user_base: Indicator to create user base or item base similarity matrix
           True    : User base or item base similarity matrix
           False   : Item base or item base similarity matrix
          mtx_file:  file name to save the matrix to a text file
     return:
          df_sum_qty_final: dataframe name
   """
def f_rs_cr_sim_matrix(train_set, xlsx_file, user_base, mtx_measure, df_idx):  
    sim_opt = {'name': mtx_measure, 'user_based': user_base }    
    model = KNNBasic(sim_options=sim_opt,  verbose = False)
    model.fit(train_set)
    simsMatrix = model.compute_similarities()
    Print ("Complete Matrix -",  mtx_measure) 
    df = pd.DataFrame(simsMatrix)
    df.columns = df_idx; df_index = df_idx
    df.to_excel(xlsx_file)
    return df
       





# The file was from the dataframe created by created from user_base_rs_v03 
file_nm =   "C:\\SYUE\\RecSys\\rs_df_sum_qty_final_case1.xlsx"
df_sum_qty_final = pd.read_excel(file_nm)


reader = Reader(rating_scale=(1, 10))  # Reader object; rating_scale is required 
data = Dataset.load_from_df(df_sum_qty_final[['HH_SK', 'PROD_SK', 'UNIT_QTY']], reader) # type:  surprise.dataset.DatasetAutoFolds

ft = data.build_full_trainset()
print("The total number of users in data:", ft.n_users,  "The total number of items in data:", ft.n_items )
# Set aside one rating per user for testing
LOOCV = LeaveOneOut(n_splits=1, random_state=1)
for train, test in LOOCV.split(data):
    trainSet = train
    testSet  =  test
print("The total number of users in trainSet:", trainSet.n_users,  "The total number of items in trainSet:", trainSet.n_items ) 
print("The total length of testSet:", len(testSet),"\n Example of testSet:", testSet[0:2]) 
_idx_user =  [trainSet.to_raw_uid(uiid) for uiid in range(trainSet.n_users)]
mtx_measure = 'cosine'; user_base = True
dir_loc = "C:\\SYUE\\RecSys\\"
xlsx_file = dir_loc + "u_sim_cosine_case1.xlsx"
df = f_rs_cr_sim_matrix(trainSet, xlsx_file, user_base, mtx_measure,  _idx_user)  
 

mtx_measure = 'msd'; user_base = True
xlsx_file = dir_loc + "u_sim_msd_case1.xlsx"
df_msd = f_rs_cr_sim_matrix(trainSet, xlsx_file, user_base, mtx_measure,  _idx_user)  

mtx_measure = 'person'; user_base = True
xlsx_file = dir_loc + "u_sim_person_case1.xlsx"
df_person = f_rs_cr_sim_matrix(trainSet, xlsx_file, user_base, mtx_measure,  _idx_user)  

mtx_measure = 'pbaseline'; user_base = True
xlsx_file = dir_loc + "u_sim_pbaseline_case1.xlsx"
df_pbaseline = f_rs_cr_sim_matrix(trainSet, xlsx_file, user_base, mtx_measure,  _idx_user)  