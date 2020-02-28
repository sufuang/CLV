# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 13:23:42 2019
f_cr_df_rs
@author: syue003
"""

import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from collections import Counter


def f_cr_df_rs(inp_file, out_file, gp_ind, mstcom_ind):
    
    """
      Module name: f_cr_df_rs.py
      Purpose: Base on the parameters to create a dataframe and save it in a excel file
      Parameters:
          inp_file: Input  file name with path to create the dataframe with file type as xlsx
          out_file: Output file name with path to save the dataframe with file type as xlsx
     return:
          df_sum_qty_final: dataframe name
   """
    df = pd.read_excel(inp_file)
    df1 = df.drop(['WGT_PROD_IND','WGT_QTY', 'TXN_DT','STR_FAC_NBR','TXN_NBR' ],axis = 1)
    df1_x = df1[(df1['PROD_SK'] > -1) & (df1["HH_SK"]> -1) &  (df1["UNIT_QTY"]> -1)]  
    print ( 'mstcom_ind= ', mstcom_ind, 'gp_ind = ', gp_ind,  'df1.shape', df1.shape,  'df1_x.shape', df1_x.shape) 
    if mstcom_ind == True:
       user_ids_count = Counter(df1_x.HH_SK)   # type: collections.Counter
       prod_ids_count = Counter(df1_x.PROD_SK) 
       tot_unq_hh     = len(df1_x.HH_SK.unique())
       tot_unq_prod   = len(df1_x.PROD_SK.unique())
       # n = int(tot_unq_hh * 0.8)
       # m = int(tot_unq_prod  * 0.8)
       n = 10000
       m = 3000
       print ("total no of unique user: ", tot_unq_hh, "select ",    n, " most common user") 
       print( "total no of unique prod: ", tot_unq_prod, "select ",  m, " most common prod"     )
       user_ids = [u for u, c in user_ids_count.most_common(n)]
       prod_ids = [m for m, c in prod_ids_count.most_common(m)]
       df_small = df1_x[df1_x.HH_SK.isin(user_ids) & df1_x.PROD_SK.isin(prod_ids)]   
    else:
      df_small = df1_x
      
    if gp_ind == True:   
       df_sum_qty = df_small.groupby(["HH_SK","PROD_SK"]).sum().reset_index() 
    else:
       df_sum_qty = df_small  
        
    df_sum_qty_final =  df_sum_qty[ (df_sum_qty["UNIT_QTY"] > 0 ) & (df_sum_qty["UNIT_QTY"] < 11 ) ]
    print ( 'df_small.shape', df_small.shape,  'df_sum_qty.shape', df_sum_qty.shape,  'df_sum_qty_final.shape', df_sum_qty_final.shape)
    df_sum_qty_final.to_excel(out_file)
    return df_sum_qty_final
 

inp_file = "C:\\SYUE\\RecSys\\pos_tnx.xlsx"
#out_file = "C:\\SYUE\\RecSys\\final_inp_gp_mstcom.xlsx"
out_file = "C:\\SYUE\\RecSys\\final_inp_gp_mstcom_m3000.xlsx"
df_sum_qty_final = f_cr_df_rs(inp_file, out_file, gp_ind=True, mstcom_ind=True)

#df_sum_qty_final.to_excel(out_file)
