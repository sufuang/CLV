"""
data_profiling.py
"""

"""
Import
"""


import pandas as pd
import pandas_profiling
import datetime
from datetime import datetime, timedelta, date
import time 
import snowflake.connector
import json
import os


"""
Mudule name: cf_get_col_nm
Purpose: out a string to have all the column name excluding the ones with DW_ prefix
parameter 
 p_tbl_nm: Table name
Output
 string with col names
"""
def cf_get_col_nm(p_tbl_nm):    
     """ get column names """   
     q_col = f'''SELECT COLUMN_NAME FROM {db_nm}.INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = '{sch_nm}'  
                and table_name = '{p_tbl_nm}' 
                and COLUMN_NAME not like 'DW_%' order by ORDINAL_POSITION'''    
                
     df_q_col = cf_cr_df_cur_qry(q_col) 
     l_col = df_q_col.values.tolist()
     l_col_flat = [item for sublist in df_q_col.values.tolist() for item in sublist]
     return  ','.join(l_col_flat)
    
    
     
"""
Mudule name: cf_cr_df_cur_qry
Purpose: Create a dataframe from the p_sql query result
parameter 
 p_qry: sql
Output
 a dataframe
"""
def cf_cr_df_cur_qry(p_qry):
    cur_p_qry = connection.cursor().execute(p_qry)
    return  pd.DataFrame.from_records(iter(cur_p_qry), columns=[x[0] for x in cur_p_qry.description])

def cf_cr_data_profile_rpt(p_df, p_file_nm):
    """
     Module name : cf_cr_data_profile_rpt
     parameter 
      p_df: Data frame to profile
      p_file_nm: file name for profile report 
    Output
     a html file
    """
    
    start_time = time.time() 
    print(f'process table:{p_file_nm}' )
    profile = p_df.profile_report(title='Data Profiling Report - ' +  p_file_nm  )
    profile.to_file(output_file= output_path + p_file_nm + ".html")
    end_time = time.time() 
    cf_elapse_time (start_time, end_time, dsc = f'Pandas Profiling Report for {p_file_nm }. ')    


""" Define constant
cnt_lmt_x: Count limit to create a profile report
           - If Count limit <  cnt_lmt_x will not create a profile report
cnt_lmt_y: Maximum count to process 

"""

cnt_lmt_x = 20  
cnt_lmt_y = 1000000 


db_nm  = 'EDM_CONFIRMED_DEV'
sch_nm = 'DW_C_PRODUCT'
output_path = 'C:\\SYUE\\EDM\\Data_profiling_report\\'

""" Get timestamp module """

prg_name = ""
path_code = "C:\\Users\\syue003\\wip_RecSys\\"
c_timedte = path_code + "c_time_dte.py" 
exec(compile(open(c_timedte, 'rb').read(),c_timedte, 'exec'))

""" Connect to snowflake 
config_itds.json:
{ "secrets" :
              {
                "account"   : "abs_itds_dev.east-us-2.azure",
                "user"      : "syue003@safeway.com",
                "warehouse" : "LOAD_WH",             
                "database"  : "EDM_CONFIRMED_DEV",
                "schema"    : "SCRATCH",
                "password"  : "Chungli$1"
                
              }
}
              
- authenticator='externalbrowser' is required when login with AzureAD 
 - Sign in with AzureAD is related to federated environment. The user authentication is separated from user access 
    through the use of one or more external entities that provide independent authentication of user credentials.
 - Specify authenticator='externalbrowser' to connect Python to Snowflake  
   - It will initiating login request with your identity provider. A browser window should have opened for you to 
     complete the login.              

"""

CONFIG_LOCATION='C:\\SYUE\\Snowflask\\'
# load config
CONFIG = json.loads(open(str(CONFIG_LOCATION+'config_itds.json')).read())

SF_ACCOUNT    = CONFIG['secrets']['account']
SF_USER       = CONFIG['secrets']['user']
SF_WAREHOUSE  = CONFIG['secrets']['warehouse']
SF_ROLE       = 'EDDM_DATA_READER_GG'
SF_DATABASE   = CONFIG['secrets']['database']
SF_SCHEMA     = CONFIG['secrets']['schema']
SF_PASSWORD   = CONFIG['secrets']['password']
# extract SQL script from config
SCRIPT  = CONFIG['load_script']
# fire up an instance of a snowflake connection
connection = snowflake.connector.connect (
    account   = SF_ACCOUNT,
    role      = SF_ROLE,
    user      = SF_USER,
    password  = SF_PASSWORD,
    database  = SF_DATABASE,
    schema    = SF_SCHEMA,
    warehouse = SF_WAREHOUSE,
    authenticator='externalbrowser'
)
 

""" Build a list to get table names under database EDM_CONFIRMED_DEV with schema  DW_C_PRODUCT"""
q_tbl_nm = f"""SELECT table_name  FROM {db_nm}.INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = '{sch_nm}' 
  and  table_name not like '%_COPY'  and  table_name not like  '%_TEST' 
  and  table_name not like '%_TMP'   and  table_name not like  '%_WRK%'
  and  table_name not like '%_FLAT'  and  table_name not like  '%_TEMP'
  and  table_name not like '%_TST'
df_q_tbl_nm = cf_cr_df_cur_qry(q_tbl_nm)"""

""" 
- Create a dataframe from the q_tbl_nm qyery result 
- Create a list of tbl_nm and flatten the list
"""
df_q_tbl_nm = cf_cr_df_cur_qry(q_tbl_nm)
l_tbl_1 =  df_q_tbl_nm.values.tolist() # list with list: [['BANNER'], ['BOD_LAST_RUN_STATUS'], ...., ['ZZ_TST_ECAT']]
l_tbl_2 = [item for sublist in l_tbl_1 for item in sublist] # list: ['BANNER', 'BOD_LAST_RUN_STATUS', ...., 'ZZ_TST_ECAT']



""" #Build a dictionary to save count from all the tables been selected"""
d_tbl_cnt = {}
for tbl_nm in l_tbl_2:   
    q_cnt= f"select count(*) from EDM_CONFIRMED_DEV.DW_C_PRODUCT.{tbl_nm}"
    cur_q_cnt  = connection.cursor().execute(q_cnt)  
    for row in cur_q_cnt:
        cnt = row[0]
        if cnt >  cnt_lmt_x:
           d_tbl_cnt[tbl_nm] = cnt
        
     
""" 
Create profile reports
 - Will base on count of table and cnt_lmt_y to get smp_pct to sample the rows for creating profile report  
 - Table CONSUMER_WARNING will cause integer converting to floating overflow error
   - Will exclude the table 
 -  For a table with count > closed to 1,000,000, it'd take more than 2 hours to complete the profile report 

"""

l_tbl_exclu = ['CONSUMER_WARNING']
for tbl_nm, cnt in d_tbl_cnt.items():
    print ('tbl_name', tbl_nm, 'cnt', cnt, 'cnt_lmt_y', cnt_lmt_y  )
    smp_pct = 100.00
    if tbl_nm in l_tbl_exclu:
       print (f"Exclude table: {tbl_nm}") 
    else:
      if cnt >  cnt_lmt_y :
           print('big') 
           smp_pct = round(cnt_lmt_y/cnt * 100, 0) 
      print(f'table_name: {tbl_nm}, smp_pct = {smp_pct}')
      s_col = cf_get_col_nm(tbl_nm)
      q_sel_data = f'select {s_col}  from EDM_CONFIRMED_DEV.DW_C_PRODUCT.{tbl_nm} tablesample bernoulli ({smp_pct})'
      file_nm_low = tbl_nm.lower()
      int_smp_pct = int(smp_pct)
      file_nm = f'{tbl_nm}_{int_smp_pct}pct.html'
      df_q_sel_data = cf_cr_df_cur_qry(q_sel_data) 
      cf_cr_data_profile_rpt(df_q_sel_data, file_nm)
   
