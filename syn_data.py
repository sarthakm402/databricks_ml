import pandas as pd
import numpy as np
import random
df_tran=pd.read_csv(r"/mnt/c/Users/sarthak mohapatra/Downloads/archive/fraudTrain.csv") 
# print(df_tran.iloc[:,2].count())
# print(len(df_tran))
# print(df_tran.count())
# print(df_tran.info())
# print(df_tran.head())
df_tran["event_ts"]=pd.to_datetime(df_tran["trans_date_trans_time"])
rows=10000
sample=5
np.random.seed(42)
#generated duplicates
syn_subset = [
    df_tran.sample(n=rows, random_state=i)
    for i in range(sample)
]

syn_df = pd.concat(syn_subset, ignore_index=True)
df = pd.concat([df_tran, syn_df], ignore_index=True)
print(df.count())

#generated nulls
null_rows=np.random.sample(len(df))<0.2# note - means select 20 percent of random rows from total
null_df=df.loc[null_rows].copy()
null_df["merchant"]=None
null_df["category"]=None
df = pd.concat([df, null_df], ignore_index=True)
#generated skew data
skew_rows=np.random.sample(len(df))<0.02
skew_df=df.loc[skew_rows].copy()
skew_data=pd.concat([skew_df]*50,ignore_index=True)
df=pd.concat([skew_data,df],ignore_index=True)

#generated duplicates with different values

duplicate_rows=np.random.sample(len(df))<0.1
duplicate_df=df.loc[duplicate_rows].copy()
duplicate_df["amt"]=duplicate_df["amt"]*50
duplicate_data=pd.concat([duplicate_df]*10,ignore_index=True)
df=pd.concat([duplicate_data,df],ignore_index=True)

#generatting late arrival events
late_rows = np.random.sample(len(df)) < 0.1
late_df = df.loc[late_rows].copy()
late_df['event_ts'] -= pd.to_timedelta(np.random.randint(1, 4, size=len(late_df)), unit='D')
df = pd.concat([df, late_df], ignore_index=True)




