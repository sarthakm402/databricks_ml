import pandas as pd
import numpy as np
import random
df_tran=pd.read_csv(r"/mnt/c/Users/sarthak mohapatra/Downloads/archive/fraudTrain.csv")
# print(df_tran.iloc[:,2].count())
# print(len(df_tran))
# print(df_tran.count())
# print(df_tran.info())
rows=10000
sample=5
np.random.seed(42)
#generated duplicates
syn_subset = [
    df_tran.sample(n=rows, random_state=i)
    for i in range(sample)
]

syn_data = pd.concat(syn_subset, ignore_index=True)
df = pd.concat([df_tran, syn_data], ignore_index=True)
print(df.count())

#generated nulls
null_unfiltered_rows=np.random.sample(len(df))<0.2# note - means select 20 percent of random rows from total
null_rows=df.loc[null_unfiltered_rows]
df=df.loc[null_rows,["merchant","category"
]]=None

#generated skew data
skew_unfiltered_rows=np.random.sample(len(df))<0.02
skew_rows=df.loc[skew_unfiltered_rows]
skew_subset=df.sample(n=skew_rows)
skew_data=pd.concat([skew_subset]*50,ignore_index=True)
df=pd.concat([skew_data,df],ignore_index=True)

#generated duplicates with different values




