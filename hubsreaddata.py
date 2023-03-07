#import json
import simplejson as json
import pandas as pd
import numpy as np

parquet_file =  r"C:\Users\Admin\Desktop\Dataset\hubsdataset.parquet"


df=pd.read_parquet(parquet_file, engine='auto', columns=None, storage_options=None, use_nullable_dtypes=False)



dfnonull = df[df['holes'].notna()]

#print(dfnonull)


new_df = dfnonull[['holes']].copy()

print(new_df)

#new_df = new_df.fillna(np.nan).replace([np.nan], [None])


#rint(new_df)

for i in range(len(new_df)):


        jsondata = new_df['holes'].values[i]
        data = json.loads(jsondata)
       # data= json.dumps(jsondata, allow_nan=True)
        #print("Type:", type(data))
        # print(data)
        dfjson = pd.json_normalize(data)

        #print(dfjson)

dfjson["has_unreachable_hole_warning"] = dfjson.apply(lambda x: x["length"] > x["radius"] * (2 * 10) , axis = 1)
dfjson["has_unreacheable_hole_error"] = dfjson.apply(lambda x: x["length"] > x["radius"] * (2 * 40) , axis = 1)


print(dfjson[['radius','length','has_unreachable_hole_warning','has_unreacheable_hole_error']])

def warning():

result= dfjson['has_unreachable_hole_warning'].all(axis=0)

#print(dfjson.has_unreacheable_hole_error.notna())
#print(df['has_unreacheable_hole_error'].all())


#df.to_parquet(r"C:\Users\Admin\Desktop\Dataset\tranformddat.parquet")



#print(df.head(200).to_csv(r"C:\Users\Admin\Desktop\Dataset\transformedcsvdata.csv",header=True,))
#df.columns = df.iloc[-1]
