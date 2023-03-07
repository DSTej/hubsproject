"""
𝑙𝑒𝑛𝑔𝑡ℎ > 𝑟𝑎𝑑𝑖𝑢𝑠 * 2 * 10 (𝑝𝑜𝑜𝑟 𝑟𝑎𝑡𝑖𝑜)
𝑙𝑒𝑛𝑔𝑡ℎ > 𝑟𝑎𝑑𝑖𝑢𝑠 * 2 * 40 (𝑐𝑟𝑖𝑡𝑖𝑐𝑎𝑙 𝑟𝑎𝑡𝑖𝑜)

['__class__', '__delattr__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__getitem__', '__gt__', '__hash__', '__init__',
'__init_subclass__', '__le__', '__len__', '__lt__', '__ne__', '__new__', '__pyx_vtable__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__setstate__', '
__sizeof__', '__str__', '__subclasshook__', '_column', '_ensure_integer_index', '_to_pandas', 'add_column', 'append_column', 'cast', 'column',
'column_names', 'columns', 'combine_chunks', 'drop', 'drop_null', 'equals', 'field', 'filter', 'flatten', 'from_arrays',
'from_batches', 'from_pandas', 'from_pydict', 'from_pylist', 'get_total_buffer_size', 'group_by', 'itercolumns', 'join',
'nbytes', 'num_columns', 'num_rows', 'remove_column', 'rename_columns', 'replace_schema_metadata', 'schema', 'select', 'set_column', 'shape',
'slice', 'sort_by', 'take', 'to_batches', 'to_pandas', 'to_pydict', 'to_pylist', 'to_reader', 'to_string', 'unify_dictionaries', 'validate']
"""

import json
import pyarrow as pa # pip install the package
import pandas as pd
parquet_file =  r"C:\Users\Admin\Desktop\Dataset\hubsdataset.parquet"

#pd.read.parquet(parquet_file, engine="auto")
df=pd.read_parquet(parquet_file, engine='pyarrow', columns=None, storage_options=None, use_nullable_dtypes=False)

print(df.columns)

table = pa.Table.from_pandas(df)

print(table)

print(table.num_rows, table.num_columns)

# Each Holes row data looks like below
"""
{"center":{"x":4.254160968e-24,"y":8.7312498093,"z":-4.254160978e-24},
"direction":{"x":0,"y":-1,"z":0},"end1":{"closed":true,"reachable":false},
"end2":{"closed":false,"reachable":true},
"facet_count":228,"length":17.4624996185,"radius":8.4074001312}
"""

with open("data.json", "w+") as fileobj:
    json.dump(table.to_pydict(), fileobj)

with open("data.json", "rb") as fileobj:
        data = json.load(fileobj)
        with open("holes.json", "w+") as fileobj:
            json.dump(data["holes"], fileobj)

length = 17.462
radius = 8.407

poor_ratio = length > radius * (2 * 10)
print(radius * (2 * 10))
print(poor_ratio)
print(radius * (2 * 40))

# #dfdropped = df.dropna(how='all',axis = 0)
# dfnonull = df[df['holes'].notna()]
# new_df = dfnonull[['holes']].copy()
# print(new_df)
# #Get a cell value
# print(new_df["holes"].values[2])
# datadict = new_df.to_dict()
# for key, value in datadict.items():
#     print("Key", key, "Value", value)
