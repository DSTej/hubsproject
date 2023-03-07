import json
import pyarrow as pa
import pandas as pd

parquet_file =  r"C:\Users\Admin\Desktop\Dataset\hubsdataset.parquet"

df=pd.read_parquet(parquet_file, engine='auto', columns=None, storage_options=None, use_nullable_dtypes=False)

table = pa.Table.from_pandas(df)

with open("hubsdata.json", "w+") as fileobj:
    json.dump(table.to_pydict(), fileobj)


with open("hubsdata.json", "rb") as fileobj:
    data = json.load(fileobj)
    with open("_holes.json", "w+") as fileobj:
         json.dump(data["holes"], fileobj)


with open("_holes.json", "rb") as fileobj:
    holes_data = json.load(fileobj)
    print(type(holes_data))

def get_all_holes_length_radius(holes_json):
    length_radius_list = []
    convert_iter = iter(holes_json)
    while True:
        try:
            value = next(convert_iter)
            if value != None:
                dfjson = json.loads(value)
                length_radius_list.append([[row['length'],row['radius']] for row in dfjson if len(dfjson) > 0])
                #print(length_radius_list)
            else:
                length_radius_list.append([[0,0]])
        except Exception as e:
            break
    return length_radius_list

length_radius_list = get_all_holes_length_radius(holes_data)

# Below is the Output of the get_all_holes_length_radius function
# data = [[[0, 0]], [[0, 0]], [[17.4624996185, 8.4074001312], [1.5874996185, 9.6773998966]]]
# above return value is passed to the below has_unreachable_hole_warning and has_unreacheable_hole_error function.

def has_unreachable_hole_warning(data):
    global_data = []
    check_unreachable_hole_warning = iter(data)
    while True:
        try:
            value = next(check_unreachable_hole_warning)
            local_data = [None] * len(value)
            for index, row in enumerate(value):
                flag = (lambda x: row[0] > row[1] * (2 * 10))(row)
                local_data[index] =  flag
            global_data.append(all(local_data))
        except Exception as e:
            print(e)
            break
    df['has_unreachable_hole_warning'] = pd.DataFrame(global_data)
    print(df['has_unreachable_hole_warning'])
    return global_data

unreachable_hole_warning_data = has_unreachable_hole_warning(length_radius_list)
#print("firstone",unreachable_hole_warning_data)

def has_unreacheable_hole_error(data):
    global_data = []
    check_unreachable_hole_warning = iter(data)
    while True:
        try:
            value = next(check_unreachable_hole_warning)
            local_data = [None] * len(value)
            for index, row in enumerate(value):
                flag = (lambda x: row[0] > row[1] * (2 * 40))(row)
                local_data[index] = flag
            global_data.append(all(local_data))
            #print(global_data)

        except Exception as e:
            print(e)
            break
    df['has_unreacheable_hole_error'] = pd.DataFrame(global_data)
    print(df['has_unreacheable_hole_error'])
    return global_data

has_unreacheable_hole_error_data = has_unreacheable_hole_error(length_radius_list)
#print("secondone", has_unreacheable_hole_error_data)


print(df.head(5000).to_csv(r"C:\Users\Admin\Desktop\Dataset\transformedcsvdata.csv",header=True,))

# Please Cross-Verify the return value of the function unreachable_hole_warning_data and has_unreacheable_hole_error_data with the original value.

