import pandas as pd
import inspect
import numpy as np

def retrieve_name(var):
        """
        Gets the name of var. Does it from the out most frame inner-wards.
        :param var: variable to get name from.
        :return: string
        """
        for fi in reversed(inspect.stack()):
            names = [var_name for var_name, var_val in fi.frame.f_locals.items() if var_val is var]
            if len(names) > 0:
                return names[0]

# on a besoin de :
# 1 csv par jour pour toutes les rooms
# room_number, type (hr, pedo, location) (device_cid) (check s'il y en a d'autres), value, time (phenomenonTime)

room_numbers = [1014, 1026, 2002, 2008, 2011, 2018, 2030, 4008]
rooms = []

for number in room_numbers:
    exec(f'room_{number} = pd.read_csv("data-imported/Room-{number}.csv")') # read the csv files
    exec(f'rooms.append(room_{number})') # put them in a list

for i in range (0, len(rooms)):
    rooms[i]['room'] = 'Room-' + str(room_numbers[i]) # add the room col to each room

df = pd.concat(rooms) # merge the csvs

df["device_cid"] = df["device_cid"].str.split("/").str.get(-1) # modify values to get only the type

del df["_id"], df["_index"], df["_score"], df["_type"], df["id"], df["resultTime"] # del unused cols

df.rename(columns={'device_cid': 'type', 'phenomenonTime.instant': 'time', 'result.valueLocationGps': 'valueLocationGps', 'result.valueNumeric': 'valueNumeric'}, inplace=True) # rename cols
df.index.name = 'id'

df['valueNumeric'] = np.where(df['valueLocationGps'] != '-', df['valueLocationGps'], df['valueNumeric'])
del df["valueLocationGps"]

print('test initial merge passed') if (sum(len(room) for room in rooms)) == len(df) else print('test initial merge failed')

d2022_06_09 = df[df["time"].str.contains("Jun 9")]
d2022_06_10 = df[df["time"].str.contains("Jun 10")]
d2022_06_11 = df[df["time"].str.contains("Jun 11")]
d2022_06_12 = df[df["time"].str.contains("Jun 12")]
d2022_06_13 = df[df["time"].str.contains("Jun 13")]

days = [d2022_06_09, d2022_06_10, d2022_06_11, d2022_06_12, d2022_06_13]
daysLength = sum(map(len, days))

# test that the amount of rows is the same as the beginning
print ('test days passed') if (daysLength == len(df)) else print('Test days failed')


for i in range (0, len(rooms)):
    for day in days:
        #day.to_csv(f"data-exported/{retrieve_name(day)}.csv", sep=',', encoding='utf-8', index=False)
        dfDayRoom = day[day["room"].str.contains(str(room_numbers[i]))]
        if(len(dfDayRoom) != 0):
            fileName = str(room_numbers[i]) + "_" + retrieve_name(day).lstrip("d").replace("_", "-")
            dfDayRoom.to_csv(f"data-exported/{fileName}.csv", sep=',', encoding='utf-8', index=False)
            print(f"{fileName}.csv", "exported successfully")
        else:
            print(f"{fileName}.csv", "not exported because empty")

#df.to_csv("data-exported/all_data.csv", sep=',', encoding='utf-8', index=False)
#print('all_data.csv exported')
print(df.head(2))










