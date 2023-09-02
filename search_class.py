from route_search import route_search
import csv


class search_Class:
    def __init__(self):
        super().__init__()
        self.Fire = [False, False, False, False, False, False]
        self.set_time = 0
        self.time_gap = 0
        self.scenario_data = [{'index': -1, 'start_time': None, 'diff': -1, 'data': {}},
                              {'index': -1, 'start_time': None, 'diff': -1, 'data': {}},
                              {'index': -1, 'start_time': None, 'diff': -1, 'data': {}},
                              {'index': -1, 'start_time': None, 'diff': -1, 'data': {}}]

        self.rs = route_search()

    def load_scenario(self, floor):
        Dir_path = './test.csv'#'D:/.2. 4차년도 국토부/data/'
        # if self.scenario[floor]['index'] >= 0:
        #     File_path = os.path.join(Dir_path, 'M' + format(self.scenario[floor]['index'], '05') + '.csv')

        with open(Dir_path, 'r', encoding='utf-8-sig') as f:
            time_value = csv.reader(f)
            for frame_datas in time_value:
                self.scenario_data[floor]['data'][str(int(float(frame_datas[0])))] = frame_datas[1:]

    def set_danger_level_Sensor(self, floor, head, data):
        floor_danger_level = []
        if floor == 0:
            if len(data) != 0:
                underfloor = [head[0:3], data[0:3]]
                outside = [head[3:6], data[3:6]]
                room1 = [head[9], data[9]]
                room2 = [head[7], data[7]]
                door = [head[6], data[6]]
                hall = [head[8], data[8]]
                stair = [head[10], data[10]]
                room_data = [room1, room2]
                hallway_data = [door, hall]

                for idx, room in enumerate(room_data):
                    temp_red = 0.0
                    gas_red = 0.0
                    if room[0] == 'temp':
                        temp_red = ((float(room[1]) - 20.0) / 100.0) if ((float(room[1]) - 20.0) <= 100) else 1.0
                    elif room[0] == 'gas':
                        gas_red = (float(room[1]) * 2) if ((float(room[1]) * 2) <= 1.0) else 1.0
                    red_value = round(max(temp_red, gas_red) * 2.0, 1)
                    floor_danger_level.append(red_value)

                for idx, hallway in enumerate(hallway_data):
                    temp_red = 0.0
                    gas_red = 0.0
                    if hallway[0] == 'temp':
                        temp_red = ((float(hallway[1]) - 20.0) / 100.0) if ((float(hallway[1]) - 20.0) <= 100) else 1.0
                    elif hallway[0] == 'gas':
                        gas_red = (float(hallway[1]) * 2) if ((float(hallway[1]) * 2) <= 1.0) else 1.0
                    red_value = round(max(temp_red, gas_red) * 2.0, 1)
                    floor_danger_level.append(red_value)

                temp_red = 0.0
                gas_red = 0.0
                if stair[0] == 'temp':
                    temp_red = (float(stair[1]) / 100.0) if (float(stair[1]) <= 100) else 1.0
                elif stair[0] == 'gas':
                    gas_red = (float(stair[1]) * 2) if ((float(stair[1]) * 2) <= 1.0) else 1.0
                red_value = round(max(temp_red, gas_red) * 2.0, 1)
                floor_danger_level.append(red_value)

        elif floor >= 5:
            a = 0

        else:
            if len(data) != 0:
                room1 = [head[0:7], data[0:7]]
                room2 = [head[7:13], data[7:13]]
                room3 = [head[13:19], data[13:19]]
                room4 = [head[19:25], data[19:25]]
                room5 = [head[25:31], data[25:31]]
                room6 = [head[31:37], data[31:37]]
                room7 = [head[37:44], data[37:44]]
                hallway1 = [head[44:45], data[44:45]]
                hallway2 = [head[45:47], data[45:47]]
                hallway3 = [head[47:48], data[47:48]]
                stair = [head[48:49], data[48:49]]
                room_data = [room1, room2, room3, room4, room5, room6, room7]
                hallway_data = [hallway1, hallway2, hallway3]

                for room, room_data in enumerate(room_data):
                    room_danger_level = [['temp', 0.0], ['gas', 0.0]]
                    for i in range(len(room_data[0])):
                        sensor_danger_level = [room_data[0][i], float(room_data[1][i])]
                        sensor_type = 0 if sensor_danger_level[0] == 'temp' else 1
                        if room_danger_level[sensor_type][1] <= sensor_danger_level[1]:
                            room_danger_level[sensor_type] = sensor_danger_level

                    temp_red = ((room_danger_level[0][1] - 20.0) / 100.0) if ((room_danger_level[0][1] - 20.0) <= 100) else 1.0
                    gas_red = (room_danger_level[1][1] * 2) if ((room_danger_level[1][1] * 2) <= 1.0) else 1.0
                    red_value = round(max(temp_red, gas_red) * 2.0, 1)
                    floor_danger_level.append(red_value)

                for hallway, hallway_data in enumerate(hallway_data):
                    hallway_danger_level = [['temp', 0.0], ['gas', 0.0]]
                    for i in range(len(hallway_data[0])):
                        sensor_danger_level = [hallway_data[0][i], float(hallway_data[1][i])]
                        sensor_type = 0 if sensor_danger_level[0] == 'temp' else 1
                        if hallway_danger_level[sensor_type][1] <= sensor_danger_level[1]:
                            hallway_danger_level[sensor_type] = sensor_danger_level

                    temp_red = ((hallway_danger_level[0][1] - 20.0) / 100.0) if ((hallway_danger_level[0][1] - 20.0) <= 100) else 1.0
                    gas_red = (hallway_danger_level[1][1] * 2) if ((hallway_danger_level[1][1] * 2) <= 1.0) else 1.0
                    red_value = round(max(temp_red, gas_red) * 2.0, 1)
                    floor_danger_level.append(red_value)

                for i in range(len(stair[0])):
                    temp_red = 0.0
                    gas_red = 0.0
                    if stair[0] == 'temp':
                        temp_red = ((float(stair[1][i]) - 20.0) / 100.0) if ((float(stair[1][i]) - 20.0) <= 100) else 1.0
                    elif stair[0][i] == 'gas':
                        gas_red = (float(stair[1][i]) * 2) if ((float(stair[1][i]) * 2) <= 1.0) else 1.0
                    red_value = round(max(temp_red, gas_red) * 2.0, 1)
                    floor_danger_level.append(red_value)

        return floor_danger_level

    def set_danger_level_Layerheight(self, floor, data):
        floor_danger_level = []
        if floor == 0:
            for _ in range(5):
                floor_danger_level.append(0.0)

        elif floor >= 5:
            a = 0

        else:
            if len(data) != 0:
                room1 = data[0:7]
                room2 = data[8:14]
                room3 = data[14:20]
                room4 = data[22:28]
                room5 = data[28:34]
                room6 = data[34:40]
                room7 = data[40:47]
                hallway1 = data[7]
                hallway2 = data[20]
                hallway3 = data[21]
                room_data = [room1, room2, room3, room4, room5, room6, room7]
                hallway_data = [hallway1, hallway2, hallway3]

                for room, room_data in enumerate(room_data):
                    red_value = 0.0
                    for value in room_data:
                        value = float(value)
                        if value >= 2.3:
                            value = 2.3
                        if value <= 1.0:
                            value = 1.0
                        val = round((((value * -1.0) + 2.3) / 1.3) * 2.0, 1)
                        if red_value <= val:
                            red_value = val
                    floor_danger_level.append(red_value)

                for hallway, value in enumerate(hallway_data):
                    value = float(value)
                    red_value = value
                    if red_value >= 2.3:
                        red_value = 2.3
                    if red_value <= 1.0:
                        red_value = 1.0
                    red_value = round((((red_value * -1.0) + 2.3) / 1.3) * 2.0, 1)
                    floor_danger_level.append(red_value)

                red_value = 0.0
                floor_danger_level.append(red_value)
            else:
                for _ in range(11):
                    floor_danger_level.append(0.0)
        return floor_danger_level
