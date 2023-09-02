from analy_data import analy_data
from node_weight import weight_checker
from search_class import search_Class

import time
import pymysql

class get_data:

    def __init__(self):
        super().__init__()
        self.pd = None
        self.sc = None
        self.ad = None
        self.kafka_worker = None
        self.wc = None

        self.IP = None
        self.Port = None
        self.ID = None
        self.PW = None
        self.DB_Name = None

        self.last_exit_rout = []
        self.last_danger_level = []

        self.func_init()
        self.set_default_param()

    def func_init(self):
        self.sc = search_Class()
        self.ad = analy_data()
        self.wc = weight_checker()

    def set_default_param(self):
        self.IP = '183.99.41.239'
        self.Port = '23306'
        self.ID = 'root'
        self.PW = 'hbrain0372!'
        self.DB_Name = 'firetest'

        self.last_danger_level = []
        self.last_exit_rout = []
        for _ in range(61):
            default_danger_level = [[], [], [], [], []]
            default_exit_route = {}
            for floor in range(5):
                if floor == 0:
                    for room in range(2):
                        default_exit_route[str(floor + 1) + '0' + str(room + 1)] = 0
                    for node in range(5):
                        default_danger_level[floor].append(0.0)

                else:
                    for room in range(7):
                        default_exit_route[str(floor + 1) + '0' + str(room + 1)] = 0
                    for node in range(11):
                        default_danger_level[floor].append(0.0)
            self.last_danger_level.append(default_danger_level)
            self.last_exit_rout.append(default_exit_route)

        for floor in range(4):
            self.sc.scenario_data[floor]['index'] = -1
            self.sc.scenario_data[floor]['start_time'] = None
            self.sc.scenario_data[floor]['diff'] = -1
            self.sc.scenario_data[floor]['data'] = {}

    def split_datas_sensor_type(self, head, datas):
        total_temp_datas = []
        total_gas_datas = []
        for data in datas:
            temp_datas = [[], [], [], [], [], []]
            gas_datas = [[], [], [], [], [], []]
            for floor, (types, values) in enumerate(zip(head, data[1:])):
                for type, value in zip(types, values):
                    if type == 'temp':
                        temp_datas[floor].append(value)
                    elif type == 'gas':
                        gas_datas[floor].append(value)
            total_temp_datas.append([data[0]] + temp_datas)
            total_gas_datas.append([data[0]] + gas_datas)
        return total_temp_datas, total_gas_datas

    def update_danger_level_DB(self, time, danger_level):
        danger_level_sql = 'UPDATE danger_level SET '
        for floor, floor_danger_level in enumerate(danger_level):
            floor += 1
            for node, node_danger_level in enumerate(floor_danger_level):
                node += 1
                if floor == 1:
                    if node <= 2:
                        danger_level_sql += ('`' + str(floor) + '0' + str(node) + '` = ' + "{:.1f}".format(
                            node_danger_level) + ', ')
                    else:
                        danger_level_sql += ('`' + str(floor) + '-' + str(node - 2) + '` = ' + "{:.1f}".format(
                            node_danger_level) + ', ')
                else:
                    if node <= 7:
                        danger_level_sql += ('`' + str(floor) + '0' + str(node) + '` = ' + "{:.1f}".format(
                            node_danger_level) + ', ')
                    else:
                        danger_level_sql += ('`' + str(floor) + '-' + str(node - 7) + '` = ' + "{:.1f}".format(
                            node_danger_level) + ', ')

        danger_level_sql = danger_level_sql[:-2] + ' WHERE time = ' + str(time) + ';'
        try:
            with pymysql.connect(host=self.IP, port=int(self.Port), user=self.ID, password=self.PW, db=self.DB_Name,
                                 charset='utf8', autocommit=True, read_timeout=5, write_timeout=5,
                                 connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute(danger_level_sql)
        except Exception as e:
            print(e)

    def update_exit_rout_DB(self, time, exit_routes):
        exit_route_sql = 'UPDATE exit_route SET '
        for room in exit_routes.keys():
            exit_route_sql += ('`' + room + '` = ' + str(exit_routes[room]) + ', ')
        exit_route_sql = exit_route_sql[:-2] + ' WHERE time = ' + str(time) + ';'

        try:
            with pymysql.connect(host=self.IP, port=int(self.Port), user=self.ID, password=self.PW, db=self.DB_Name,
                                 charset='utf8', autocommit=True, read_timeout=5, write_timeout=5,
                                 connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute(exit_route_sql)
        except Exception as e:
            print(e)

    def check_danger_level_changed(self, last_data, new_data):
        output = False
        for last_floor_data, new_floor_data in zip(last_data, new_data):
            for last_value, new_value in zip(last_floor_data, new_floor_data):
                if last_value != new_value:
                    output = True
        return output

    def check_exit_route_changed(self, last_data, new_data):
        output = False

        for key in new_data.keys():
            if last_data[key] != new_data[key]:
                output = True

        return output

    def check_scenario_changed(self, last_data, new_data):
        output = False

        for floor, (last_index, new_index) in enumerate(zip(last_data, new_data)):
            if last_index != new_index:
                self.sc.scenario_data[floor]['index'] = new_data[floor]
                self.sc.load_scenario(floor)
                output = True

        return output

    def search_all_eixt_route(self, weight):
        exit_routs = {}
        for floor in range(5):
            if floor == 0:
                for room in range(2):
                    start_node = 'room' + str(floor) + str(room)
                    path_route = self.sc.rs.search(weight, start_node)
                    if path_route[-1] == 'escape00':
                        exit_routs[str(floor + 1) + '0' + str(room + 1)] = 0
                    elif path_route[-1] == 'escape01':
                        exit_routs[str(floor + 1) + '0' + str(room + 1)] = 1
            else:
                for room in range(7):
                    start_node = 'room' + str(floor) + str(room)
                    path_route = self.sc.rs.search(weight, start_node)
                    if path_route[-1] == 'escape00':
                        exit_routs[str(floor + 1) + '0' + str(room + 1)] = 0
                    elif path_route[-1] == 'escape01':
                        exit_routs[str(floor + 1) + '0' + str(room + 1)] = 1
        return exit_routs

    def data_analy(self, head, total_temp_idx, total_gas_idx, total_datas):
        try:
            #region get data
            temp_datas, gas_datas = self.split_datas_sensor_type(head, total_datas)
            #endregion

            #region check Fire
            Fire_status, temp_idx, gas_idx = self.ad.check_danger(temp_datas, gas_datas)
            print(Fire_status)


            for floor, last, new in zip(range(5), self.sc.Fire, Fire_status):
                if (not last) and (new):
                    if (floor >= 1) and (floor <= 4):
                        self.sc.scenario_data[floor - 1]['start_time'] = int(time.time())
                        min_time = time.time()
                        for dif_floor in range(4):
                            if (floor - 1) != dif_floor:
                                if self.sc.scenario_data[dif_floor]['start_time'] is not None:
                                    if (self.sc.scenario_data[dif_floor]['start_time'] - min_time) <= 0:
                                        min_time = self.sc.scenario_data[dif_floor]['start_time']
                        for dif_floor in range(4):
                            if self.sc.scenario_data[dif_floor]['start_time'] is not None:
                                self.sc.scenario_data[dif_floor]['diff'] = int(int(self.sc.scenario_data[dif_floor]['start_time'] - min_time) / 60.0)

            """
            insert kafka code 
            [False, False, False, False, False] 
            [False, True, False, False, False]  화재 발생
            """
            self.sc.Fire = Fire_status
            #endregion

            # region real time DB Update
            real_time = 0
            danger_level = []
            for floor in range(6):
                danger_level.append(self.sc.set_danger_level_Sensor(floor, head[floor], total_datas[-1][floor + 1]))

            danger_level_change = self.check_danger_level_changed(self.last_danger_level[real_time], danger_level)

            if danger_level_change:
                self.last_danger_level[real_time] = danger_level
                self.update_danger_level_DB(real_time, danger_level)

            self.wc.set_node_weight_useSensor(temp_idx, gas_idx, total_temp_idx, total_gas_idx)
            exit_routs = self.search_all_eixt_route(self.wc.node)

            exit_rout_change = self.check_exit_route_changed(self.last_exit_rout[real_time], exit_routs)
            if exit_rout_change:
                self.last_exit_rout[real_time] = exit_routs
                self.update_exit_rout_DB(real_time, exit_routs)

            # endregion

            if True in self.sc.Fire:
                scenario_idx = self.ad.check_scenario(self.sc.Fire, temp_datas, gas_datas)
                print(scenario_idx)
                change_scenario = self.check_scenario_changed([floor_data['index'] for floor_data in self.sc.scenario_data], scenario_idx)

                # region future DB Update

                floor_idx = [[0, 47], [47, 94], [94, 141], [141, 188]]
                if change_scenario:
                    for minute in range(1, 61):
                        layer_height_data = [3.0 for _ in range(188)]

                        danger_level = []
                        for floor in range(5):
                            data = []
                            if floor != 0:
                                if self.sc.scenario_data[floor - 1]['index'] != -1:
                                    search_time = minute - self.sc.scenario_data[floor - 1]['diff'] + 1
                                    if search_time >= 60:
                                        search_time = 60
                                    elif search_time <= 1:
                                        search_time = 1
                                    data = self.sc.scenario_data[floor - 1]['data'][str(search_time)]
                                    layer_height_data[floor_idx[floor - 1][0]:floor_idx[floor - 1][1]] = data
                            danger_level.append(self.sc.set_danger_level_Layerheight(floor, data))

                        danger_level_change = self.check_danger_level_changed(self.last_danger_level[minute], danger_level)
                        if danger_level_change:
                            self.last_danger_level[minute] = danger_level
                            self.update_danger_level_DB(minute, danger_level)

                        self.wc.set_node_weight_useLayerheight(layer_height_data)
                        exit_routs = self.search_all_eixt_route(self.wc.node)
                        exit_rout_change = self.check_exit_route_changed(self.last_exit_rout[minute], exit_routs)
                        if exit_rout_change:
                            self.last_exit_rout[minute] = exit_routs
                            self.update_exit_rout_DB(minute, exit_routs)

                #endregion


            else:
                self.sc.Fire = [False for _ in range(5)]

        except Exception as e:
            print(e)
