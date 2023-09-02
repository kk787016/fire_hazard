import queue
import threading

import numpy as np
import torch
import torch.nn as nn
from pyts.image import RecurrencePlot
from model import resnet18

device = 'cuda' if torch.cuda.is_available() else 'cpu'
torch.manual_seed(777)
if device == 'cuda':
    torch.cuda.manual_seed_all(777)




RP = RecurrencePlot()

class FireDetection_thread():
    def __init__(self):
        super().__init__( )

        self.rp = RecurrencePlot()

        self.temp_datas = None
        self.gas_datas = None
        self.output = []
        self.fire_Detect_model_temp = resnet18()
        self.fire_Detect_model_temp = nn.DataParallel(self.fire_Detect_model_temp).to(device)
        self.fire_Detect_model_temp.load_state_dict(torch.load("model/weight.pt", map_location=torch.device('cpu')))
        self.fire_Detect_model_temp.eval()

        self.fire_Detect_model_gas = resnet18()
        self.fire_Detect_model_gas = nn.DataParallel(self.fire_Detect_model_gas).to(device)
        self.fire_Detect_model_gas.load_state_dict(torch.load("model/weight.pt", map_location=torch.device('cpu')))
        self.fire_Detect_model_gas.eval()

        self.scenario_serach_model = resnet18()
        self.scenario_serach_model = nn.DataParallel(self.scenario_serach_model).to(device)
        self.scenario_serach_model.load_state_dict(torch.load("model/weight.pt", map_location=torch.device('cpu')))
        self.scenario_serach_model.eval()

    def run(self, output_q):
        input_temp_datas = self.temp_datas.copy()
        input_gas_datas = self.gas_datas.copy()

        total_temp_datas = [[], [], [], [], [], []]
        total_gas_datas = [[], [], [], [], [], []]
        for frame_temp_datas in input_temp_datas:
            for floor, floor_temp_datas in enumerate(frame_temp_datas[1:]):
                total_temp_datas[floor].append(floor_temp_datas)
        for frame_gas_datas in input_gas_datas:
            for floor, floor_gas_datas in enumerate(frame_gas_datas[1:]):
                total_gas_datas[floor].append(floor_gas_datas)

        danger_temp_idx = [[], [], [], [], [], []]
        danger_gas_idx = [[], [], [], [], [], []]

        for floor, floor_temp_datas in enumerate(total_temp_datas):
            for idx, value in enumerate(floor_temp_datas[-1]):
                if float(value) >= 22.0:
                    if not(idx in danger_temp_idx[floor]):
                        danger_temp_idx[floor].append(idx)
            total_temp_datas_reshaped = floor_temp_datas.copy()
            total_temp_datas_reshaped = list(zip(*total_temp_datas_reshaped))
            for idx, input_datas_str in enumerate(total_temp_datas_reshaped):
                input_datas_float = [float(value) for value in input_datas_str]
                input_data = (torch.tensor(self.rp.fit_transform(np.array([input_datas_float]))).unsqueeze(dim=0)).to(device, dtype=torch.float)
                output = self.fire_Detect_model_temp(input_data)
                pred = output.argmax(dim=1, keepdim=True)
                if pred == 1:
                    if not(idx in danger_temp_idx[floor]):
                        danger_temp_idx[floor].append(idx)

        for floor, floor_gas_datas in enumerate(total_gas_datas):
            for idx, value in enumerate(floor_gas_datas[-1]):
                if float(value) >= 0.01:
                    if not(idx in danger_gas_idx[floor]):
                        danger_gas_idx[floor].append(idx)
            total_gas_datas_reshaped = floor_gas_datas.copy()
            total_gas_datas_reshaped = list(zip(*total_gas_datas_reshaped))
            for idx, input_datas_str in enumerate(total_gas_datas_reshaped):
                input_datas_float = [float(value) for value in input_datas_str]
                input_data = (torch.tensor(self.rp.fit_transform(np.array([input_datas_float]))).unsqueeze(dim=0)).to(device, dtype=torch.float)
                output = self.fire_Detect_model_gas(input_data)
                pred = output.argmax(dim=1, keepdim=True)
                if pred == 1:
                    if not(idx in danger_gas_idx[floor]):
                        danger_gas_idx[floor].append(idx)

        output_q.put([danger_temp_idx, danger_gas_idx])

class scenarioMatching_thread():
    def __init__(self):
        super().__init__( )

        self.scenario = [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]] # [level, start_room, fire_level, spread_level, door_status]
        self.Fire = None
        self.temp_datas = None
        self.gas_datas = None


    def run(self, output_q):
        Fire = self.Fire
        input_temp_datas = self.temp_datas.copy()
        input_gas_datas = self.gas_datas.copy()

        for floor, danger in enumerate(Fire):
            if floor != 0:
                if danger:
                    # if self.scenario[floor - 1][0] == 0:
                    #     self.scenario[floor - 1][0] += 1
                    # 
                    #     floor_temp_datas = [frame_data[floor + 1] for frame_data in input_temp_datas]
                    #     floor_gas_datas = [frame_data[floor + 1][:-5] for frame_data in input_gas_datas]
                    # 
                    #     total_temp_datas_reshaped = floor_temp_datas.copy()
                    #     total_temp_datas_reshaped = list(zip(*total_temp_datas_reshaped))
                    #     for idx, input_datas_str in enumerate(total_temp_datas_reshaped):
                    #         input_datas_float = [float(value) for value in input_datas_str]
                    #         input_data = (torch.tensor(self.rp.fit_transform(np.array([input_datas_float]))).unsqueeze(dim=0)).to(device, dtype=torch.float)
                    #         output = self.fire_Detect_model_temp(input_data)
                    #         pred = output.argmax(dim=1, keepdim=True)
                    #         self.scenario[floor - 1][1] = pred
                    # 
                    # elif self.scenario[floor - 1][0] == 1:
                    #     self.scenario[floor - 1][0] += 1
                    # 
                    #     floor_temp_datas = [frame_data[floor + 1] for frame_data in input_temp_datas]
                    #     floor_gas_datas = [frame_data[floor + 1][:-5] for frame_data in input_gas_datas]
                    # 
                    #     total_temp_datas_reshaped = floor_temp_datas.copy()
                    #     total_temp_datas_reshaped = list(zip(*total_temp_datas_reshaped))
                    #     for idx, input_datas_str in enumerate(total_temp_datas_reshaped):
                    #         input_datas_float = [float(value) for value in input_datas_str]
                    #         input_data = (torch.tensor(self.rp.fit_transform(np.array([input_datas_float]))).unsqueeze(dim=0)).to(device, dtype=torch.float)
                    #         output = self.fire_Detect_model_temp(input_data)
                    #         pred = output.argmax(dim=1, keepdim=True)
                    #         self.scenario[floor - 1][1] = pred

                    DL_output  = 3
                    self.scenario[floor - 1] = DL_output

        output_q.put(self.scenario)

class analy_data:
    def __init__(self):
        super().__init__()

        self.FireDetection_worker = FireDetection_thread()
        self.scenarioMatching_worker = scenarioMatching_thread()
        self.warning_count = 0
        self.fire_count = 0
        self.Fire_status = False
        self.temp_Fire_idx = []
        self.gas_Fire_idx = []
        self.scenario_idx = [-1, -1, -1, -1]

        self.FireDetection_q = queue.Queue()
        self.ScenarioMatching_q = queue.Queue()
        self.FireDetection_thread = threading.Thread(target=self.FireDetection_worker.run, args=(self.FireDetection_q,))
        self.ScenarioMatching_thread = threading.Thread(target=self.scenarioMatching_worker.run, args=(self.ScenarioMatching_q,))

    def scenarioMatching_data_analy(self, scenario_output):
        self.scenario_idx = scenario_output

    def check_danger(self, temp_datas, gas_datas):

        self.FireDetection_worker.temp_datas = temp_datas
        self.FireDetection_worker.gas_datas = gas_datas

        if not self.FireDetection_thread.is_alive():
            self.FireDetection_thread = threading.Thread(target=self.FireDetection_worker.run, args=(self.FireDetection_q,))
            self.FireDetection_thread.start()

        while self.FireDetection_q.qsize():
            thread_output = self.FireDetection_q.get()
            self.temp_Fire_idx = thread_output[0]
            self.gas_Fire_idx = thread_output[1]

        danger = [False for i in range(5)]
        for floor, floor_idx in enumerate(self.temp_Fire_idx):
            if len(floor_idx) != 0:
                danger[floor] = True
        for floor, floor_idx in enumerate(self.gas_Fire_idx):
            if len(floor_idx) != 0:
                danger[floor] = True
        self.Fire_status = danger

        return self.Fire_status, self.temp_Fire_idx, self.gas_Fire_idx

    def check_scenario(self, Fire, temp_datas, gas_datas):

        self.scenarioMatching_worker.Fire = Fire
        self.scenarioMatching_worker.temp_datas = temp_datas
        self.scenarioMatching_worker.gas_datas = gas_datas

        if not self.ScenarioMatching_thread.is_alive():
            self.ScenarioMatching_thread = threading.Thread(target=self.scenarioMatching_worker.run, args=(self.ScenarioMatching_q,))
            self.ScenarioMatching_thread.start()


        while self.ScenarioMatching_q.qsize():
            self.scenario_idx = self.ScenarioMatching_q.get()

        return self.scenario_idx
