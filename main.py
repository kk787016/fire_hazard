import threading

import get_data

from threading import Thread

import pymysql

from kafka import KafkaConsumer
from json import loads
from shared_data import SharedData
from collections import deque
import time

class kafka_thread(Thread):

    pem_dir = './pem/'
    caRootLocation = pem_dir + 'CARoot.pem'
    certLocation = pem_dir + 'certificate.pem'



    def __init__(self,shared_data):
        super().__init__()
        self.shared_data = shared_data
        self.values = [0] * 210
        self.max_size = 3
        self.total_deques = 210
        self.sensor_data = [[deque(maxlen=self.max_size)] for _ in range(self.total_deques)]

    def run(self):
        consumer = KafkaConsumer(
            "maintenance_in",
            #
            bootstrap_servers=["dev.iwaz.co.kr:9097"],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="hbrain1669",
            value_deserializer=lambda x: loads(x.decode('UTF-8')),
            consumer_timeout_ms=10000,
            security_protocol="SSL",
            ssl_check_hostname=True,
            ssl_cafile=self.caRootLocation,
            ssl_certfile=self.certLocation,
        )

        memory_map = [0] * 210
        memory_map[0] = 1
        memory_map[15] = 1
        memory_map[22] = 1
        memory_map[24] = 1
        memory_map[153] = 1
        memory_map[202] = 1

        memory_map_check = False
        start_time = time.time()

        for message in consumer:
            current_time = time.time()
            elapsed_time = current_time - start_time
            if int(elapsed_time) % 2 == 1:
                sum = 0
                if not memory_map_check:
                    for i in range(210):
                        sum = sum + memory_map[i]
                        if sum == 210:
                            memory_map_check = True
                elif memory_map_check:
                    for i in range(210):
                        if i == 15 or i == 22:  # temp
                            self.values[i] = 20
                        elif i == 0 or i == 24 or i == 153 or i == 202:  # gas , 0
                            self.values[i] = 0
                        else:
                            self.values[i] = (self.sensor_data[i][0][-1][1])
                    self.shared_data.set_a(self.values)

                start_time = current_time
            parsed_data = message.value

            complex_name = parsed_data.get('msg_header', {}).get('complex_name')
            if complex_name == '에너지체험하우스':
                source_key_prefix = parsed_data["msg_header"]["source_key"][:4]
                timestamp_value = parsed_data["msg_data"][0]["field_value"]
                heat_value = parsed_data["msg_data"][1]["field_value"]
                status_value = parsed_data["msg_data"][2]["field_value"]
                # region
                if source_key_prefix == "662c":
                    memory_map[1] = 1
                    self.sensor_data[1][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e139":
                    memory_map[2] = 1
                    self.sensor_data[2][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "517b":
                    memory_map[3] = 1
                    self.sensor_data[3][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "de2e":
                    memory_map[4] = 1
                    self.sensor_data[4][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "dfca":
                    memory_map[5] = 1
                    self.sensor_data[5][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7ede":
                    memory_map[6] = 1
                    self.sensor_data[6][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "951c":
                    memory_map[7] = 1
                    self.sensor_data[7][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e293":
                    memory_map[8] = 1
                    self.sensor_data[8][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d8a5":
                    memory_map[9] = 1
                    self.sensor_data[9][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8b07":
                    memory_map[10] = 1
                    self.sensor_data[10][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8cb4":
                    memory_map[11] = 1
                    self.sensor_data[11][0].append([timestamp_value, heat_value, status_value])
                # 1층
                elif source_key_prefix == "a13c":
                    memory_map[12] = 1
                    self.sensor_data[12][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "75c6":
                    memory_map[13] = 1
                    self.sensor_data[13][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "af5c":
                    memory_map[14] = 1
                    self.sensor_data[14][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7830":  # g
                    memory_map[16] = 1
                    self.sensor_data[16][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "51cc":  # g
                    memory_map[17] = 1
                    self.sensor_data[17][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "0d8e":  # g
                    memory_map[18] = 1
                    self.sensor_data[18][0].append([timestamp_value, heat_value, status_value])
                    # 현재 15(temp)까지 201호 ,
                elif source_key_prefix == "4e79":
                    memory_map[19] = 1
                    self.sensor_data[19][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "0b09":
                    memory_map[20] = 1
                    self.sensor_data[20][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a223":
                    memory_map[21] = 1
                    self.sensor_data[21][0].append([timestamp_value, heat_value, status_value])
                    # 23, 24 가 추가 데이터.
                elif source_key_prefix == "764b":  # g
                    memory_map[23] = 1
                    self.sensor_data[23][0].append([timestamp_value, heat_value, status_value])
                    # 24 는 연기 센서
                elif source_key_prefix == "5cf2":
                    memory_map[25] = 1
                    self.sensor_data[25][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "bca1":
                    memory_map[26] = 1
                    self.sensor_data[26][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "136c":
                    memory_map[27] = 1
                    self.sensor_data[27][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a339":
                    memory_map[28] = 1
                    self.sensor_data[28][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a43b":  # g
                    memory_map[29] = 1
                    self.sensor_data[29][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8068":  # g
                    memory_map[30] = 1
                    self.sensor_data[30][0].append([timestamp_value, heat_value, status_value])
                    # 까지 203호
                elif source_key_prefix == "8ae3":
                    memory_map[31] = 1
                    self.sensor_data[31][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "b5c1":
                    memory_map[32] = 1
                    self.sensor_data[32][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "26c7":
                    memory_map[33] = 1
                    self.sensor_data[33][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "64c7":
                    memory_map[34] = 1
                    self.sensor_data[34][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "83a9":
                    memory_map[35] = 1
                    self.sensor_data[35][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a16b":
                    memory_map[36] = 1
                    self.sensor_data[36][0].append([timestamp_value, heat_value, status_value])
                    # 204호
                elif source_key_prefix == "6a8b":
                    memory_map[37] = 1
                    self.sensor_data[37][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "de4b":
                    memory_map[38] = 1
                    self.sensor_data[38][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8fb0":
                    memory_map[39] = 1
                    self.sensor_data[39][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "679f":
                    memory_map[40] = 1
                    self.sensor_data[40][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7b39":  # g
                    memory_map[41] = 1
                    self.sensor_data[41][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6df7":  # g
                    memory_map[42] = 1
                    self.sensor_data[42][0].append([timestamp_value, heat_value, status_value])
                    # 205호
                elif source_key_prefix == "6cab":
                    memory_map[43] = 1
                    self.sensor_data[43][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "fdec":
                    memory_map[44] = 1
                    self.sensor_data[44][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "ff97":
                    memory_map[45] = 1
                    self.sensor_data[45][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9226":
                    memory_map[46] = 1
                    self.sensor_data[46][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3fc6":  # g
                    memory_map[47] = 1
                    self.sensor_data[47][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "bf99":  # g
                    memory_map[48] = 1
                    self.sensor_data[48][0].append([timestamp_value, heat_value, status_value])
                    # 206호
                elif source_key_prefix == "cd80":
                    memory_map[49] = 1
                    self.sensor_data[49][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "310f":
                    memory_map[50] = 1
                    self.sensor_data[50][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2c2c":
                    memory_map[51] = 1
                    self.sensor_data[51][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f6aa":
                    memory_map[52] = 1
                    self.sensor_data[52][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2546":  # g
                    memory_map[53] = 1
                    self.sensor_data[53][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "b9a4":  # g
                    memory_map[54] = 1
                    self.sensor_data[54][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "4e35":  # g
                    memory_map[55] = 1
                    self.sensor_data[55][0].append([timestamp_value, heat_value, status_value])
                    # 207호
                elif source_key_prefix == "c7f6":
                    memory_map[56] = 1
                    self.sensor_data[56][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "ff69":
                    memory_map[57] = 1
                    self.sensor_data[57][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "1cdd":
                    memory_map[58] = 1
                    self.sensor_data[58][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "1a3f":
                    memory_map[59] = 1
                    self.sensor_data[59][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "cb02":  # 계단
                    memory_map[60] = 1
                    self.sensor_data[60][0].append([timestamp_value, heat_value, status_value])
                    # 2층 공용부
                elif source_key_prefix == "33f9":
                    memory_map[61] = 1
                    self.sensor_data[61][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "75d4":
                    memory_map[62] = 1
                    self.sensor_data[62][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "24a3":
                    memory_map[63] = 1
                    self.sensor_data[63][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3522":
                    memory_map[64] = 1
                    self.sensor_data[64][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6029":
                    memory_map[65] = 1
                    self.sensor_data[65][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "27a3":
                    memory_map[66] = 1
                    self.sensor_data[66][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6bcf":
                    memory_map[67] = 1
                    self.sensor_data[67][0].append([timestamp_value, heat_value, status_value])
                    # 301
                elif source_key_prefix == "d8a9":
                    memory_map[68] = 1
                    self.sensor_data[68][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "1d39":
                    memory_map[69] = 1
                    self.sensor_data[69][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "bd12":
                    memory_map[70] = 1
                    self.sensor_data[70][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "550f":
                    memory_map[71] = 1
                    self.sensor_data[71][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e842":
                    memory_map[72] = 1
                    self.sensor_data[72][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "efc3":
                    memory_map[73] = 1
                    self.sensor_data[73][0].append([timestamp_value, heat_value, status_value])
                    # 302
                elif source_key_prefix == "bd90":
                    memory_map[74] = 1
                    self.sensor_data[74][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "41a0":
                    memory_map[75] = 1
                    self.sensor_data[75][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a2d2":
                    memory_map[76] = 1
                    self.sensor_data[76][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "cd6d":
                    memory_map[77] = 1
                    self.sensor_data[77][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9351":
                    memory_map[78] = 1
                    self.sensor_data[78][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "621c":
                    memory_map[79] = 1
                    self.sensor_data[79][0].append([timestamp_value, heat_value, status_value])
                    # 303
                elif source_key_prefix == "c5cc":
                    memory_map[80] = 1
                    self.sensor_data[80][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "cf50":
                    memory_map[81] = 1
                    self.sensor_data[81][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2d09":
                    memory_map[82] = 1
                    self.sensor_data[82][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "fbac":
                    memory_map[83] = 1
                    self.sensor_data[83][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d6be":
                    memory_map[84] = 1
                    self.sensor_data[84][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7e8a":
                    memory_map[85] = 1
                    self.sensor_data[85][0].append([timestamp_value, heat_value, status_value])
                    # 304
                elif source_key_prefix == "b57f":
                    memory_map[86] = 1
                    self.sensor_data[86][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "84b8":
                    memory_map[87] = 1
                    self.sensor_data[87][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e6df":
                    memory_map[88] = 1
                    self.sensor_data[88][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "5ce1":
                    memory_map[89] = 1
                    self.sensor_data[89][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "5aaf":
                    memory_map[90] = 1
                    self.sensor_data[90][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "1abf":
                    memory_map[91] = 1
                    self.sensor_data[91][0].append([timestamp_value, heat_value, status_value])
                    # 305
                elif source_key_prefix == "21a7":
                    memory_map[92] = 1
                    self.sensor_data[92][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8132":
                    memory_map[93] = 1
                    self.sensor_data[93][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a27e":
                    memory_map[94] = 1
                    self.sensor_data[94][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "15e0":
                    memory_map[95] = 1
                    self.sensor_data[95][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7578":
                    memory_map[96] = 1
                    self.sensor_data[96][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3539":
                    memory_map[97] = 1
                    self.sensor_data[97][0].append([timestamp_value, heat_value, status_value])
                    # 306
                elif source_key_prefix == "34c7":
                    memory_map[98] = 1
                    self.sensor_data[98][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9c96":
                    memory_map[99] = 1
                    self.sensor_data[99][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6438":
                    memory_map[100] = 1
                    self.sensor_data[100][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9d94":
                    memory_map[101] = 1
                    self.sensor_data[101][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "23ea":  # G
                    memory_map[102] = 1
                    self.sensor_data[102][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3c8f":  # G
                    memory_map[103] = 1
                    self.sensor_data[103][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2f14":  # g
                    memory_map[104] = 1
                    self.sensor_data[104][0].append([timestamp_value, heat_value, status_value])
                    # 307
                elif source_key_prefix == "2f8e":  # 3층 앞복도
                    memory_map[105] = 1
                    self.sensor_data[105][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "472e":  # 3층 복도
                    memory_map[106] = 1
                    self.sensor_data[106][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2632":  # 3층 EV홀
                    memory_map[107] = 1
                    self.sensor_data[107][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f0aa":  # 3층 앞복도
                    memory_map[108] = 1
                    self.sensor_data[108][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "da17":  # 3층 계단
                    memory_map[109] = 1
                    self.sensor_data[109][0].append([timestamp_value, heat_value, status_value])
                    # 공용부 부분
                elif source_key_prefix == "496f":
                    memory_map[110] = 1
                    self.sensor_data[110][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "dcea":
                    memory_map[111] = 1
                    self.sensor_data[111][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d220":
                    memory_map[112] = 1
                    self.sensor_data[112][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9c00":
                    memory_map[113] = 1
                    self.sensor_data[113][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "89d4":
                    memory_map[114] = 1
                    self.sensor_data[114][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "17c2":
                    memory_map[115] = 1
                    self.sensor_data[115][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "133f":
                    memory_map[116] = 1
                    self.sensor_data[116][0].append([timestamp_value, heat_value, status_value])
                    # 401
                elif source_key_prefix == "7d73":
                    memory_map[117] = 1
                    self.sensor_data[117][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "0e0e":
                    memory_map[118] = 1
                    self.sensor_data[118][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2905":
                    memory_map[119] = 1
                    self.sensor_data[119][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "1791":
                    memory_map[120] = 1
                    self.sensor_data[120][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f6fd":
                    memory_map[121] = 1
                    self.sensor_data[121][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "35e3":
                    memory_map[122] = 1
                    self.sensor_data[122][0].append([timestamp_value, heat_value, status_value])
                    # 402
                elif source_key_prefix == "f606":
                    memory_map[123] = 1
                    self.sensor_data[123][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "bc24":
                    memory_map[124] = 1
                    self.sensor_data[124][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7e6d":
                    memory_map[125] = 1
                    self.sensor_data[125][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3d9f":
                    memory_map[126] = 1
                    self.sensor_data[126][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "5aa4":  # G
                    memory_map[127] = 1
                    self.sensor_data[127][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "bf79":  # G
                    memory_map[128] = 1
                    self.sensor_data[128][0].append([timestamp_value, heat_value, status_value])
                    # 403
                elif source_key_prefix == "e572":
                    memory_map[129] = 1
                    self.sensor_data[129][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "0a80":
                    memory_map[130] = 1
                    self.sensor_data[130][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6828":
                    memory_map[131] = 1
                    self.sensor_data[131][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a213":
                    memory_map[132] = 1
                    self.sensor_data[132][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "ca65":
                    memory_map[133] = 1
                    self.sensor_data[133][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d7fb":
                    memory_map[134] = 1
                    self.sensor_data[134][0].append([timestamp_value, heat_value, status_value])
                    # 404
                elif source_key_prefix == "3471":
                    memory_map[135] = 1
                    self.sensor_data[135][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "c1c7":
                    memory_map[136] = 1
                    self.sensor_data[136][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "5869":
                    memory_map[137] = 1
                    self.sensor_data[137][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "77ac":
                    memory_map[138] = 1
                    self.sensor_data[138][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "495a":
                    memory_map[139] = 1
                    self.sensor_data[139][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "c3dd":
                    memory_map[140] = 1
                    self.sensor_data[140][0].append([timestamp_value, heat_value, status_value])
                    # 405
                elif source_key_prefix == "5841":
                    memory_map[141] = 1
                    self.sensor_data[141][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "c3b4":
                    memory_map[142] = 1
                    self.sensor_data[142][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2d90":
                    memory_map[143] = 1
                    self.sensor_data[143][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2edc":
                    memory_map[144] = 1
                    self.sensor_data[144][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e2b2":
                    memory_map[145] = 1
                    self.sensor_data[145][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "fda0":
                    memory_map[146] = 1
                    self.sensor_data[146][0].append([timestamp_value, heat_value, status_value])
                    # 406
                elif source_key_prefix == "67d6":
                    memory_map[147] = 1
                    self.sensor_data[147][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "4d9a":
                    memory_map[148] = 1
                    self.sensor_data[148][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f28a":
                    memory_map[149] = 1
                    self.sensor_data[149][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "51d5":
                    memory_map[150] = 1
                    self.sensor_data[150][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "ae25":
                    memory_map[151] = 1
                    self.sensor_data[151][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d04a":
                    memory_map[152] = 1
                    self.sensor_data[152][0].append([timestamp_value, heat_value, status_value])
                # 153 gas
                # 407
                elif source_key_prefix == "06b1":  # 4층 앞복도
                    memory_map[154] = 1
                    self.sensor_data[154][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "826e":  # 4층 복도
                    memory_map[155] = 1
                    self.sensor_data[155][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f11d":  # 4층 EV홀
                    memory_map[156] = 1
                    self.sensor_data[156][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "ba9f":  # 4층 앞복도
                    memory_map[157] = 1
                    self.sensor_data[157][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "b76b":  # 4층 계단
                    memory_map[158] = 1
                    self.sensor_data[158][0].append([timestamp_value, heat_value, status_value])
                # 공용부 부분
                elif source_key_prefix == "ca19":
                    memory_map[159] = 1
                    self.sensor_data[159][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6eee":
                    memory_map[160] = 1
                    self.sensor_data[160][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "af9e":
                    memory_map[161] = 1
                    self.sensor_data[161][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "0a55":
                    memory_map[162] = 1
                    self.sensor_data[162][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "b5f1":
                    memory_map[163] = 1
                    self.sensor_data[163][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "6121":
                    memory_map[164] = 1
                    self.sensor_data[164][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3ba8":
                    memory_map[165] = 1
                    self.sensor_data[165][0].append([timestamp_value, heat_value, status_value])
                    # 501
                elif source_key_prefix == "f536":
                    memory_map[166] = 1
                    self.sensor_data[166][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "92a8":
                    memory_map[167] = 1
                    self.sensor_data[167][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "efc7":
                    memory_map[168] = 1
                    self.sensor_data[168][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "eb6a":
                    memory_map[169] = 1
                    self.sensor_data[169][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "4044":
                    memory_map[170] = 1
                    self.sensor_data[170][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e23e":
                    memory_map[171] = 1
                    self.sensor_data[171][0].append([timestamp_value, heat_value, status_value])
                    # 502
                elif source_key_prefix == "b383":
                    memory_map[172] = 1
                    self.sensor_data[172][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f995":
                    memory_map[173] = 1
                    self.sensor_data[173][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d7e8":
                    memory_map[174] = 1
                    self.sensor_data[174][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8932":
                    memory_map[175] = 1
                    self.sensor_data[175][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "8805":
                    memory_map[176] = 1
                    self.sensor_data[176][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "e9c0":
                    memory_map[177] = 1
                    self.sensor_data[177][0].append([timestamp_value, heat_value, status_value])
                    # 503
                elif source_key_prefix == "a450":
                    memory_map[178] = 1
                    self.sensor_data[178][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9958":
                    memory_map[179] = 1
                    self.sensor_data[179][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f206":
                    memory_map[180] = 1
                    self.sensor_data[180][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a721":
                    memory_map[181] = 1
                    self.sensor_data[181][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "b0b2":
                    memory_map[182] = 1
                    self.sensor_data[182][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "abba":
                    memory_map[183] = 1
                    self.sensor_data[183][0].append([timestamp_value, heat_value, status_value])
                    # 504
                elif source_key_prefix == "8e05":
                    memory_map[184] = 1
                    self.sensor_data[184][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "793d":
                    memory_map[185] = 1
                    self.sensor_data[185][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "977f":
                    memory_map[186] = 1
                    self.sensor_data[186][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "a3cf":
                    memory_map[187] = 1
                    self.sensor_data[187][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "d815":
                    memory_map[188] = 1
                    self.sensor_data[188][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f855":
                    memory_map[189] = 1
                    self.sensor_data[189][0].append([timestamp_value, heat_value, status_value])
                    # 505
                elif source_key_prefix == "7611":
                    memory_map[190] = 1
                    self.sensor_data[190][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "96ec":
                    memory_map[191] = 1
                    self.sensor_data[191][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "4abc":
                    memory_map[192] = 1
                    self.sensor_data[192][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7882":
                    memory_map[193] = 1
                    self.sensor_data[193][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "612f":
                    memory_map[194] = 1
                    self.sensor_data[194][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "7773":
                    memory_map[195] = 1
                    self.sensor_data[195][0].append([timestamp_value, heat_value, status_value])
                    # 506
                elif source_key_prefix == "6a0c":
                    memory_map[196] = 1
                    self.sensor_data[196][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "11c5":
                    memory_map[197] = 1
                    self.sensor_data[197][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9469":
                    memory_map[198] = 1
                    self.sensor_data[198][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3437":
                    memory_map[199] = 1
                    self.sensor_data[199][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "fc31":  # g
                    memory_map[200] = 1
                    self.sensor_data[200][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "4051":  # g
                    memory_map[201] = 1
                    self.sensor_data[201][0].append([timestamp_value, heat_value, status_value])
                    # 202 = gas
                    # 507 , 연기 추가
                    # 203~ 209
                elif source_key_prefix == "1d69":  # 5층 앞복도
                    memory_map[203] = 1
                    self.sensor_data[203][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "3181":  # 5 층 복도
                    memory_map[204] = 1
                    self.sensor_data[204][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "9dc3":  # EV홀
                    memory_map[205] = 1
                    self.sensor_data[205][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f118":  # 5층 앞복도
                    memory_map[206] = 1
                    self.sensor_data[206][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "2db9":  # 5층 계단
                    memory_map[207] = 1
                    self.sensor_data[207][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "5d26":  # EV
                    memory_map[208] = 1
                    self.sensor_data[208][0].append([timestamp_value, heat_value, status_value])
                elif source_key_prefix == "f4a0":  # 6층 계단
                    memory_map[209] = 1
                    self.sensor_data[209][0].append([timestamp_value, heat_value, status_value])
                # endreiogn



class read_Class(threading.Thread):
    def __init__(self,shared_data):
        super().__init__()

        self.shared_data = shared_data

        self.head = [[], [], [], [], [], []]
        self.floor_idx_list = [[], [], [], [], [], []]
        self.temp_index = [[], [], [], [], [], []]
        self.gas_index = [[], [], [], [], [], []]

        self.IP = '183.99.41.239'
        self.Port = '23306'
        self.ID = 'root'
        self.PW = 'hbrain0372!'
        self.DB_Name = 'firetest'

        self.working = False
        self.connection = False
        self.db_connection = None
        self.speed = 1
        self.sensor_stack = 30
        self.floor_num = 4

        self.analy_class = get_data.get_data()

        self.cur = None

        self.var_init()

        self.run()

    def var_init(self):
        idx_1F = [i for i in range(1, 12)]
        idx_2F = [i for i in range(12, 61)]
        idx_3F = [i for i in range(61, 110)]
        idx_4F = [i for i in range(110, 159)]
        idx_5F = [i for i in range(159, 208)]
        idx_6F = [i for i in range(208, 210)]
        self.floor_idx_list = [idx_1F, idx_2F, idx_3F, idx_4F, idx_5F, idx_6F]

    def init_DB(self):

        try:
            sql = 'CREATE DATABASE IF NOT EXISTS ' + self.DB_Name
            self.cur.execute(sql)
        except Exception as e:
            print(e)

        time.sleep(1)

        create_danger_level = "CREATE TABLE IF NOT EXISTS danger_level(time SMALLINT(3) NOT NULL, `101` FLOAT default 0.0, `102` FLOAT default 0.0, `1-1` FLOAT default 0.0, `1-2` FLOAT default 0.0, `1-3` FLOAT default 0.0, " \
                              "`201` FLOAT default 0.0, `202` FLOAT default 0.0, `203` FLOAT default 0.0, `204` FLOAT default 0.0, `205` FLOAT default 0.0, `206` FLOAT default 0.0, `207` FLOAT default 0.0, " \
                              "`2-1` FLOAT default 0.0, `2-2` FLOAT default 0.0, `2-3` FLOAT default 0.0, `2-4` FLOAT default 0.0, " \
                              "`301` FLOAT default 0.0, `302` FLOAT default 0.0, `303` FLOAT default 0.0, `304` FLOAT default 0.0, `305` FLOAT default 0.0, `306` FLOAT default 0.0, `307` FLOAT default 0.0, " \
                              "`3-1` FLOAT default 0.0, `3-2` FLOAT default 0.0, `3-3` FLOAT default 0.0, `3-4` FLOAT default 0.0, " \
                              "`401` FLOAT default 0.0, `402` FLOAT default 0.0, `403` FLOAT default 0.0, `404` FLOAT default 0.0, `405` FLOAT default 0.0, `406` FLOAT default 0.0, `407` FLOAT default 0.0, " \
                              "`4-1` FLOAT default 0.0, `4-2` FLOAT default 0.0, `4-3` FLOAT default 0.0, `4-4` FLOAT default 0.0, " \
                              "`501` FLOAT default 0.0, `502` FLOAT default 0.0, `503` FLOAT default 0.0, `504` FLOAT default 0.0, `505` FLOAT default 0.0, `506` FLOAT default 0.0, `507` FLOAT default 0.0, " \
                              "`5-1` FLOAT default 0.0, `5-2` FLOAT default 0.0, `5-3` FLOAT default 0.0, `5-4` FLOAT default 0.0, " \
                              "PRIMARY KEY(time));"
        create_exit_route = "CREATE TABLE IF NOT EXISTS exit_route(time SMALLINT(3) NOT NULL, `101` TINYINT default 0, `102` TINYINT default 0, " \
                            "`201` TINYINT default 0, `202` TINYINT default 0, `203` TINYINT default 0, `204` TINYINT default 0, `205` TINYINT default 0, `206` TINYINT default 0, `207` TINYINT default 0, " \
                            "`301` TINYINT default 0, `302` TINYINT default 0, `303` TINYINT default 0, `304` TINYINT default 0, `305` TINYINT default 0, `306` TINYINT default 0, `307` TINYINT default 0, " \
                            "`401` TINYINT default 0, `402` TINYINT default 0, `403` TINYINT default 0, `404` TINYINT default 0, `405` TINYINT default 0, `406` TINYINT default 0, `407` TINYINT default 0, " \
                            "`501` TINYINT default 0, `502` TINYINT default 0, `503` TINYINT default 0, `504` TINYINT default 0, `505` TINYINT default 0, `506` TINYINT default 0, `507` TINYINT default 0, " \
                            "PRIMARY KEY(time));"

        try:
            self.cur.execute(create_danger_level)
        except Exception as e:
            print("err create_danger_level: ", e)
        try:
            self.cur.execute(create_exit_route)
        except Exception as e:
            print("err create_exit_route: ", e)

        truccate_danger_level_sql = 'TRUNCATE TABLE danger_level;'
        truccate_exit_rout_sql = 'TRUNCATE TABLE exit_route;'
        try:
            self.cur.execute(truccate_danger_level_sql)
        except Exception as e:
            print("err truccate_danger_level_sql: ", e)
        try:
            self.cur.execute(truccate_exit_rout_sql)
        except Exception as e:
            print("err truccate_exit_rout_sql: ", e)

        danger_level_value = ""
        for _ in range(49):
            danger_level_value += "0.0, "
        danger_level_value = danger_level_value[:-2]

        insert_danger_level_1 = "INSERT INTO danger_level VALUES"
        insert_danger_level_2 = ""
        for i in range(61):
            insert_danger_level_2 += "(" + str(i) + ", " + danger_level_value + "), "

        exit_route_value = ""
        for _ in range(30):
            exit_route_value += "0, "
        exit_route_value = exit_route_value[:-2]

        insert_exit_route_1 = "INSERT INTO exit_route VALUES"
        insert_exit_route_2 = ""
        for i in range(61):
            insert_exit_route_2 += "(" + str(i) + ", " + exit_route_value + "), "

        try:
            self.cur.execute(insert_danger_level_1 + insert_danger_level_2[:-2] + ";")
        except Exception as e:
            print("err insert_danger_level: ", e)
        try:
            self.cur.execute(insert_exit_route_1 + insert_exit_route_2[:-2] + ";")
        except Exception as e:
            print("err insert_exit_route: ", e)

    def connect_DB(self):
        if not self.connection:
            try:
                self.db_connection = pymysql.connect(host=self.IP,
                                                     port=int(self.Port),
                                                     user=self.ID,
                                                     password=self.PW,
                                                     db=self.DB_Name,
                                                     charset='utf8',
                                                     autocommit=True,
                                                     read_timeout=5,
                                                     write_timeout=5,
                                                     connect_timeout=5)
                self.cur = self.db_connection.cursor()
                self.init_DB()
                self.connection = True
                self.working = True
            except Exception as err:
                print(err)
        else:
            try:
                self.db_connection.close()
                self.connection = False
                self.working = False
            except Exception as err:
                print(err)

    def disconnect_DB(self):
        if self.connection:
            try:
                self.db_connection.close()
                self.connection = False

            except Exception as err:
                print(err)

    def list_split_per_floor(self, data, all_index):
        output = []
        for _ in range(len(all_index)):
            output.append([])
        for floor, idx_list in enumerate(all_index):
            for idx in idx_list:
                output[floor].append(data[idx])

        return output

    def run(self):
        self.connect_DB()
        if self.connection:
            # #region DB_version
            # sql = "SELECT * FROM sensor_data;"
            #
            # self.cur.execute(sql)
            # result = self.cur.fetchall()
            # self.disconnect_DB()
            #
            # head = list(result[0]).copy()
            # for idx, type in enumerate(head):
            #     this_floor = 0
            #     for floor, floor_nodes in enumerate(self.floor_idx_list):
            #         if idx in floor_nodes:
            #             this_floor = floor
            #     if type == 'temp':
            #         self.temp_index[this_floor].append(idx)
            #     if type == 'gas':
            #         self.gas_index[this_floor].append(idx)
            # self.head = [head[1:12], head[12:61], head[61:110], head[110:159], head[159:208], head[208:210]]
            #
            # total_datas = []
            #
            # for _ in range(self.sensor_stack):
            #     frame_datas = [result[1][1:12], result[1][12:61], result[1][61:110], result[1][110:159],
            #                    result[1][159:208], result[1][208:210]]
            #     total_datas.append([time.time()] + frame_datas)
            #
            # for frame, data in enumerate(result[1:]):
            #     data = list(data)
            #     if self.working:
            #         frame_datas = [data[1:12], data[12:61], data[61:110], data[110:159], data[159:208], data[208:210]]
            #         total_datas.pop(0)
            #         total_datas.append([time.time()] + frame_datas)
            #         self.analy_class.data_analy(self.head, self.temp_index, self.gas_index, total_datas)
            #         time.sleep(self.speed / 1)
            #     else:
            #         break
            # # endregion

            # #reguin kafka_version
            data_str = "0000 temp temp gas temp temp temp gas temp gas temp gas gas gas temp temp gas temp temp temp gas temp temp temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp temp temp gas gas temp temp gas temp temp gas gas temp temp temp temp gas gas temp gas temp temp temp gas gas temp temp gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp temp temp gas gas temp temp gas temp temp gas gas temp temp temp temp gas gas temp gas temp temp temp gas gas temp temp gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp temp temp gas gas temp temp gas temp temp gas gas temp temp temp temp gas gas temp temp temp temp temp gas gas temp temp gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp gas gas temp temp temp temp gas gas temp temp gas temp temp gas gas temp temp temp temp gas gas temp temp temp gas gas"
            head = data_str.split()
            for idx, type in enumerate(head):
                this_floor = 0
                for floor, floor_nodes in enumerate(self.floor_idx_list):
                    if idx in floor_nodes:
                        this_floor = floor
                        break
                if type == 'temp':
                    self.temp_index[this_floor].append(idx)
                if type == 'gas':
                    self.gas_index[this_floor].append(idx)
            self.head = [head[1:12], head[12:61], head[61:110], head[110:159], head[159:208], head[208:210]]

            total_datas = []
            while True:
                time.sleep(2)
                result = self.shared_data.get_a()
                frame_datas = [result[1:12], result[12:61], result[61:110], result[110:159],result[159:208], result[208:210]]
                total_datas.append([time.time()] + frame_datas)

                if len(total_datas) > 3:
                    if self.working:
                        self.analy_class.data_analy(self.head, self.temp_index, self.gas_index, total_datas)
            #endregion
            self.working = False
            time.sleep(1)




def start_read_thread(shared_data):
    readClass = read_Class(shared_data)
    readClass.start()

def start_kafka_thread(shared_data):
    kafka_worker = kafka_thread(shared_data)
    kafka_worker.start()


if __name__ == '__main__':
    shared_data = SharedData()
    read_thread_instance = threading.Thread(target=start_read_thread, args=(shared_data,))
    kafka_thread_instance = threading.Thread(target=start_kafka_thread, args=(shared_data,))

    read_thread_instance.start()
    kafka_thread_instance.start()

    read_thread_instance.join()
    kafka_thread_instance.join()


    #if use kafka

