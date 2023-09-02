from get_data import get_data

from shared_data import SharedData

from PyQt5.QtCore import QTimer, QTime
from PyQt5.QtWidgets import QLineEdit, QHeaderView
from PyQt5.QtGui import QIntValidator



class func_set:
    def __init__(self, ui):
        super().__init__()
        self.ui = ui
        self.ui_init()
        self.func_init()
        self.event_init()

    def ui_init(self):
        self.ui.PORT_Edit.setValidator(QIntValidator())
        self.ui.PW_Edit.setEchoMode(QLineEdit.Password)

        header = self.ui.system_log_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        # header = self.ui.react_log_table.horizontalHeader()
        # header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        # header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header = self.ui.analysis_log_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header = self.ui.temp_data_log_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header = self.ui.gas_data_log_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)


    def func_init(self):
        self.set_timer_event()
        self.gd = get_data(self.ui)



    def event_init(self):
        self.ui.Start_Service_btn.clicked.connect(self.gd.worker_start)
        self.ui.reset_btn.clicked.connect(self.gd.set_default_param)
        self.ui.WatchMode0.clicked.connect(self.gd.change_Watch_Mode)
        self.ui.WatchMode1.clicked.connect(self.gd.change_Watch_Mode)
        self.ui.WatchMode2.clicked.connect(self.gd.change_Watch_Mode)
        self.ui.WatchMode_Highlight.clicked.connect(self.gd.change_Watch_Mode)
        self.ui.WatchFloor_comboBox.currentIndexChanged.connect(self.gd.change_Watch_Floor)
        self.ui.StartFloor_comboBox.currentIndexChanged.connect(self.gd.change_Start_Floor)
        self.ui.StartRoom_comboBox.currentIndexChanged.connect(self.gd.change_Start_Room)
        self.ui.watch_present.clicked.connect(self.gd.change_N_Mode)
        self.ui.N_min_later_combobox.currentIndexChanged.connect(self.gd.change_N_min)

    def resizeWidget(self):
        self.gd.resizeWidget()

    def set_timer_event(self):
        self.clock = QTimer()
        self.clock.setInterval(100)
        self.clock.timeout.connect(self.clock_func)
        self.clock.start()

    def clock_func(self):
        currentTime = QTime.currentTime().toString("hh:mm:ss")
        self.ui.time_label.setText(currentTime)
