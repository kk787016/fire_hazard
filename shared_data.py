import threading

class SharedData:
    def __init__(self):
        self.a = [0] * 210  # 초기값이 0인 길이가 210인 리스트
        self.lock = threading.Lock()

    def get_a(self):
        with self.lock:
            return self.a

    def set_a(self, new_values):
        with self.lock:
            self.a = new_values
