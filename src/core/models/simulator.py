# -*- coding: utf-8 -*-
import random

class MachineStatusSimulator:
    def __init__(self):
        # TODO 定義轉移矩陣 (Transition Matrix)
        self.transition_rules = {
            'MAINTENANCE': (['MAINTENANCE', 'IDLE'], [0.99, 0.01]),
            'IDLE':        (['IDLE', 'MAINTENANCE'], [0.99, 0.01]),
            'RUNNING':     (['RUNNING', 'ALARM'], [0.999, 0.001]),
            'ALARM':       (['ALARM', 'RUNNING'], [0.995, 0.005])
        }


    def get_next_status(self, current_status) -> str:
        """TODO 根據權重取得下一個狀態"""
        if current_status not in self.transition_rules:
            raise Exception(f'{current_status} not in self.transition_rules, please check it.')

        possible_states, weights = self.transition_rules[current_status]
        return random.choices(possible_states, weights=weights)[0]


    def get_load_profile(self, hour: int) -> str:
        """TODO 定義各時間區間的負載類型"""
        if 0 <= hour < 8:
            return 'OFF_PEAK'
        elif 8 <= hour < 12:
            return 'PEAK'
        elif 12 <= hour < 13:
            return 'OFF_PEAK'
        elif 13 <= hour < 15:
            return 'NORMAL'
        elif 15 <= hour < 24:
            return 'PEAK'
        else:
            return 'OFF_PEAK'