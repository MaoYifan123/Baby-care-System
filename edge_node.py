#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婴幼儿智能监护系统 - 边缘计算节点
Edge Computing Node for Infant Care System

模拟部署在家庭环境中的边缘计算设备（如树莓派），
负责接收传感器数据流、执行实时告警和本地流处理。

功能：流处理 + 实时告警 + 本地数据缓存
"""

import json
import time
import random
import threading
import statistics
from datetime import datetime
from collections import deque, defaultdict

# ============================================================
# 边缘计算节点 - 实时流处理引擎
# ============================================================

class EdgeStreamProcessor:
    """边缘流处理引擎：基于滑动窗口的实时计算"""

    def __init__(self, window_size=10):
        self.window_size = window_size
        self.windows = defaultdict(lambda: deque(maxlen=window_size))
        self.alert_thresholds = {
            'temperature': {'critical_high': 37.5, 'warning_high': 37.2, 'warning_low': 36.0, 'critical_low': 35.5},
            'heart_rate': {'critical_high': 180, 'warning_high': 160, 'warning_low': 80, 'critical_low': 60},
            'crying': {'threshold': 0.7}
        }
        self.alert_callback = None

    def set_alert_callback(self, callback):
        self.alert_callback = callback

    def process_data(self, sensor_type, value, timestamp=None):
        """处理单个传感器数据，返回分析结果"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()

        result = {
            'timestamp': timestamp,
            'sensor_type': sensor_type,
            'value': value,
            'alerts': []
        }

        # 1. 滑动窗口更新
        self.windows[sensor_type].append({'value': value, 'ts': timestamp})

        # 2. 实时分析
        if sensor_type == 'temperature':
            result['analysis'] = self._analyze_temperature(sensor_type, value)
            result['alerts'] = self._check_temperature_alerts(value)
        elif sensor_type == 'heart_rate':
            result['analysis'] = self._analyze_heart_rate(sensor_type, value)
            result['alerts'] = self._check_heart_rate_alerts(value)
        elif sensor_type == 'crying':
            result['analysis'] = self._analyze_crying(sensor_type, value)
            result['alerts'] = self._check_crying_alerts(value)
        elif sensor_type == 'body_position':
            result['analysis'] = self._analyze_position(sensor_type, value)
            result['alerts'] = self._check_position_alerts(value)

        # 3. 边缘告警（本地立即触发）
        if result['alerts'] and self.alert_callback:
            for alert in result['alerts']:
                self.alert_callback(alert)

        return result

    def _analyze_temperature(self, sensor_type, value):
        """温度分析：使用滑动窗口计算均值和趋势"""
        window = list(self.windows[sensor_type])
        if len(window) < 3:
            return {'status': 'normal', 'trend': 'stable', 'confidence': 0.5}

        values = [w['value'] for w in window]
        mean = statistics.mean(values)
        trend = 'rising' if values[-1] - values[0] > 0.3 else 'falling' if values[0] - values[-1] > 0.3 else 'stable'

        if value > 37.2 or value < 36.2:
            status = 'warning'
        elif value > 37.5 or value < 35.8:
            status = 'critical'
        else:
            status = 'normal'

        return {
            'status': status,
            'trend': trend,
            'mean': round(mean, 2),
            'confidence': min(1.0, len(window) / self.window_size)
        }

    def _analyze_heart_rate(self, sensor_type, value):
        """心率分析：结合婴儿月龄的参考范围"""
        window = list(self.windows[sensor_type])
        if len(window) < 5:
            return {'status': 'normal', 'trend': 'stable', 'confidence': 0.5}

        values = [w['value'] for w in window]
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 0
        trend = 'rising' if values[-1] - values[0] > 15 else 'falling' if values[0] - values[-1] > 15 else 'stable'

        if 80 <= value <= 160:
            status = 'normal'
        elif 70 <= value < 80 or 160 < value <= 180:
            status = 'warning'
        else:
            status = 'critical'

        return {
            'status': status,
            'trend': trend,
            'mean': round(mean, 1),
            'std': round(std, 1),
            'confidence': min(1.0, len(window) / self.window_size)
        }

    def _analyze_crying(self, sensor_type, value):
        """哭声分析：基于概率和持续时间"""
        window = list(self.windows[sensor_type])
        crying_count = sum(1 for w in window if w['value'] > 0.5)
        duration = len(window)

        if value > 0.7:
            status = 'critical'
        elif value > 0.4:
            status = 'warning'
        else:
            status = 'normal'

        return {
            'status': status,
            'crying_ratio': round(crying_count / duration, 2) if duration > 0 else 0,
            'duration': duration,
            'confidence': 0.8
        }

    def _analyze_position(self, sensor_type, value):
        """体位分析：检测翻身和窒息风险"""
        pos_labels = {0: '仰卧', 1: '俯卧', 2: '侧卧', 3: '站立'}
        status = 'normal'
        if value == 1:
            status = 'warning'  # 俯卧有窒息风险
        return {
            'status': status,
            'position': pos_labels.get(value, '未知'),
            'confidence': 0.95
        }

    def _check_temperature_alerts(self, value):
        alerts = []
        t = self.alert_thresholds['temperature']
        if value >= t['critical_high']:
            alerts.append({'level': 'critical', 'message': f'体温过高！{value}°C', 'action': '立即就医'})
        elif value >= t['warning_high']:
            alerts.append({'level': 'warning', 'message': f'体温偏高 {value}°C', 'action': '物理降温'})
        elif value <= t['critical_low']:
            alerts.append({'level': 'critical', 'message': f'体温过低！{value}°C', 'action': '保暖并就医'})
        elif value <= t['warning_low']:
            alerts.append({'level': 'warning', 'message': f'体温偏低 {value}°C', 'action': '保暖'})
        return alerts

    def _check_heart_rate_alerts(self, value):
        alerts = []
        h = self.alert_thresholds['heart_rate']
        if value >= h['critical_high']:
            alerts.append({'level': 'critical', 'message': f'心率过快！{value}bpm', 'action': '立即就医'})
        elif value >= h['warning_high']:
            alerts.append({'level': 'warning', 'message': f'心率偏快 {value}bpm', 'action': '观察'})
        elif value <= h['critical_low']:
            alerts.append({'level': 'critical', 'message': f'心率过慢！{value}bpm', 'action': '立即就医'})
        elif value <= h['warning_low']:
            alerts.append({'level': 'warning', 'message': f'心率偏慢 {value}bpm', 'action': '观察'})
        return alerts

    def _check_crying_alerts(self, value):
        alerts = []
        window = list(self.windows['crying'])
        crying_duration = len([w for w in window if w['value'] > 0.5])
        if crying_duration >= 8:
            alerts.append({'level': 'critical', 'message': f'持续哭闹超过{crying_duration}次采样', 'action': '检查需求'})
        elif crying_duration >= 5:
            alerts.append({'level': 'warning', 'message': f'哭声检测频繁', 'action': '安抚'})
        return alerts

    def _check_position_alerts(self, value):
        alerts = []
        if value == 1:
            alerts.append({'level': 'warning', 'message': '婴儿处于俯卧位', 'action': '建议翻正'})
        return alerts

    def get_summary(self):
        """获取边缘计算节点当前状态摘要"""
        summary = {}
        for sensor, window in self.windows.items():
            if len(window) > 0:
                values = [w['value'] for w in window]
                summary[sensor] = {
                    'latest': values[-1],
                    'mean': round(statistics.mean(values), 2),
                    'min': round(min(values), 2),
                    'max': round(max(values), 2),
                    'count': len(values)
                }
        return summary


# ============================================================
# 边缘节点模拟器 - 模拟传感器数据采集
# ============================================================

class EdgeNodeSimulator:
    """边缘节点数据模拟器：生成模拟传感器数据"""

    def __init__(self, infant_age_months=6):
        self.infant_age_months = infant_age_months
        self.stream_processor = EdgeStreamProcessor(window_size=15)
        self.running = False
        self.data_callback = None
        self.alert_history = deque(maxlen=100)

        # 正常范围参考（基于婴儿月龄）
        self.normal_ranges = {
            'temperature': (36.5, 37.0),
            'heart_rate': (100, 140),
        }

    def set_data_callback(self, callback):
        self.data_callback = callback

    def start(self, interval=2.0):
        """启动数据采集"""
        self.running = True
        self.stream_processor.set_alert_callback(self._on_edge_alert)
        thread = threading.Thread(target=self._collect_loop, args=(interval,), daemon=True)
        thread.start()
        return thread

    def stop(self):
        self.running = False

    def _collect_loop(self, interval):
        """数据采集循环"""
        while self.running:
            sensor_readings = self._generate_sensor_data()
            for data in sensor_readings:
                result = self.stream_processor.process_data(**data)
                if self.data_callback:
                    self.data_callback(result)
            time.sleep(interval)

    def _generate_sensor_data(self):
        """生成模拟传感器数据（偶尔引入异常）"""
        rand = random.random()

        # 95% 正常数据，5% 异常数据
        if rand > 0.95:
            anomaly = random.choice(['temp_high', 'temp_low', 'hr_high', 'hr_low', 'crying', 'prone'])
        else:
            anomaly = None

        if anomaly == 'temp_high':
            temp = round(random.uniform(37.3, 38.5), 2)
        elif anomaly == 'temp_low':
            temp = round(random.uniform(35.0, 35.8), 2)
        else:
            temp = round(random.uniform(*self.normal_ranges['temperature']), 2)

        if anomaly == 'hr_high':
            hr = random.randint(165, 200)
        elif anomaly == 'hr_low':
            hr = random.randint(50, 65)
        else:
            hr = random.randint(*self.normal_ranges['heart_rate'])

        if anomaly == 'crying':
            crying = round(random.uniform(0.7, 1.0), 2)
        else:
            crying = round(random.uniform(0.0, 0.2), 2)

        if anomaly == 'prone':
            position = 1
        else:
            position = random.choice([0, 2, 3])  # 仰卧、侧卧为主

        return {
            'sensor_type': 'temperature',
            'value': temp,
            'timestamp': datetime.now().isoformat()
        }, {
            'sensor_type': 'heart_rate',
            'value': hr,
            'timestamp': datetime.now().isoformat()
        }, {
            'sensor_type': 'crying',
            'value': crying,
            'timestamp': datetime.now().isoformat()
        }, {
            'sensor_type': 'body_position',
            'value': position,
            'timestamp': datetime.now().isoformat()
        }

    def _on_edge_alert(self, alert):
        """边缘层告警回调"""
        alert['source'] = 'edge'
        alert['time'] = datetime.now().isoformat()
        self.alert_history.append(alert)

    def generate_batch(self, seconds=60, interval=2.0):
        """生成一批测试数据"""
        results = []
        steps = int(seconds / interval)
        for _ in range(steps):
            batch = self._generate_sensor_data()
            for data in batch:
                result = self.stream_processor.process_data(**data)
                results.append(result)
            time.sleep(0.01)
        return results


# ============================================================
# 主入口
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("婴幼儿智能监护系统 - 边缘计算节点")
    print("=" * 60)

    node = EdgeNodeSimulator()

    def on_data(data):
        alerts_str = f" [ALERT: {data['alerts']}]" if data['alerts'] else ""
        print(f"[Edge] {data['sensor_type']:15s} = {data['value']:8.2f} | {data['analysis']}{alerts_str}")

    node.set_data_callback(on_data)
    node.start(interval=2.0)

    try:
        print("\n边缘节点已启动，按 Ctrl+C 停止...\n")
        while True:
            time.sleep(10)
            summary = node.stream_processor.get_summary()
            print(f"\n--- 边缘节点摘要 ({datetime.now().strftime('%H:%M:%S')}) ---")
            for k, v in summary.items():
                print(f"  {k}: latest={v['latest']}, mean={v['mean']}, min={v['min']}, max={v['max']}")
    except KeyboardInterrupt:
        node.stop()
        print("\n边缘节点已停止")
