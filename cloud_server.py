#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婴幼儿智能监护系统 - Flask 后端服务
Cloud Backend for Infant Care System

负责云边端协调、任务分发、数据存储、趋势分析和智能建议。
支持流处理（实时数据）和批处理（历史分析）两种模式。

云边端计算划分策略：
- 边缘层(Edge): 实时流处理、紧急告警、本地缓存
- 雾层(Fog/本地服务器): 数据汇聚、短期分析


- 云层(Cloud): 长期存储、趋势分析、大模型建议
"""

import json
import time
import threading
import statistics
import random
from datetime import datetime, timedelta
from collections import deque, defaultdict
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ============================================================
# 全局数据存储（生产环境应使用数据库）
# ============================================================

DATA_STORE = {
    'realtime': deque(maxlen=500),      # 实时数据流
    'alerts': deque(maxlen=200),        # 告警记录
    'history': defaultdict(list),       # 历史数据（按传感器类型）
    'sessions': {},                     # 监护会话
    'stats': {                          # 统计信息
        'total_records': 0,
        'total_alerts': 0,
        'uptime_start': datetime.now().isoformat()
    }
}

EDGE_NODE_STATUS = {
    'connected': True,
    'last_heartbeat': datetime.now().isoformat(),
    'data_rate': 0
}

COMPUTE_PARTITION = {
    'edge': ['realtime_filter', 'immediate_alert', 'local_cache', 'stream_stats'],
    'fog': ['data_aggregation', 'short_term_trend', 'noise_reduction'],
    'cloud': ['long_term_analysis', 'anomaly_detection', 'llm_suggestion', 'report_generation']
}

# ============================================================
# 云端流处理引擎
# ============================================================

class CloudStreamProcessor:
    """云端流处理：跨节点数据关联分析"""

    def __init__(self):
        self.global_windows = defaultdict(lambda: deque(maxlen=100))
        self.correlation_buffer = deque(maxlen=50)
        self.pattern_buffer = defaultdict(lambda: deque(maxlen=20))

    def process(self, sensor_type, value, timestamp=None, source='edge'):
        """云端处理：多节点数据融合 + 关联分析"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()

        result = {
            'timestamp': timestamp,
            'sensor_type': sensor_type,
            'value': value,
            'source': source,
            'cloud_analysis': {}
        }

        self.global_windows[sensor_type].append({
            'value': value, 'ts': timestamp, 'source': source
        })

        # 云端分析：跨传感器关联
        result['cloud_analysis'] = self._cross_sensor_analysis(sensor_type)
        # 云端分析：长期趋势
        result['cloud_analysis']['long_term_trend'] = self._long_term_trend(sensor_type)
        # 云端分析：异常检测
        result['cloud_analysis']['anomaly_score'] = self._anomaly_score(sensor_type, value)

        return result

    def _cross_sensor_analysis(self, sensor_type):
        """跨传感器关联分析"""
        if sensor_type not in self.global_windows:
            return {}

        window = list(self.global_windows[sensor_type])
        if len(window) < 5:
            return {'correlation': {}, 'insight': '数据不足'}

        analysis = {}

        # 哭声与其他指标关联
        if sensor_type == 'crying' and 'heart_rate' in self.global_windows:
            hr_window = list(self.global_windows['heart_rate'])
            crying_vals = [w['value'] for w in window[-10:]]
            hr_vals = [w['value'] for w in hr_window[-10:]]

            if len(crying_vals) == len(hr_vals) and len(crying_vals) > 2:
                mean_diff = sum(abs(c - h/200) for c, h in zip(crying_vals, hr_vals)) / len(crying_vals)
                if mean_diff > 0.3:
                    analysis['correlation'] = '哭声与心率同步性高'
                    analysis['insight'] = '婴儿可能感到不适'
                else:
                    analysis['correlation'] = '哭声与心率无显著关联'
                    analysis['insight'] = '婴儿状态正常'

        # 体温与心率关联
        if sensor_type == 'temperature' and 'heart_rate' in self.global_windows:
            hr_window = list(self.global_windows['heart_rate'])
            temp_vals = [w['value'] for w in window[-10:]]
            hr_vals = [w['value'] for w in hr_window[-10:]]

            if len(temp_vals) == len(hr_vals) > 2:
                temp_trend = temp_vals[-1] - temp_vals[0]
                hr_trend = hr_vals[-1] - hr_vals[0]
                if temp_trend > 0.3 and hr_trend > 10:
                    analysis['correlation'] = '体温心率双升'
                    analysis['insight'] = '可能存在感染风险'
                elif temp_trend < -0.3 and hr_trend < -10:
                    analysis['correlation'] = '体温心率双降'
                    analysis['insight'] = '注意保暖'

        if not analysis:
            analysis['insight'] = '各指标运行正常'

        return analysis

    def _long_term_trend(self, sensor_type):
        """长期趋势分析"""
        window = list(self.global_windows[sensor_type])
        if len(window) < 10:
            return {'trend': 'insufficient_data'}

        values = [w['value'] for w in window]

        # 简单线性回归斜率
        n = len(values)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(values) / n
        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0

        if abs(slope) < 0.1:
            trend = 'stable'
        elif slope > 0:
            trend = 'gradually_rising'
        else:
            trend = 'gradually_falling'

        return {
            'trend': trend,
            'slope': round(slope, 4),
            'current_mean': round(statistics.mean(values[-10:]), 2),
            'historical_mean': round(statistics.mean(values[:10]), 2)
        }

    def _anomaly_score(self, sensor_type, value):
        """基于统计的异常评分（简化版Isolation Forest思想）"""
        window = list(self.global_windows[sensor_type])
        if len(window) < 5:
            return 0.0

        values = [w['value'] for w in window]
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 1

        if std == 0:
            std = 0.1

        z_score = abs(value - mean) / std
        score = min(1.0, z_score / 3.0)
        return round(score, 3)


# 云端流处理实例
cloud_stream = CloudStreamProcessor()


# ============================================================
# 云端分析模块：大数据分析 + 智能建议
# ============================================================

class CloudAnalytics:
    """云端分析模块：历史数据挖掘 + LLM风格智能建议"""

    def __init__(self):
        self.alert_patterns = self._load_alert_patterns()

    def _load_alert_patterns(self):
        """加载告警模式知识库"""
        return {
            'fever_crying': {
                'conditions': {'temperature': ('>', 37.2), 'crying': ('>', 0.5)},
                'suggestion': '婴儿可能因发热感到不适。建议：1) 检查尿布；2) 适当补水；3) 物理降温；4) 如体温持续上升，请就医。'
            },
            'prolonged_crying': {
                'conditions': {'crying': ('>', 0.6)},
                'suggestion': '婴儿持续哭闹超过正常范围。可能原因：饥饿、困倦、需要安抚、肠绞痛等。建议：检查喂养时间、检查衣物是否舒适、轻柔摇晃安抚。'
            },
            'bradycardia_fever': {
                'conditions': {'temperature': ('>', 37.5), 'heart_rate': ('<', 100)},
                'suggestion': '体温升高伴随心率偏低，罕见但需警惕。建议立即就医检查。'
            },
            'tachycardia_fever': {
                'conditions': {'temperature': ('>', 37.3), 'heart_rate': ('>', 160)},
                'suggestion': '婴儿发热，心率加快。建议：1) 测量确认体温；2) 物理降温（温水擦拭）；3) 保持室内通风；4) 半小时后复测。'
            },
            'prone_risk': {
                'conditions': {'body_position': ('==', 1)},
                'suggestion': '婴儿处于俯卧位，存在窒息风险。建议：将婴儿翻正为仰卧位，清理周围杂物，密切关注。'
            },
            'normal': {
                'conditions': {},
                'suggestion': '各项指标正常。继续常规监护，保持舒适室温(24-26°C)，注意喂养和睡眠规律。'
            }
        }

    def analyze_history(self, sensor_type, hours=24):
        """分析历史数据，返回统计摘要"""
        history_key = f"{sensor_type}_history"
        records = DATA_STORE['history'].get(sensor_type, [])
        if not records:
            return {'count': 0, 'message': '暂无历史数据'}

        values = [r['value'] for r in records]
        timestamps = [r['timestamp'] for r in records]

        analysis = {
            'count': len(values),
            'time_range': f"{timestamps[0]} ~ {timestamps[-1]}",
            'mean': round(statistics.mean(values), 2),
            'std': round(statistics.stdev(values), 2) if len(values) > 1 else 0,
            'min': round(min(values), 2),
            'max': round(max(values), 2),
            'median': round(statistics.median(values), 2),
            'normal_ratio': self._calculate_normal_ratio(sensor_type, values)
        }
        return analysis

    def _calculate_normal_ratio(self, sensor_type, values):
        """计算正常值比例"""
        ranges = {
            'temperature': (36.2, 37.2),
            'heart_rate': (80, 160),
            'crying': (0, 0.3),
            'body_position': None
        }
        if sensor_type not in ranges or ranges[sensor_type] is None:
            return 'N/A'

        low, high = ranges[sensor_type]
        normal_count = sum(1 for v in values if low <= v <= high)
        return round(normal_count / len(values), 3) if values else 0

    def generate_suggestion(self, current_data):
        """基于当前数据生成智能建议"""
        temp = current_data.get('temperature', 36.8)
        hr = current_data.get('heart_rate', 120)
        crying = current_data.get('crying', 0.1)
        position = current_data.get('body_position', 0)

        # 规则匹配
        if temp > 37.2 and crying > 0.5:
            return self.alert_patterns['fever_crying']['suggestion']
        if crying > 0.6:
            return self.alert_patterns['prolonged_crying']['suggestion']
        if temp > 37.5 and hr < 100:
            return self.alert_patterns['bradycardia_fever']['suggestion']
        if temp > 37.3 and hr > 160:
            return self.alert_patterns['tachycardia_fever']['suggestion']
        if position == 1:
            return self.alert_patterns['prone_risk']['suggestion']

        return self.alert_patterns['normal']['suggestion']

    def detect_patterns(self, hours=1):
        """检测告警模式（分布式计算风格的多轮聚合）"""
        patterns_found = []

        for pattern_name, pattern_data in self.alert_patterns.items():
            if pattern_name == 'normal':
                continue

            conditions = pattern_data['conditions']
            match = True

            for sensor, (op, threshold) in conditions.items():
                history = DATA_STORE['history'].get(sensor, [])
                if not history:
                    match = False
                    break

                recent = [r['value'] for r in history[-10:]]
                if not recent:
                    match = False
                    break

                latest = recent[-1]
                if op == '>' and latest <= threshold:
                    match = False
                elif op == '<' and latest >= threshold:
                    match = False
                elif op == '==' and latest != threshold:
                    match = False

            if match:
                patterns_found.append({
                    'pattern': pattern_name,
                    'suggestion': pattern_data['suggestion'],
                    'timestamp': datetime.now().isoformat()
                })

        return patterns_found


# 云端分析实例
analytics = CloudAnalytics()


# ============================================================
# 计算任务划分调度器
# ============================================================

class ComputeScheduler:
    """云边端计算任务划分调度器"""

    def __init__(self):
        self.partition_rules = {
            'realtime_filter': 'edge',
            'immediate_alert': 'edge',
            'local_cache': 'edge',
            'stream_stats': 'edge',
            'data_aggregation': 'fog',
            'short_term_trend': 'fog',
            'noise_reduction': 'fog',
            'long_term_analysis': 'cloud',
            'anomaly_detection': 'cloud',
            'llm_suggestion': 'cloud',
            'report_generation': 'cloud'
        }

    def get_compute_layer(self, task):
        """根据任务类型确定计算层"""
        return self.partition_rules.get(task, 'cloud')

    def get_recommendation(self, sensor_type, value, context=None):
        """根据数据决定在哪里执行计算"""
        # 实时性要求高的任务下沉到边缘
        urgent_tasks = ['realtime_filter', 'immediate_alert']
        # 大数据量分析在云端
        heavy_tasks = ['long_term_analysis', 'report_generation']

        if sensor_type in ['temperature', 'heart_rate', 'body_position']:
            return {
                'compute_layer': 'edge',
                'reason': '实时性要求高，本地处理',
                'local_action': 'immediate_alert'
            }
        elif sensor_type == 'crying' and value > 0.7:
            return {
                'compute_layer': 'edge',
                'reason': '紧急哭声，需立即响应',
                'local_action': 'immediate_alert'
            }
        else:
            return {
                'compute_layer': 'cloud',
                'reason': '需综合分析，汇聚处理',
                'local_action': 'stream_stats'
            }


# 调度器实例
scheduler = ComputeScheduler()


# ============================================================
# Flask 路由
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'uptime': DATA_STORE['stats']['uptime_start']
    })


@app.route('/api/edge/status', methods=['GET'])
def edge_status():
    """边缘节点状态"""
    return jsonify(EDGE_NODE_STATUS)


@app.route('/api/data/stream', methods=['POST'])
def receive_stream_data():
    """接收边缘层流数据（流处理入口）"""
    data = request.get_json()

    if not data:
        return jsonify({'error': 'No data provided'}), 400

    sensor_type = data.get('sensor_type', 'unknown')
    value = float(data.get('value', 0))
    timestamp = data.get('timestamp', datetime.now().isoformat())
    source = data.get('source', 'edge')

    # 云端处理
    cloud_result = cloud_stream.process(sensor_type, value, timestamp, source)

    # 存储
    record = {
        'sensor_type': sensor_type,
        'value': value,
        'timestamp': timestamp,
        'source': source,
        'cloud_analysis': cloud_result.get('cloud_analysis', {})
    }
    DATA_STORE['realtime'].append(record)
    DATA_STORE['history'][sensor_type].append(record)
    DATA_STORE['stats']['total_records'] += 1

    # 如果云端检测到异常，生成告警
    anomaly_score = cloud_result.get('cloud_analysis', {}).get('anomaly_score', 0)
    if anomaly_score > 0.7:
        alert = {
            'type': 'cloud_anomaly',
            'sensor_type': sensor_type,
            'value': value,
            'score': anomaly_score,
            'timestamp': timestamp,
            'message': f'云端检测到异常：{sensor_type}={value}，异常分数={anomaly_score}'
        }
        DATA_STORE['alerts'].append(alert)
        DATA_STORE['stats']['total_alerts'] += 1

    return jsonify({
        'status': 'received',
        'cloud_analysis': cloud_result['cloud_analysis'],
        'compute_layer': scheduler.get_compute_layer('long_term_analysis')
    })


@app.route('/api/data/batch', methods=['POST'])
def receive_batch_data():
    """接收批量数据（批处理入口）"""
    data = request.get_json()

    if not data or 'records' not in data:
        return jsonify({'error': 'No records provided'}), 400

    records = data['records']
    results = []

    for record in records:
        sensor_type = record.get('sensor_type', 'unknown')
        value = float(record.get('value', 0))
        timestamp = record.get('timestamp', datetime.now().isoformat())

        cloud_result = cloud_stream.process(sensor_type, value, timestamp, 'batch')

        store_record = {
            'sensor_type': sensor_type,
            'value': value,
            'timestamp': timestamp,
            'source': 'batch',
            'cloud_analysis': cloud_result.get('cloud_analysis', {})
        }
        DATA_STORE['history'][sensor_type].append(store_record)
        results.append(store_record)

    DATA_STORE['stats']['total_records'] += len(records)

    return jsonify({
        'status': 'processed',
        'count': len(records),
        'message': f'批处理完成，处理了 {len(records)} 条记录'
    })


@app.route('/api/realtime', methods=['GET'])
def get_realtime():
    """获取实时数据"""
    limit = request.args.get('limit', 50, type=int)
    sensor = request.args.get('sensor', None)

    data = list(DATA_STORE['realtime'])
    if sensor:
        data = [d for d in data if d['sensor_type'] == sensor]
    data = data[-limit:]

    return jsonify({
        'count': len(data),
        'data': data
    })


@app.route('/api/history/<sensor_type>', methods=['GET'])
def get_history(sensor_type):
    """获取历史数据（云端批处理分析）"""
    hours = request.args.get('hours', 24, type=int)
    limit = request.args.get('limit', 200, type=int)

    history = DATA_STORE['history'].get(sensor_type, [])
    data = history[-limit:]

    # 云端分析
    analysis = analytics.analyze_history(sensor_type, hours)

    return jsonify({
        'sensor_type': sensor_type,
        'count': len(data),
        'analysis': analysis,
        'data': data
    })


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """获取告警记录"""
    limit = request.args.get('limit', 50, type=int)
    level = request.args.get('level', None)

    alerts = list(DATA_STORE['alerts'])
    if level:
        alerts = [a for a in alerts if a.get('level') == level]
    alerts = alerts[-limit:]

    return jsonify({
        'count': len(alerts),
        'alerts': alerts
    })


@app.route('/api/alerts/edge', methods=['POST'])
def receive_edge_alert():
    """接收边缘层告警"""
    alert = request.get_json()

    if not alert:
        return jsonify({'error': 'No alert data'}), 400

    alert['timestamp'] = datetime.now().isoformat()
    alert['source'] = 'edge'

    DATA_STORE['alerts'].append(alert)
    DATA_STORE['stats']['total_alerts'] += 1

    return jsonify({'status': 'alert_received', 'alert': alert})


@app.route('/api/analytics/suggestion', methods=['GET'])
def get_suggestion():
    """获取智能建议（模拟LLM大模型建议）"""
    # 收集当前最新数据
    current = {}
    for sensor in ['temperature', 'heart_rate', 'crying', 'body_position']:
        history = DATA_STORE['history'].get(sensor, [])
        if history:
            current[sensor] = history[-1]['value']

    suggestion = analytics.generate_suggestion(current)
    patterns = analytics.detect_patterns()

    return jsonify({
        'suggestion': suggestion,
        'current_data': current,
        'patterns_detected': patterns,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/analytics/trend', methods=['GET'])
def get_trend():
    """获取趋势分析"""
    sensor = request.args.get('sensor', 'temperature')
    window = cloud_stream.global_windows.get(sensor, [])

    trend = cloud_stream._long_term_trend(sensor)

    return jsonify({
        'sensor_type': sensor,
        'trend': trend,
        'window_size': len(window)
    })


@app.route('/api/compute/partition', methods=['GET'])
def get_compute_partition():
    """获取计算划分信息"""
    return jsonify({
        'partition': COMPUTE_PARTITION,
        'scheduler_rules': scheduler.partition_rules
    })


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """获取系统统计"""
    return jsonify({
        'stats': DATA_STORE['stats'],
        'store_sizes': {
            'realtime': len(DATA_STORE['realtime']),
            'alerts': len(DATA_STORE['alerts']),
            'history_keys': list(DATA_STORE['history'].keys())
        },
        'edge_status': EDGE_NODE_STATUS
    })


@app.route('/api/session/start', methods=['POST'])
def start_session():
    """开始监护会话"""
    data = request.get_json() or {}
    session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    DATA_STORE['sessions'][session_id] = {
        'id': session_id,
        'infant_name': data.get('infant_name', '宝宝'),
        'infant_age_months': data.get('infant_age_months', 6),
        'start_time': datetime.now().isoformat(),
        'status': 'active'
    }

    return jsonify({'session_id': session_id, 'status': 'started'})


@app.route('/api/latest', methods=['GET'])
def get_latest():
    """获取最新传感器数据（GUI轮询用）"""
    sensor = request.args.get('sensor', None)
    limit = request.args.get('limit', 1, type=int)

    data = list(DATA_STORE['realtime'])
    if sensor:
        data = [d for d in data if d['sensor_type'] == sensor]

    latest_by_sensor = {}
    for record in reversed(data):
        stype = record['sensor_type']
        if stype not in latest_by_sensor:
            latest_by_sensor[stype] = []

    for stype in list(latest_by_sensor.keys()):
        sensor_data = [d for d in data if d['sensor_type'] == stype]
        latest_by_sensor[stype] = sensor_data[-limit:]

    records = []
    for stype, entries in latest_by_sensor.items():
        for entry in entries:
            alerts = []
            if stype == 'temperature':
                if entry['value'] >= 37.5 or entry['value'] <= 35.5:
                    alerts.append({'level': 'critical', 'message': f'体温异常：{entry["value"]}°C', 'timestamp': entry['timestamp']})
                elif entry['value'] >= 37.2 and entry['value'] < 37.5:
                    alerts.append({'level': 'warning', 'message': f'体温需关注：{entry["value"]}°C', 'timestamp': entry['timestamp']})
                elif entry['value'] >= 36.0 and entry['value'] < 37.2:
                    alerts.append({'level': 'info', 'message': f'体温偏低：{entry["value"]}°C', 'timestamp': entry['timestamp']})
            elif stype == 'heart_rate':
                if entry['value'] >= 170 or entry['value'] <= 70:
                    alerts.append({'level': 'critical', 'message': f'心率异常：{entry["value"]}bpm', 'timestamp': entry['timestamp']})
                elif entry['value'] >= 160 and entry['value'] < 170:
                    alerts.append({'level': 'warning', 'message': f'心率需关注：{entry["value"]}bpm', 'timestamp': entry['timestamp']})
                elif entry['value'] >= 80 and entry['value'] < 160:
                    alerts.append({'level': 'info', 'message': f'心率偏低：{entry["value"]}bpm', 'timestamp': entry['timestamp']})
            elif stype == 'crying':
                if entry['value'] > 0.5:
                    alerts.append({'level': 'warning', 'message': '持续哭闹', 'timestamp': entry['timestamp']})
                elif entry['value'] > 0.3:
                    alerts.append({'level': 'info', 'message': '轻微哭闹', 'timestamp': entry['timestamp']})
            elif stype == 'body_position':
                if entry['value'] == 1:
                    alerts.append({'level': 'warning', 'message': '俯卧位告警', 'timestamp': entry['timestamp']})

            records.append({
                'sensor_type': stype,
                'value': entry['value'],
                'timestamp': entry['timestamp'],
                'alerts': alerts
            })

    return jsonify({'data': records})


@app.route('/api/session/<session_id>', methods=['GET'])
def get_session(session_id):
    """获取会话详情"""
    session = DATA_STORE['sessions'].get(session_id)
    if not session:
        return jsonify({'error': 'Session not found'}), 404

    session['total_records'] = DATA_STORE['stats']['total_records']
    session['total_alerts'] = DATA_STORE['stats']['total_alerts']

    return jsonify(session)


# ============================================================
# 数据模拟：自动生成测试数据
# ============================================================

def simulate_data_stream():
    """后台线程：模拟边缘节点发送数据"""
    import random

    while True:
        try:
            temp = round(random.uniform(36.2, 37.5), 2)
            hr = random.randint(85, 165)
            crying = round(random.random() * 0.8, 2)
            position = random.choice([0, 0, 2])  # 偶尔俯卧

            for sensor, value in [('temperature', temp), ('heart_rate', hr),
                                    ('crying', crying), ('body_position', position)]:
                cloud_result = cloud_stream.process(
                    sensor, value, datetime.now().isoformat(), 'simulator'
                )
                record = {
                    'sensor_type': sensor,
                    'value': value,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'simulator',
                    'cloud_analysis': cloud_result.get('cloud_analysis', {})
                }
                DATA_STORE['realtime'].append(record)
                DATA_STORE['history'][sensor].append(record)
                DATA_STORE['stats']['total_records'] += 1

                # 边缘层告警检测
                alerts_to_add = []
                if sensor == 'temperature' and (value > 37.5 or value < 35.5):
                    alerts_to_add.append({
                        'type': 'edge_alert', 'sensor_type': sensor,
                        'value': value, 'level': 'critical',
                        'timestamp': datetime.now().isoformat(),
                        'message': f'体温异常：{value}°C'
                    })
                if sensor == 'crying' and value > 0.7:
                    alerts_to_add.append({
                        'type': 'edge_alert', 'sensor_type': sensor,
                        'value': value, 'level': 'warning',
                        'timestamp': datetime.now().isoformat(),
                        'message': f'持续哭闹检测'
                    })
                if sensor == 'body_position' and value == 1:
                    alerts_to_add.append({
                        'type': 'edge_alert', 'sensor_type': sensor,
                        'value': value, 'level': 'warning',
                        'timestamp': datetime.now().isoformat(),
                        'message': f'俯卧位告警'
                    })

                for alert in alerts_to_add:
                    DATA_STORE['alerts'].append(alert)
                    DATA_STORE['stats']['total_alerts'] += 1

            EDGE_NODE_STATUS['last_heartbeat'] = datetime.now().isoformat()
            EDGE_NODE_STATUS['data_rate'] += 4

            time.sleep(3)
        except Exception:
            time.sleep(3)


def start_simulation():
    """启动数据模拟线程（在模块模式被外部调用时使用）"""
    sim_thread = threading.Thread(target=simulate_data_stream, daemon=True)
    sim_thread.start()


# ============================================================
# 主入口
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("婴幼儿智能监护系统 - 云端服务")
    print("=" * 60)
    print("云边端计算划分：")
    for layer, tasks in COMPUTE_PARTITION.items():
        print(f"  {layer}: {', '.join(tasks)}")
    print()

    # 启动数据模拟线程
    start_simulation()

    print("后端服务启动中...")
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
