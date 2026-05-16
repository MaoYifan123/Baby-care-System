#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婴幼儿智能监护系统 - 大数据分析模块
Big Data Analytics Module

使用分布式计算思想进行多维度数据分析和趋势预测。
功能：
1. 多传感器数据聚合分析
2. 婴儿健康趋势预测
3. 异常模式挖掘
4. 历史数据统计报告
"""

import statistics
import random
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import List, Dict, Optional, Tuple


# ============================================================
# 分布式风格的数据分片处理
# ============================================================

class DataShardProcessor:
    """数据分片处理器 - 模拟分布式计算的数据切分"""

    def __init__(self, shard_size=100):
        self.shard_size = shard_size
        self.shards = []

    def split_data(self, data: List) -> List[List]:
        """将数据切分成多个分片（MapReduce风格）"""
        shards = []
        for i in range(0, len(data), self.shard_size):
            shards.append(data[i:i + self.shard_size])
        self.shards = shards
        return shards

    def map_phase(self, mapper_func, parallel=False):
        """Map阶段：并行处理每个分片"""
        results = []
        for shard in self.shards:
            mapped = [mapper_func(item) for item in shard]
            results.extend(mapped)
        return results

    def reduce_phase(self, reducer_func, mapped_data):
        """Reduce阶段：汇总Map结果"""
        return reducer_func(mapped_data)


# ============================================================
# 健康趋势分析
# ============================================================

class HealthTrendAnalyzer:
    """婴儿健康趋势分析器"""

    # 婴儿正常参考范围（按月龄）
    REFERENCE_RANGES = {
        'temperature': {'min': 36.2, 'max': 37.2, 'unit': '°C'},
        'heart_rate': {
            'newborn': {'min': 70, 'max': 170},
            'infant': {'min': 80, 'max': 160},
            'toddler': {'min': 80, 'max': 140}
        },
        'sleep_hours': {
            'newborn': 16, 'infant': 14, 'toddler': 12
        }
    }

    def __init__(self, infant_age_months=6):
        self.infant_age_months = infant_age_months
        self.age_group = self._get_age_group()

    def _get_age_group(self):
        if self.infant_age_months < 1:
            return 'newborn'
        elif self.infant_age_months < 12:
            return 'infant'
        return 'toddler'

    def calculate_health_score(self, data_points: Dict[str, List[float]]) -> Dict:
        """计算综合健康评分 (0-100)"""
        scores = {}

        # 体温评分
        temps = data_points.get('temperature', [])
        if temps:
            temp_mean = statistics.mean(temps)
            temp_dev = abs(temp_mean - 36.8)
            scores['temperature'] = max(0, 100 - temp_dev * 50)

        # 心率评分
        hrs = data_points.get('heart_rate', [])
        if hrs:
            hr_mean = statistics.mean(hrs)
            ref = self.REFERENCE_RANGES['heart_rate'][self.age_group]
            hr_mid = (ref['min'] + ref['max']) / 2
            hr_dev = abs(hr_mean - hr_mid)
            scores['heart_rate'] = max(0, 100 - hr_dev * 2)

        # 哭闹评分（越低越好）
        crying = data_points.get('crying', [])
        if crying:
            crying_mean = statistics.mean(crying)
            scores['crying'] = max(0, 100 - crying_mean * 100)

        # 综合评分
        if scores:
            overall = statistics.mean(scores.values())
        else:
            overall = 0

        return {
            'overall_score': round(overall, 1),
            'component_scores': {k: round(v, 1) for k, v in scores.items()},
            'grade': self._score_to_grade(overall),
            'age_group': self.age_group
        }

    def _score_to_grade(self, score):
        if score >= 90:
            return '优秀'
        elif score >= 75:
            return '良好'
        elif score >= 60:
            return '一般'
        else:
            return '需关注'

    def predict_trend(self, historical_data: List[float], horizon=5) -> Dict:
        """预测未来趋势（简单线性回归）"""
        if len(historical_data) < 3:
            return {'prediction': [], 'confidence': 0, 'trend': 'insufficient_data'}

        n = len(historical_data)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(historical_data) / n

        # 线性回归
        num = sum((x[i] - x_mean) * (historical_data[i] - y_mean) for i in range(n))
        den = sum((x[i] - x_mean) ** 2 for i in range(n))
        slope = num / den if den != 0 else 0
        intercept = y_mean - slope * x_mean

        # 预测
        predictions = []
        for i in range(1, horizon + 1):
            pred = intercept + slope * (n - 1 + i)
            predictions.append(round(pred, 2))

        # 置信度
        y_pred = [intercept + slope * xi for xi in x]
        ss_res = sum((historical_data[i] - y_pred[i]) ** 2 for i in range(n))
        ss_tot = sum((historical_data[i] - y_mean) ** 2 for i in range(n))
        r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0

        return {
            'predictions': predictions,
            'slope': round(slope, 4),
            'r_squared': round(r_squared, 3),
            'confidence': round(min(1.0, r_squared), 2),
            'trend': 'rising' if slope > 0.1 else 'falling' if slope < -0.1 else 'stable',
            'intercept': round(intercept, 2)
        }


# ============================================================
# 异常模式挖掘
# ============================================================

class AnomalyPatternMiner:
    """异常模式挖掘器 - 使用统计方法和规则匹配"""

    def __init__(self):
        self.pattern_library = self._build_pattern_library()

    def _build_pattern_library(self):
        """构建告警模式知识库"""
        return {
            'fever_pattern': {
                'name': '发热模式',
                'conditions': [('temperature', '>', 37.2, 0.6)],
                'description': '体温持续高于正常值',
                'severity': 'high',
                'suggestions': [
                    '使用温水擦拭进行物理降温',
                    '保持室内温度适宜(24-26°C)',
                    '补充适量温水',
                    '每30分钟复测体温',
                    '如持续超过38°C，考虑就医'
                ]
            },
            'tachycardia_pattern': {
                'name': '心动过速模式',
                'conditions': [('heart_rate', '>', 160, 0.5)],
                'description': '心率持续偏快',
                'severity': 'medium',
                'suggestions': [
                    '检查婴儿是否发烧',
                    '排除哭闹、激动等生理因素',
                    '保持环境安静',
                    '如持续不缓解，就医检查'
                ]
            },
            'bradycardia_pattern': {
                'name': '心动过缓模式',
                'conditions': [('heart_rate', '<', 70, 0.5)],
                'description': '心率异常偏低',
                'severity': 'high',
                'suggestions': [
                    '立即检查婴儿呼吸和意识状态',
                    '保持呼吸道通畅',
                    '立即就医'
                ]
            },
            'crying_storm_pattern': {
                'name': '哭闹风暴模式',
                'conditions': [('crying', '>', 0.6, 0.7)],
                'description': '持续哭闹超过阈值',
                'severity': 'medium',
                'suggestions': [
                    '检查饥饿和尿布状态',
                    '排除肠绞痛可能',
                    '轻柔摇晃安抚',
                    '播放白噪音',
                    '如超过30分钟持续哭闹，请就医'
                ]
            },
            'prone_risk_pattern': {
                'name': '俯卧窒息风险模式',
                'conditions': [('body_position', '==', 1, 0.8)],
                'description': '婴儿处于俯卧位',
                'severity': 'high',
                'suggestions': [
                    '立即将婴儿翻正为仰卧位',
                    '清理周围可能导致窒息的物品',
                    '婴儿睡眠时应始终仰卧位',
                    '密切观察呼吸情况'
                ]
            },
            'fever_tachycardia_pattern': {
                'name': '发热心动过速综合模式',
                'conditions': [
                    ('temperature', '>', 37.3, 0.5),
                    ('heart_rate', '>', 150, 0.5)
                ],
                'description': '发热伴随心率加快',
                'severity': 'high',
                'suggestions': [
                    '高度怀疑感染',
                    '进行物理降温',
                    '监测体温变化',
                    '建议就医进行血常规检查'
                ]
            },
            'sleep_deprivation_pattern': {
                'name': '睡眠不足模式',
                'conditions': [('crying', '>', 0.4, 0.8)],
                'description': '频繁哭闹可能表示睡眠不足',
                'severity': 'low',
                'suggestions': [
                    '建立规律作息',
                    '注意睡眠信号（揉眼睛、打哈欠）',
                    '创造安静的睡眠环境',
                    '白天适当活动'
                ]
            }
        }

    def mine_patterns(self, recent_data: Dict[str, List[float]]) -> List[Dict]:
        """从最近数据中挖掘匹配的模式"""
        matched = []

        for pattern_id, pattern in self.pattern_library.items():
            conditions = pattern['conditions']
            match_count = 0

            for sensor, op, threshold, weight in conditions:
                values = recent_data.get(sensor, [])
                if not values:
                    continue

                latest = values[-1]
                recent_avg = statistics.mean(values[-5:]) if len(values) >= 5 else statistics.mean(values)

                if op == '>' and (latest > threshold or recent_avg > threshold):
                    match_count += weight
                elif op == '<' and (latest < threshold or recent_avg < threshold):
                    match_count += weight
                elif op == '==' and int(latest) == int(threshold):
                    match_count += weight

            if match_count >= 0.5:
                matched.append({
                    'pattern_id': pattern_id,
                    'name': pattern['name'],
                    'description': pattern['description'],
                    'severity': pattern['severity'],
                    'confidence': round(min(1.0, match_count), 2),
                    'suggestions': pattern['suggestions'],
                    'timestamp': datetime.now().isoformat()
                })

        # 按严重程度排序
        severity_order = {'high': 0, 'medium': 1, 'low': 2}
        matched.sort(key=lambda x: severity_order.get(x['severity'], 3))

        return matched


# ============================================================
# 历史数据统计分析
# ============================================================

class HistoricalStatsAnalyzer:
    """历史数据统计分析器"""

    def __init__(self):
        self.data = defaultdict(list)

    def add_data(self, sensor_type: str, records: List[Dict]):
        """添加历史数据"""
        self.data[sensor_type].extend(records)

    def generate_report(self, hours=24) -> Dict:
        """生成统计报告"""
        report = {
            'generated_at': datetime.now().isoformat(),
            'period_hours': hours,
            'sensors': {}
        }

        for sensor, records in self.data.items():
            if not records:
                continue

            values = [r['value'] for r in records]
            timestamps = [r['timestamp'] for r in records]

            sensor_stats = {
                'count': len(values),
                'time_range': {
                    'start': timestamps[0] if timestamps else None,
                    'end': timestamps[-1] if timestamps else None
                },
                'statistics': {
                    'mean': round(statistics.mean(values), 2),
                    'median': round(statistics.median(values), 2),
                    'std': round(statistics.stdev(values), 2) if len(values) > 1 else 0,
                    'min': round(min(values), 2),
                    'max': round(max(values), 2),
                    'range': round(max(values) - min(values), 2)
                },
                'distribution': self._calculate_distribution(values)
            }

            report['sensors'][sensor] = sensor_stats

        # 综合分析
        report['summary'] = self._generate_summary(report['sensors'])

        return report

    def _calculate_distribution(self, values: List[float]) -> Dict:
        """计算数据分布"""
        if not values:
            return {}

        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 0

        below_mean = sum(1 for v in values if v < mean)
        above_mean = sum(1 for v in values if v > mean)
        within_1std = sum(1 for v in values if abs(v - mean) <= std)

        return {
            'below_mean_ratio': round(below_mean / len(values), 3),
            'above_mean_ratio': round(above_mean / len(values), 3),
            'within_1std_ratio': round(within_1std / len(values), 3),
            'skewness': self._calculate_skewness(values)
        }

    def _calculate_skewness(self, values: List[float]) -> str:
        """计算偏度"""
        if len(values) < 3:
            return 'insufficient_data'

        n = len(values)
        mean = statistics.mean(values)
        std = statistics.stdev(values)

        if std == 0:
            return 'symmetric'

        skew = sum((v - mean) ** 3 for v in values) / (n * std ** 3)

        if skew > 0.5:
            return 'right_skewed'
        elif skew < -0.5:
            return 'left_skewed'
        else:
            return 'symmetric'

    def _generate_summary(self, sensors: Dict) -> Dict:
        """生成综合摘要"""
        total_records = sum(s['count'] for s in sensors.values())

        # 正常率评估
        normal_rates = {}
        for sensor, stats in sensors.items():
            if sensor == 'temperature':
                normal = sum(1 for v in self.data.get(sensor, [])
                             if 36.2 <= v['value'] <= 37.2)
                normal_rates[sensor] = round(normal / stats['count'], 3) if stats['count'] else 0
            elif sensor == 'heart_rate':
                normal = sum(1 for v in self.data.get(sensor, [])
                             if 80 <= v['value'] <= 160)
                normal_rates[sensor] = round(normal / stats['count'], 3) if stats['count'] else 0

        return {
            'total_records': total_records,
            'normal_rates': normal_rates,
            'health_assessment': '正常' if all(r > 0.9 for r in normal_rates.values()) else '需关注'
        }


# ============================================================
# 模拟数据生成器（用于测试）
# ============================================================

class DataSimulator:
    """模拟传感器数据生成器"""

    def __init__(self, seed=None):
        if seed is not None:
            random.seed(seed)

    def generate_historical_data(self, hours=24, interval_minutes=5) -> Dict[str, List[Dict]]:
        """生成历史模拟数据"""
        import random
        from datetime import timedelta

        records = defaultdict(list)
        start_time = datetime.now() - timedelta(hours=hours)
        total_points = hours * 60 // interval_minutes

        for i in range(total_points):
            timestamp = (start_time + timedelta(minutes=i * interval_minutes)).isoformat()

            # 体温：正常波动
            temp = round(36.6 + random.gauss(0, 0.3) + random.choice([0, 0, 0, 0.2, -0.1]), 2)
            temp = max(35.5, min(38.5, temp))
            records['temperature'].append({'timestamp': timestamp, 'value': temp})

            # 心率：随时间和活动变化
            base_hr = 110
            hr = round(base_hr + random.gauss(0, 15) + random.choice([-20, -10, 0, 10, 20]))
            hr = max(60, min(200, hr))
            records['heart_rate'].append({'timestamp': timestamp, 'value': hr})

            # 哭声：偶尔发生
            crying = random.random()
            if random.random() < 0.15:
                crying = round(random.uniform(0.5, 0.9), 2)
            else:
                crying = round(random.uniform(0, 0.2), 2)
            records['crying'].append({'timestamp': timestamp, 'value': crying})

            # 体位：主要是仰卧
            position = random.choices([0, 1, 2, 3], weights=[70, 10, 15, 5])[0]
            records['body_position'].append({'timestamp': timestamp, 'value': position})

        return dict(records)


# ============================================================
# 主入口：演示分析
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("婴幼儿智能监护系统 - 大数据分析模块")
    print("=" * 60)

    # 1. 生成模拟历史数据
    print("\n[1] 生成模拟历史数据...")
    simulator = DataSimulator(seed=42)
    historical = simulator.generate_historical_data(hours=6, interval_minutes=5)

    for sensor, records in historical.items():
        print(f"     {sensor}: {len(records)} 条记录")

    # 2. 历史统计分析
    print("\n[2] 历史数据统计分析...")
    stats_analyzer = HistoricalStatsAnalyzer()
    stats_analyzer.add_data('temperature', historical['temperature'])
    stats_analyzer.add_data('heart_rate', historical['heart_rate'])
    stats_analyzer.add_data('crying', historical['crying'])

    report = stats_analyzer.generate_report(hours=6)
    print(f"\n     统计报告:")
    for sensor, data in report['sensors'].items():
        s = data['statistics']
        print(f"     {sensor}: 均值={s['mean']}, 标准差={s['std']}, "
              f"范围=[{s['min']}, {s['max']}]")
    print(f"     健康评估: {report['summary']['health_assessment']}")

    # 3. 趋势预测
    print("\n[3] 健康趋势预测...")
    trend_analyzer = HealthTrendAnalyzer(infant_age_months=6)

    temp_values = [r['value'] for r in historical['temperature']]
    hr_values = [r['value'] for r in historical['heart_rate']]

    temp_trend = trend_analyzer.predict_trend(temp_values[-30:])
    hr_trend = trend_analyzer.predict_trend(hr_values[-30:])

    print(f"     体温趋势: {temp_trend['trend']}, "
          f"预测: {temp_trend['predictions']}, "
          f"R²={temp_trend['r_squared']}")
    print(f"     心率趋势: {hr_trend['trend']}, "
          f"预测: {hr_trend['predictions']}, "
          f"R²={hr_trend['r_squared']}")

    # 4. 健康评分
    print("\n[4] 综合健康评分...")
    data_points = {
        'temperature': temp_values[-20:],
        'heart_rate': hr_values[-20:],
        'crying': [r['value'] for r in historical['crying']][-20:]
    }
    score = trend_analyzer.calculate_health_score(data_points)
    print(f"     综合评分: {score['overall_score']}/100 ({score['grade']})")
    for k, v in score['component_scores'].items():
        print(f"       {k}: {v}")

    # 5. 异常模式挖掘
    print("\n[5] 异常模式挖掘...")
    miner = AnomalyPatternMiner()

    recent_data = {
        'temperature': temp_values[-10:],
        'heart_rate': hr_values[-10:],
        'crying': [r['value'] for r in historical['crying']][-10:],
        'body_position': [r['value'] for r in historical['body_position']][-10:]
    }

    patterns = miner.mine_patterns(recent_data)
    if patterns:
        print(f"     检测到 {len(patterns)} 个模式:")
        for p in patterns:
            print(f"     - {p['name']} (严重程度: {p['severity']}, 置信度: {p['confidence']})")
            for s in p['suggestions'][:2]:
                print(f"       建议: {s}")
    else:
        print("     未检测到异常模式")

    # 6. 分布式计算演示
    print("\n[6] 分布式计算演示 (MapReduce风格)...")
    processor = DataShardProcessor(shard_size=20)

    # 模拟大量数据
    big_data = [{'value': v} for v in temp_values]
    shards = processor.split_data(big_data)
    print(f"     数据分片: {len(shards)} 个分片, 每片 {processor.shard_size} 条")

    # Map阶段
    mapped = processor.map_phase(lambda x: {'squared': x['value'] ** 2})
    print(f"     Map阶段: 处理了 {len(mapped)} 条")

    # Reduce阶段
    total = sum(m['squared'] for m in mapped)
    mean_sq = total / len(mapped) if mapped else 0
    rms = mean_sq ** 0.5
    print(f"     Reduce阶段: RMS = {round(rms, 3)}")

    print("\n" + "=" * 60)
    print("大数据分析模块演示完成！")
    print("=" * 60)
