#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婴幼儿智能监护系统 - 主启动脚本
支持多种运行模式：独立运行、云端+GUI、测试模式
"""

import sys
import time
import argparse


def main():
    parser = argparse.ArgumentParser(description='婴幼儿智能监护系统')
    parser.add_argument('--mode', '-m', choices=['all', 'cloud', 'edge', 'gui', 'test'],
                        default='all', help='运行模式')
    parser.add_argument('--port', '-p', type=int, default=5000,
                        help='云端服务端口 (默认: 5000)')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='云端服务地址 (默认: 127.0.0.1)')
    parser.add_argument('--no-gui', action='store_true',
                        help='不启动GUI（仅云端服务）')
    parser.add_argument('--simulate', '-s', action='store_true',
                        help='启动数据模拟器')
    parser.add_argument('--test', '-t', action='store_true',
                        help='运行测试模式')

    args = parser.parse_args()

    print("=" * 60)
    print("  婴幼儿智能监护系统 - Cloud-Edge Orchestration")
    print("=" * 60)
    print()

    if args.mode == 'test' or args.test:
        print("[模式] 测试模式")
        run_test()
        return

    if args.mode == 'edge':
        print("[模式] 边缘节点独立模式")
        run_edge_only()
        return

    if args.mode == 'cloud':
        print("[模式] 云端服务模式")
        run_cloud_only(args)
        return

    if args.mode == 'gui':
        print("[模式] GUI独立模式")
        run_gui_only(args)
        return

    # 默认: all - 启动完整系统
    print("[模式] 完整系统（云端 + GUI）")
    run_full_system(args)


def run_edge_only():
    """仅运行边缘节点"""
    from edge_node import EdgeNodeSimulator
    import signal

    node = EdgeNodeSimulator()

    def on_data(data):
        alerts = f" [ALERT: {data['alerts']}]" if data['alerts'] else ""
        print(f"[Edge] {data['sensor_type']:15s} = {data['value']:8.2f} | {data['analysis']}{alerts}")

    node.set_data_callback(on_data)
    node.start(interval=2.0)

    print("边缘节点已启动，按 Ctrl+C 停止...")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        node.stop()
        print("\n边缘节点已停止")


def run_cloud_only(args):
    """仅运行云端服务"""
    import cloud_server
    print(f"云端服务启动中: http://{args.host}:{args.port}")
    print("按 Ctrl+C 停止")
    cloud_server.app.run(host=args.host, port=args.port, debug=False, threaded=True)


def run_gui_only(args):
    """仅运行GUI"""
    import sys
    from PyQt5 import QtWidgets

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    app.setApplicationName('宝宝智能监护系统')

    from gui_monitor import MainWindow
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


def run_full_system(args):
    """运行完整系统（云端 + GUI）"""
    import threading
    import sys as _sys

    # 1. 先启动边缘节点模拟
    print("[1/4] 启动边缘计算节点...")
    from edge_node import EdgeNodeSimulator
    edge_node = EdgeNodeSimulator()

    def on_edge_data(result):
        alerts = f" [ALERT]" if result['alerts'] else ""
        print(f"     Edge: {result['sensor_type']:15s}={result['value']:7.2f}{alerts}")

    edge_node.set_data_callback(on_edge_data)
    edge_node.start(interval=2.0)
    print("     边缘计算节点已启动")

    # 2. 提前启动 Flask 云端服务（在 Qt 事件循环之前）
    print("[2/4] 启动云端服务...")
    import cloud_server as cs

    def run_server():
        cs.app.run(host='0.0.0.0', port=args.port, debug=False, threaded=True, use_reloader=False)

    flask_thread = threading.Thread(target=run_server, daemon=True)
    flask_thread.start()

    # 等待 Flask 完全启动（避免 DataCollector 立即连接失败）
    time.sleep(1.0)

    # 启动数据模拟线程
    cs.start_simulation()
    print(f"     云端服务已启动: http://127.0.0.1:{args.port}")
    print("     数据模拟线程已启动")

    # 3. 创建 QApplication（必须在导入任何 QWidget 之前）
    print("[3/4] 启动监控界面...")
    from PyQt5 import QtWidgets, QtCore

    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    qt_app = QtWidgets.QApplication(_sys.argv)
    qt_app.setStyle('Fusion')
    qt_app.setApplicationName('宝宝智能监护系统')

    # 4. QApplication 创建后才导入 GUI 模块
    from gui_monitor import MainWindow

    window = MainWindow()
    window.show()

    print("[4/4] 验证服务连接...")
    # 验证 Flask 服务可用
    import requests
    try:
        r = requests.get(f'http://127.0.0.1:{args.port}/api/health', timeout=2)
        if r.status_code == 200:
            print("     云端服务连接正常")
    except Exception:
        print("     警告: 云端服务连接失败，GUI 将重试连接")

    print()
    print("=" * 60)
    print("  所有服务已就绪！")
    print(f"  - 云端服务: http://127.0.0.1:{args.port}")
    print("  - 边缘节点: 模拟器运行中")
    print("  - 监控界面: 已打开")
    print("=" * 60)

    def cleanup():
        edge_node.stop()
        print("系统已关闭")

    qt_app.aboutToQuit.connect(cleanup)

    _sys.exit(qt_app.exec_())


def run_test():
    """测试模式：运行所有模块的自检"""
    print("[测试] 婴幼儿智能监护系统自检\n")

    tests_passed = 0
    tests_total = 0

    # 测试1: 边缘节点
    print("[测试 1] 边缘计算节点...")
    tests_total += 1
    try:
        from edge_node import EdgeStreamProcessor, EdgeNodeSimulator
        proc = EdgeStreamProcessor(window_size=5)

        results = []
        def cb(r): results.append(r)
        proc.set_alert_callback(cb)

        for i in range(5):
            proc.process_data('temperature', 36.5 + i * 0.1)
            proc.process_data('heart_rate', 100 + i * 5)
            proc.process_data('crying', 0.1)
            proc.process_data('body_position', 0)

        summary = proc.get_summary()
        assert len(summary) >= 1
        print(f"     PASS - 处理了{len(results)}条数据，摘要: {list(summary.keys())}")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    # 测试2: 云端流处理
    print("[测试 2] 云端流处理引擎...")
    tests_total += 1
    try:
        import statistics
        from datetime import datetime
        from collections import deque, defaultdict

        from cloud_server import CloudStreamProcessor, CloudAnalytics, ComputeScheduler

        stream = CloudStreamProcessor()
        for i in range(15):
            stream.process('temperature', 36.5 + (i % 5) * 0.1)
            stream.process('heart_rate', 110 + (i % 3) * 10)

        trend = stream._long_term_trend('temperature')
        assert trend['trend'] in ['stable', 'rising', 'falling', 'insufficient_data']
        print(f"     PASS - 趋势分析: {trend['trend']}, 斜率: {trend.get('slope', 0):.4f}")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    # 测试3: 计算划分调度器
    print("[测试 3] 计算任务划分调度器...")
    tests_total += 1
    try:
        from cloud_server import ComputeScheduler
        scheduler = ComputeScheduler()

        assert scheduler.get_compute_layer('immediate_alert') == 'edge'
        assert scheduler.get_compute_layer('long_term_analysis') == 'cloud'
        assert scheduler.get_compute_layer('data_aggregation') == 'fog'

        rec = scheduler.get_recommendation('temperature', 38.0)
        assert rec['compute_layer'] == 'edge'
        print(f"     PASS - 任务划分正确，紧急任务下沉边缘: {rec['compute_layer']}")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    # 测试4: 云端分析模块
    print("[测试 4] 云端分析模块（智能建议）...")
    tests_total += 1
    try:
        from cloud_server import CloudAnalytics
        analytics = CloudAnalytics()

        # 测试正常情况
        suggestion1 = analytics.generate_suggestion({
            'temperature': 36.8, 'heart_rate': 120, 'crying': 0.1, 'body_position': 0
        })
        assert '正常' in suggestion1 or '正常' in suggestion1

        # 测试发热+哭闹
        suggestion2 = analytics.generate_suggestion({
            'temperature': 37.8, 'heart_rate': 130, 'crying': 0.8, 'body_position': 0
        })
        assert len(suggestion2) > 10

        print(f"     PASS - 正常建议: {suggestion1[:40]}...")
        print(f"     PASS - 异常建议: {suggestion2[:40]}...")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    # 测试5: 边缘节点批量数据生成
    print("[测试 5] 边缘节点批量数据生成...")
    tests_total += 1
    try:
        from edge_node import EdgeNodeSimulator
        node = EdgeNodeSimulator()
        batch = node.generate_batch(seconds=5, interval=0.5)
        assert len(batch) > 0
        print(f"     PASS - 生成了 {len(batch)} 条批量测试数据")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    # 测试6: GUI组件导入
    print("[测试 6] GUI组件导入检查...")
    tests_total += 1
    try:
        from gui_monitor import (RealtimeChart, MetricCard, AlertCard,
                                  MainWindow, DataCollector, CollapsiblePanel)
        print("     PASS - 所有GUI组件导入成功")
        tests_passed += 1
    except Exception as e:
        print(f"     FAIL - {e}")

    print()
    print("=" * 50)
    print(f"  测试结果: {tests_passed}/{tests_total} 通过")
    print("=" * 50)

    if tests_passed == tests_total:
        print("\n所有测试通过！系统已就绪。")
        print("运行方式:")
        print("  python main.py              # 完整系统")
        print("  python main.py --mode cloud # 仅云端")
        print("  python main.py --mode edge  # 仅边缘节点")
        print("  python main.py --test       # 运行测试")
    else:
        print(f"\n有 {tests_total - tests_passed} 个测试失败，请检查错误信息。")


if __name__ == '__main__':
    main()
