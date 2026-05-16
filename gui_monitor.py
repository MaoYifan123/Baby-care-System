#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婴幼儿智能监护系统 - 家长友好版图形界面
Warm, parent-friendly GUI for infant care monitoring
Pure Qt implementation (no matplotlib)
"""

import sys
import time
import threading
import queue
import collections
import math

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QTimer, Qt, pyqtSignal
from PyQt5.QtGui import QFont, QColor, QPainter, QBrush, QPen, QLinearGradient
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QScrollArea, QFrame, QSplitter, QSizePolicy,
    QDialog
)


# ============================================================
# Thread-safe signal emitter for cross-thread data transfer
# ============================================================

class DataSignalEmitter(QtCore.QObject):
    """Emits data updates from a worker thread to the Qt main thread via signal."""
    data_ready = QtCore.pyqtSignal(object)


# ============================================================
# Data Collector Thread
# ============================================================

class DataCollector(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
        self._first_success = False
        self._emitter = DataSignalEmitter()

    @property
    def data_ready(self):
        return self._emitter.data_ready

    def run(self):
        import requests
        poll_interval = 0.8
        while self.running:
            try:
                r = requests.get('http://127.0.0.1:5000/api/latest', timeout=4)
                if r.status_code == 200:
                    data = r.json()
                    records = data.get('data', [])
                    self._emitter.data_ready.emit(records)
                    self._first_success = True
                    time.sleep(poll_interval)
            except Exception:
                retry_delay = 0.3 if not self._first_success else 3.0
                time.sleep(retry_delay)

    def stop(self):
        self.running = False


# ============================================================
# Pure Qt Real-time Chart Widget
# ============================================================

class RealtimeChart(QWidget):
    """实时折线图，纯 Qt 绘制，无 matplotlib"""

    def __init__(self, title, ylabel, color='#64B5F6', unit='', max_points=50, parent=None):
        super().__init__(parent)
        self.title = title
        self.ylabel = ylabel
        self.line_color = QtGui.QColor(color)
        self.max_points = max_points
        self.y_data = collections.deque(maxlen=max_points)
        self.y_min = 0.0
        self.y_max = 100.0
        self._painting = False
        self.setMinimumSize(300, 130)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self._font_title = QtGui.QFont('Microsoft YaHei', 9, QtGui.QFont.Bold)
        self._font_yl = QtGui.QFont('Microsoft YaHei', 8)
        self._font_lbl = QtGui.QFont('Microsoft YaHei', 7)
        self._pen_line = QtGui.QPen(self.line_color, 2)
        self._pen_grid = QtGui.QPen(QtGui.QColor(215, 204, 200))
        self._pen_grid.setDashPattern([2, 2])
        self._pen_border = QtGui.QPen(QtGui.QColor(215, 204, 200))

        self._pending_value = None
        self._update_timer = QTimer()
        self._update_timer.timeout.connect(self._flush_update)
        self._update_timer.setSingleShot(True)

    def _flush_update(self):
        if self._pending_value is not None:
            value = self._pending_value
            self._pending_value = None
            self.y_data.append(value)
            if len(self.y_data) >= 2:
                mn = min(self.y_data)
                mx = max(self.y_data)
                margin = (mx - mn) * 0.25 if mx > mn else 10.0
                self.y_min = mn - margin
                self.y_max = mx + margin
            self.update()

    def update_plot(self, value):
        self._pending_value = value
        if not self._update_timer.isActive():
            self._update_timer.start(100)

    def paintEvent(self, event):
        if self._painting:
            return
        self._painting = True
        try:
            self._do_paint()
        finally:
            self._painting = False

    def _do_paint(self):
        p = QtGui.QPainter(self)
        w = self.width()
        h = self.height()
        pad_l = 46
        pad_t = 30
        pad_r = 12
        chart_w = w - pad_l - pad_r
        chart_h = h - pad_t - 24

        p.fillRect(0, 0, w, h, QtGui.QColor(255, 253, 245))

        p.setFont(self._font_title)
        p.setPen(QtGui.QPen(QtGui.QColor(93, 64, 55)))
        p.drawText(pad_l, 18, self.title)

        if self.ylabel:
            p.setFont(self._font_yl)
            p.setPen(QtGui.QPen(QtGui.QColor(161, 136, 127)))
            p.drawText(4, h // 2, self.ylabel)

        p.fillRect(pad_l, pad_t, chart_w, chart_h, QtGui.QColor(255, 254, 248))

        p.setPen(self._pen_grid)
        for i in range(5):
            y = pad_t + chart_h * i // 4
            p.drawLine(pad_l, y, pad_l + chart_w, y)
        for i in range(6):
            x = pad_l + chart_w * i // 5
            p.drawLine(x, pad_t, x, pad_t + chart_h)

        p.setPen(self._pen_border)
        p.drawRect(pad_l, pad_t, chart_w, chart_h)

        n = len(self.y_data)
        if n < 2:
            p.setFont(self._font_lbl)
            p.setPen(QtGui.QPen(QtGui.QColor(161, 136, 127)))
            p.drawText(pad_l - 42, pad_t + chart_h + 4, f'{self.y_min:.1f}')
            p.drawText(pad_l - 42, pad_t + 4, f'{self.y_max:.1f}')
            return

        rng = self.y_max - self.y_min
        if rng <= 0:
            rng = 1.0

        p.setPen(self._pen_line)
        prev_x = None
        prev_y = None
        for j in range(n):
            xi = pad_l + int((j / (self.max_points - 1)) * chart_w)
            frac = (self.y_data[j] - self.y_min) / rng
            yi = pad_t + chart_h - int(frac * chart_h)
            if prev_x is not None:
                p.drawLine(prev_x, prev_y, xi, yi)
            prev_x = xi
            prev_y = yi

        p.setBrush(QtGui.QBrush(self.line_color))
        p.setPen(self._pen_line)
        for j in range(n):
            if j % 5 == 0 or j == n - 1:
                xi = pad_l + int((j / (self.max_points - 1)) * chart_w)
                frac = (self.y_data[j] - self.y_min) / rng
                yi = pad_t + chart_h - int(frac * chart_h)
                p.drawEllipse(QtCore.QPoint(xi, yi), 3, 3)

        p.setFont(self._font_lbl)
        p.setPen(QtGui.QPen(QtGui.QColor(161, 136, 127)))
        p.drawText(pad_l - 42, pad_t + chart_h + 4, f'{self.y_min:.1f}')
        p.drawText(pad_l - 42, pad_t + 4, f'{self.y_max:.1f}')
        p.drawText(pad_l - 42, pad_t + chart_h // 2 + 4, f'{(self.y_min + self.y_max) / 2:.1f}')


# ============================================================
# Circular Status Indicator
# ============================================================

class CircularIndicator(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._state = 'normal'
        self._pulse_angle = 0
        self.setFixedSize(90, 90)
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._pulse)
        self._timer.start(80)

        self._r = 67
        self._g = 160
        self._b = 71
        self._glow_alpha = 30
        self._a = 30

        self._gradient_brush = QtGui.QLinearGradient(
            QtCore.QPointF(-32.0, -32.0), QtCore.QPointF(32.0, 32.0))
        self._icon_font = QtGui.QFont('Segoe UI Symbol', 26, QtGui.QFont.Bold)
        self._label_font = QtGui.QFont('Microsoft YaHei', 10, QtGui.QFont.Bold)
        self._painting = False

    def _pulse(self):
        self._pulse_angle = (self._pulse_angle + 16) % 360
        self._a = int(30 + 20 * math.sin(math.radians(self._pulse_angle)))
        self.update()

    def set_state(self, state):
        if self._state != state:
            self._state = state
            if state == 'critical':
                self._r, self._g, self._b = 224, 67, 54
                self._a = 80
            elif state == 'warning':
                self._r, self._g, self._b = 251, 140, 0
                self._a = 60
            else:
                self._r, self._g, self._b = 67, 160, 71
                self._a = 30
            self.update()

    def paintEvent(self, event):
        if self._painting:
            return
        self._painting = True
        try:
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)

            cx = self.width() // 2
            cy = self.height() // 2
            outer_r = 42
            inner_r = 32

            r, g, b, a = self._r, self._g, self._b, self._a

            glow = QtGui.QColor(r, g, b, a)
            if self._state == 'normal':
                bg = QtGui.QColor(232, 245, 233)
            elif self._state == 'warning':
                bg = QtGui.QColor(255, 243, 224)
            else:
                bg = QtGui.QColor(255, 235, 238)
            icon_bg = QtGui.QColor(r, g, b)

            painter.setPen(Qt.NoPen)
            painter.setBrush(glow)
            painter.drawEllipse(cx - outer_r - 4, cy - outer_r - 4,
                                (outer_r + 4) * 2, (outer_r + 4) * 2)

            painter.setBrush(bg)
            painter.drawEllipse(cx - outer_r, cy - outer_r, outer_r * 2, outer_r * 2)

            self._gradient_brush.setColorAt(0, QtGui.QColor(min(r + 40, 255), min(g + 40, 255), min(b + 40, 255)))
            self._gradient_brush.setColorAt(1, icon_bg)
            painter.setBrush(self._gradient_brush)
            painter.drawEllipse(cx - inner_r, cy - inner_r, inner_r * 2, inner_r * 2)

            icon_map = {'critical': '!', 'warning': '~', 'normal': '\u2713'}
            icon = icon_map.get(self._state, '\u2713')
            painter.setFont(self._icon_font)
            painter.setPen(QtGui.QPen(QtGui.QColor(255, 255, 255)))
            painter.drawText(self.rect(), Qt.AlignCenter, icon)

            label_map = {'critical': '\u7d27\u6025', 'warning': '\u6ce8\u610f', 'normal': '\u6b63\u5e38'}
            label_text = label_map.get(self._state, '')
            fm2 = QtGui.QFontMetrics(self._label_font)
            lw = fm2.horizontalAdvance(label_text)
            lx = cx - lw // 2
            ly = cy + outer_r + 14
            painter.setPen(Qt.NoPen)
            painter.setBrush(icon_bg)
            rw = lw + 16
            rh = 18
            painter.drawRoundedRect(lx - 8, ly - 10, rw, rh, 6, 6)
            painter.setPen(QtGui.QPen(QtGui.QColor(255, 255, 255)))
            painter.setFont(self._label_font)
            painter.drawText(lx, ly, label_text)
        finally:
            self._painting = False


# ============================================================
# Metric Card
# ============================================================

class MetricCard(QWidget):
    def __init__(self, title, unit, icon, min_val, max_val, warn_low, warn_high,
                 crit_low, crit_high, parent=None):
        super().__init__(parent)
        self.title = title
        self.unit = unit
        self.icon = icon
        self.min_val = min_val
        self.max_val = max_val
        self.warn_low = warn_low
        self.warn_high = warn_high
        self.crit_low = crit_low
        self.crit_high = crit_high
        self.current_value = None
        self.setFixedHeight(150)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 12)
        layout.setSpacing(4)

        header = QHBoxLayout()
        icon_lbl = QLabel(self.icon)
        icon_lbl.setFont(QFont('Segoe UI Emoji', 20))
        title_lbl = QLabel(self.title)
        title_lbl.setFont(QFont('Microsoft YaHei', 12, QFont.Bold))
        title_lbl.setStyleSheet("color: #5D4037;")
        header.addWidget(icon_lbl)
        header.addWidget(title_lbl)
        header.addStretch()
        layout.addLayout(header)

        self.value_lbl = QLabel('--')
        self.value_lbl.setFont(QFont('Microsoft YaHei', 38, QFont.Bold))
        self.value_lbl.setStyleSheet("color: #4E342E;")
        self.value_lbl.setFixedHeight(56)
        layout.addWidget(self.value_lbl)

        unit_row = QHBoxLayout()
        self.unit_lbl = QLabel(self.unit)
        self.unit_lbl.setFont(QFont('Microsoft YaHei', 10))
        self.unit_lbl.setStyleSheet("color: #A1887F;")
        unit_row.addWidget(self.unit_lbl)
        unit_row.addStretch()
        self.status_lbl = QLabel('')
        self.status_lbl.setFont(QFont('Microsoft YaHei', 9, QFont.Bold))
        self.status_lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        unit_row.addWidget(self.status_lbl)
        layout.addLayout(unit_row)

        self._apply_style('#FFFDF5', '#D7CCC8', '#4E342E')

    def _apply_style(self, bg, border, value_color):
        self.setObjectName('MetricCard')
        self.setStyleSheet(f"""
            #MetricCard {{
                background-color: {bg};
                border-radius: 18px;
                border: 1.5px solid {border};
            }}
        """)

    def update_value(self, value):
        self.current_value = value
        self.value_lbl.setText(f'{value:.1f}')

        if value >= self.crit_high or value <= self.crit_low:
            color = '#C62828'; bg = '#FFEBEE'; status_text = '\u5371\u9669'; border = '#EF9A9A'
        elif value >= self.warn_high or value <= self.warn_low:
            color = '#E65100'; bg = '#FFF3E0'; status_text = '\u6ce8\u610f'; border = '#FFCC80'
        else:
            color = '#2E7D32'; bg = '#E8F5E9'; status_text = '\u6b63\u5e38'; border = '#A5D6A7'

        self.value_lbl.setStyleSheet(f"color: {color};")
        self.status_lbl.setText(f'  {status_text}  ')
        self.status_lbl.setStyleSheet(
            f"color: {color}; background: {bg}; border-radius: 8px; padding: 3px 12px;"
        )
        self._apply_style(bg, border, color)


# ============================================================
# Position Card
# ============================================================

class PositionCard(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_pos = 0
        self.setFixedHeight(150)
        self.init_ui()

    def init_ui(self):
        self.setObjectName('PosCard')
        self.setStyleSheet("""
            #PosCard {
                background-color: #FFFDF5;
                border-radius: 18px;
                border: 1.5px solid #D7CCC8;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 12)
        layout.setSpacing(4)

        header = QHBoxLayout()
        icon_lbl = QLabel('\U0001f6cb')
        icon_lbl.setFont(QFont('Segoe UI Emoji', 20))
        title_lbl = QLabel('\u4f53\u4f4d')
        title_lbl.setFont(QFont('Microsoft YaHei', 12, QFont.Bold))
        title_lbl.setStyleSheet("color: #5D4037;")
        header.addWidget(icon_lbl)
        header.addWidget(title_lbl)
        header.addStretch()
        layout.addLayout(header)

        self.pos_lbl = QLabel('\u4ef0\u5367')
        self.pos_lbl.setFont(QFont('Microsoft YaHei', 30, QFont.Bold))
        self.pos_lbl.setStyleSheet("color: #2E7D32;")
        self.pos_lbl.setFixedHeight(56)
        layout.addWidget(self.pos_lbl)

        self.status_lbl = QLabel('  \u6b63\u5e38  ')
        self.status_lbl.setFont(QFont('Microsoft YaHei', 9, QFont.Bold))
        self.status_lbl.setStyleSheet(
            "color: #2E7D32; background: #C8E6C9; border-radius: 8px; padding: 3px 12px;"
        )
        layout.addWidget(self.status_lbl)

    def update_position(self, value):
        self.current_pos = int(value)
        pos_map = {
            0: ('\u4ef0\u5367', '\u6b63\u5e38', '#2E7D32', '#C8E6C9', '#A5D6A7', '#E8F5E9'),
            1: ('\u4fef\u5367 \u26a0', '\u6ce8\u610f\uff01', '#BF360C', '#FFCDD2', '#EF9A9A', '#FFEBEE'),
            2: ('\u4fa7\u5367', '\u6b63\u5e38', '#2E7D32', '#C8E6C9', '#A5D6A7', '#E8F5E9'),
            3: ('\u7ad9\u7acb', '\u6b63\u5e38', '#2E7D32', '#C8E6C9', '#A5D6A7', '#E8F5E9'),
        }
        text, status, txt_color, status_bg, border, bg = pos_map.get(
            self.current_pos, ('\u672a\u77e5', '\u672a\u77e5', '#9E9E9E', '#EEEEEE', '#BDBDBD', '#FAFAFA')
        )
        self.pos_lbl.setText(text)
        self.pos_lbl.setStyleSheet(f"color: {txt_color};")
        self.status_lbl.setText(f'  {status}  ')
        self.status_lbl.setStyleSheet(
            f"color: {txt_color}; background: {status_bg}; border-radius: 8px; padding: 3px 12px;"
        )
        self.setStyleSheet(f"""
            #PosCard {{
                background-color: {bg};
                border-radius: 18px;
                border: 2px solid {border};
            }}
        """)


# ============================================================
# Collapsible Panel
# ============================================================

class CollapsiblePanel(QWidget):
    toggled = pyqtSignal(bool)

    def __init__(self, title, icon, parent=None):
        super().__init__(parent)
        self._collapsed = True
        self.init_ui(title, icon)

    def init_ui(self, title, icon):
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.header = QWidget()
        self.header.setFixedHeight(52)
        self.header.setCursor(Qt.PointingHandCursor)
        self.header.setStyleSheet("""
            QWidget {
                background-color: #FFFFFF;
                border-radius: 14px;
                border: 1.5px solid #D7CCC8;
            }
            QWidget:hover { background-color: #FFF8F0; }
        """)

        header_layout = QHBoxLayout(self.header)
        header_layout.setContentsMargins(16, 0, 16, 0)

        self.icon_lbl = QLabel(icon)
        self.icon_lbl.setFont(QFont('Segoe UI Emoji', 14))

        self.title_lbl = QLabel(title)
        self.title_lbl.setFont(QFont('Microsoft YaHei', 12, QFont.Bold))
        self.title_lbl.setStyleSheet("color: #5D4037;")

        self.badge = QLabel('')
        self.badge.setFont(QFont('Microsoft YaHei', 9, QFont.Bold))
        self.badge.setAlignment(Qt.AlignCenter)
        self.badge.setFixedSize(24, 24)
        self.badge.setStyleSheet(
            "background-color: #FFEBEE; color: #C62828; border-radius: 12px;"
        )

        self.arrow = QLabel('\u25b6')
        self.arrow.setFont(QFont('Microsoft YaHei', 9))
        self.arrow.setStyleSheet("color: #A1887F;")

        header_layout.addWidget(self.icon_lbl)
        header_layout.addWidget(self.title_lbl)
        header_layout.addWidget(self.badge)
        header_layout.addStretch()
        header_layout.addWidget(self.arrow)

        self.header.mousePressEvent = self._toggle
        main_layout.addWidget(self.header)

        self.content_area = QWidget()
        content_layout = QVBoxLayout(self.content_area)
        content_layout.setContentsMargins(10, 10, 10, 10)
        content_layout.setSpacing(6)
        self.content_area.setVisible(False)
        main_layout.addWidget(self.content_area)

    def _toggle(self, event=None):
        self._collapsed = not self._collapsed
        self.content_area.setVisible(not self._collapsed)
        self.arrow.setText('\u25bc' if not self._collapsed else '\u25b6')
        self.toggled.emit(not self._collapsed)

    def set_badge(self, count):
        if count > 0:
            self.badge.setText(str(count))
            self.badge.setStyleSheet(
                "background-color: #C62828; color: white; border-radius: 12px;"
            )
        else:
            self.badge.setText('')
            self.badge.setStyleSheet(
                "background-color: #FFEBEE; color: #C62828; border-radius: 12px;"
            )

    def get_content_layout(self):
        return self.content_area.layout()


# ============================================================
# Alert Card
# ============================================================

class AlertCard(QWidget):
    def __init__(self, alert_data, parent=None):
        super().__init__(parent)
        self.alert_data = alert_data
        self._expanded = False
        self.main_layout = None
        self.detail_wgt = None
        self.msg_lbl = None
        self.init_ui()

    def init_ui(self):
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.setMinimumHeight(60)

        level = self.alert_data.get('level', 'info')
        color_map = {
            'critical': ('#C62828', '#FFEBEE', '#EF9A9A', '\u7d27\u6025'),
            'warning': ('#E65100', '#FFF3E0', '#FFCC80', '\u63d0\u9192'),
            'info': ('#1565C0', '#E3F2FD', '#90CAF9', '\u63d0\u793a'),
        }
        border_color, bg_color, light_color, level_text = color_map.get(
            level, color_map['info'])

        self.setStyleSheet(f"""
            QWidget {{
                background-color: {bg_color};
                border-radius: 12px;
                border: 1px solid {light_color};
            }}
        """)

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(14, 10, 14, 10)
        self.main_layout.setSpacing(3)

        row = QHBoxLayout()
        row.setSpacing(8)

        dot = QLabel('\u25cf')
        dot.setFont(QFont('Segoe UI Symbol', 11))
        dot.setStyleSheet(f"color: {border_color};")
        dot.setFixedWidth(18)

        level_lbl = QLabel(f'[{level_text}]')
        level_lbl.setFont(QFont('Microsoft YaHei', 10, QFont.Bold))
        level_lbl.setStyleSheet(f"color: {border_color};")

        time_str = self.alert_data.get('timestamp', '')
        if 'T' in time_str:
            time_str = time_str.replace('T', ' ')[:16]
        time_lbl = QLabel(time_str)
        time_lbl.setFont(QFont('Microsoft YaHei', 8))
        time_lbl.setStyleSheet("color: #A1887F;")
        time_lbl.setAlignment(Qt.AlignRight)

        row.addWidget(dot)
        row.addWidget(level_lbl)
        row.addWidget(time_lbl)
        self.main_layout.addLayout(row)

        message = self.alert_data.get('message', '')
        self.msg_lbl = QLabel(message)
        self.msg_lbl.setFont(QFont('Microsoft YaHei', 10))
        self.msg_lbl.setStyleSheet("color: #4E342E;")
        self.msg_lbl.setWordWrap(True)
        self.main_layout.addWidget(self.msg_lbl)

        self.detail_wgt = QWidget()
        detail_layout = QVBoxLayout(self.detail_wgt)
        detail_layout.setContentsMargins(26, 4, 4, 4)
        detail_layout.setSpacing(4)

        sensor = self.alert_data.get('sensor_type', '')
        value = self.alert_data.get('value', '')
        if sensor:
            sensor_lbl = QLabel(f'\u4f53\u6e29: {sensor}')
            sensor_lbl.setFont(QFont('Microsoft YaHei', 9))
            sensor_lbl.setStyleSheet("color: #795548;")
            detail_layout.addWidget(sensor_lbl)
        if value:
            val_lbl = QLabel(f'\u6570\u503c: {value}')
            val_lbl.setFont(QFont('Microsoft YaHei', 9))
            val_lbl.setStyleSheet("color: #795548;")
            detail_layout.addWidget(val_lbl)

        self.detail_wgt.setVisible(False)
        self.main_layout.addWidget(self.detail_wgt)

        self.setCursor(Qt.PointingHandCursor)

    def mousePressEvent(self, event):
        self._expanded = not self._expanded
        self.detail_wgt.setVisible(self._expanded)


# ============================================================
# Alert History Dialog
# ============================================================

class AlertHistoryDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('\u5386\u53f2\u544a\u8b66\u8bb0\u5f55')
        self.setMinimumSize(500, 400)
        self.setStyleSheet("""
            QDialog { background-color: #FFF8F0; }
            QLabel { background: transparent; }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(12)

        title = QLabel('\u5386\u53f2\u544a\u8b66\u8bb0\u5f55')
        title.setFont(QFont('Microsoft YaHei', 16, QFont.Bold))
        title.setStyleSheet("color: #5D4037;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("""
            QScrollArea { background: transparent; border: none; }
            QScrollBar:vertical { background: #EFEBE9; width: 6px; border-radius: 3px; }
            QScrollBar::handle:vertical { background: #A1887F; border-radius: 3px; }
        """)
        self.container = QWidget()
        self.container_layout = QVBoxLayout(self.container)
        self.container_layout.setSpacing(8)
        self.container_layout.addStretch()
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll, 1)

        close_btn = QPushButton('\u5173\u95ed')
        close_btn.setFont(QFont('Microsoft YaHei', 11))
        close_btn.setFixedSize(100, 36)
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #8D6E63;
                color: white;
                border-radius: 8px;
                border: none;
            }
            QPushButton:hover { background-color: #6D4C41; }
        """)
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, 0, Qt.AlignRight)

    def add_alerts(self, alerts):
        for i in range(self.container_layout.count() - 1, -1, -1):
            w = self.container_layout.itemAt(i)
            if w and w.widget():
                w.widget().deleteLater()

        for alert in reversed(alerts):
            card = AlertCard(alert)
            self.container_layout.insertWidget(0, card)


# ============================================================
# Main Window
# ============================================================

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.temp_buffer = collections.deque(maxlen=60)
        self.hr_buffer = collections.deque(maxlen=60)
        self.crying_buffer = collections.deque(maxlen=60)

        self.alert_history = []
        self.alert_count = 0

        self._last_critical_sound = 0
        self._last_warning_sound = 0

        # 系统级告警状态，用于检测全局状态转换
        # 0=normal, 1=warning, 2=critical
        # 仅在 normal -> warning/critical 时记录一次告警
        self._prev_danger_level = 0

        self.setWindowTitle('\u5b9d\u5b9d\u667a\u80fd\u76d1\u62a4\u7cfb\u7edf')
        self.setMinimumSize(1200, 780)
        self.resize(1280, 820)
        self.move(80, 40)

        self._build_ui()

        self.clock_timer = QTimer()
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)
        self._update_clock()

        self.start_data_collector()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(24, 20, 24, 16)
        main_layout.setSpacing(14)

        self._build_header(main_layout)
        self._build_status_banner(main_layout)
        self._build_metric_cards(main_layout)
        self._build_charts_area(main_layout)
        self._build_bottom_panels(main_layout)

        self.setStyleSheet("""
            QMainWindow { background-color: #FFF8F0; }
            QLabel { background: transparent; }
            QScrollArea { background: transparent; border: none; }
            QScrollArea > QWidget > QWidget { background: transparent; }
            QScrollBar:vertical {
                background: #EFEBE9;
                width: 6px;
                border-radius: 3px;
            }
            QScrollBar::handle:vertical {
                background: #A1887F;
                border-radius: 3px;
            }
        """)

    def _build_header(self, parent):
        header = QWidget()
        header.setFixedHeight(50)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(4, 0, 4, 0)

        baby_icon = QLabel('\U0001f476')
        baby_icon.setFont(QFont('Segoe UI Emoji', 22))
        baby_icon.setFixedWidth(40)

        title = QLabel('\u5b9d\u5b9d\u667a\u80fd\u76d1\u62a4\u7cfb\u7edf')
        title.setFont(QFont('Microsoft YaHei', 16, QFont.Bold))
        title.setStyleSheet("color: #5D4037;")

        hl.addWidget(baby_icon)
        hl.addWidget(title)
        hl.addStretch()

        self.time_lbl = QLabel('')
        self.time_lbl.setFont(QFont('Microsoft YaHei', 11))
        self.time_lbl.setStyleSheet("color: #8D6E63;")

        hl.addWidget(self.time_lbl)
        parent.addWidget(header)

    def _build_status_banner(self, parent):
        banner = QWidget()
        banner.setFixedHeight(120)
        banner_layout = QHBoxLayout(banner)
        banner_layout.setContentsMargins(24, 0, 24, 0)
        banner_layout.setSpacing(20)

        self.indicator = CircularIndicator()
        self.indicator.set_state('normal')

        status_vl = QVBoxLayout()
        self.status_main_lbl = QLabel('\u5b9d\u5b9d\u72b6\u6001\u826f\u597d')
        self.status_main_lbl.setFont(QFont('Microsoft YaHei', 20, QFont.Bold))
        self.status_main_lbl.setStyleSheet("color: #FFFFFF;")
        self.status_sub_lbl = QLabel('\u5b9d\u5b9d\u751f\u547d\u4f53\u5f81\u5065\u5eb7\uff0c\u65e0\u9700\u5173\u6ce8')
        self.status_sub_lbl.setFont(QFont('Microsoft YaHei', 12))
        self.status_sub_lbl.setStyleSheet("color: rgba(255,255,255,0.85);")
        status_vl.addWidget(self.status_main_lbl)
        status_vl.addWidget(self.status_sub_lbl)
        status_vl.setSpacing(4)

        banner_layout.addWidget(self.indicator)
        banner_layout.addLayout(status_vl)
        banner_layout.addStretch()

        self.alert_btn = QPushButton('\u67e5\u770b\u5386\u53f2\u544a\u8b66 (0)')
        self.alert_btn.setFont(QFont('Microsoft YaHei', 11, QFont.Bold))
        self.alert_btn.setFixedSize(180, 42)
        self.alert_btn.setCursor(Qt.PointingHandCursor)
        self.alert_btn.clicked.connect(self._show_alert_history)
        banner_layout.addWidget(self.alert_btn)

        parent.addWidget(banner)
        self.banner = banner
        self._set_banner_normal()

    def _set_banner_normal(self):
        self.banner.setStyleSheet("""
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #81C784, stop:1 #66BB6A);
            border-radius: 20px;
        """)
        self.indicator.set_state('normal')
        self.status_main_lbl.setText('\u5b9d\u5b9d\u72b6\u6001\u826f\u597d')
        self.status_main_lbl.setStyleSheet("color: #FFFFFF; font-weight: bold;")
        self.status_sub_lbl.setText('\u5b9d\u5b9d\u751f\u547d\u4f53\u5f81\u5065\u5eb7\uff0c\u65e0\u9700\u5173\u6ce8')
        self.status_sub_lbl.setStyleSheet("color: rgba(255,255,255,0.85);")

    def _set_banner_warning(self):
        self.banner.setStyleSheet("""
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #FFB74D, stop:1 #FFA726);
            border-radius: 20px;
        """)
        self.indicator.set_state('warning')
        self.status_main_lbl.setText('\u6ce8\u610f\uff1a\u9700\u8981\u5173\u6ce8')
        self.status_main_lbl.setStyleSheet("color: #FFFFFF; font-weight: bold;")
        self.status_sub_lbl.setText('\u68c0\u6d4b\u5230\u5f02\u5e38\uff0c\u8bf7\u67e5\u770b\u8b66\u544a\u8bb0\u5f55')

    def _set_banner_danger(self):
        self.banner.setStyleSheet("""
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #EF5350, stop:1 #E53935);
            border-radius: 20px;
        """)
        self.indicator.set_state('critical')
        self.status_main_lbl.setText('\u7d27\u6025\uff01\u8bf7\u7acb\u5373\u5904\u7406')
        self.status_main_lbl.setStyleSheet("color: #FFFFFF; font-weight: bold;")
        self.status_sub_lbl.setText('\u68c0\u6d4b\u5230\u5371\u9669\u4fe1\u53f7\uff0c\u8bf7\u7acb\u5373\u5173\u6ce8\u5b9d\u5b9d')

    def _build_metric_cards(self, parent):
        cards = QHBoxLayout()
        cards.setSpacing(14)

        self.temp_card = MetricCard(
            '\u4f53\u6e29', '\u2103', '\U0001f912',
            min_val=35.0, max_val=40.0,
            warn_low=35.8, warn_high=37.4,
            crit_low=35.5, crit_high=37.8
        )
        self.hr_card = MetricCard(
            '\u5fc3\u7387', '\u6b21/\u5206', '\U0001f49a',
            min_val=50, max_val=200,
            warn_low=80, warn_high=160,
            crit_low=70, crit_high=170
        )
        self.crying_card = MetricCard(
            '\u54ed\u95f9', '\u7ea7', '\U0001f622',
            min_val=0, max_val=100,
            warn_low=0, warn_high=30,
            crit_low=0, crit_high=50
        )
        self.pos_card = PositionCard()

        cards.addWidget(self.temp_card, 1)
        cards.addWidget(self.hr_card, 1)
        cards.addWidget(self.crying_card, 1)
        cards.addWidget(self.pos_card, 1)

        parent.addLayout(cards)

    def _build_charts_area(self, parent):
        charts = QWidget()
        charts_layout = QHBoxLayout(charts)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        charts_layout.setSpacing(14)

        self.temp_chart = RealtimeChart(
            '\u4f53\u6e29\u8d8b\u52bf (\u2103)', '\u2103', '#EF5350', max_points=40)
        self.hr_chart = RealtimeChart(
            '\u5fc3\u7387\u8d8b\u52bf (\u6b21/\u5206)', 'bpm', '#EC407A', max_points=40)
        self.crying_chart = RealtimeChart(
            '\u54ed\u95f9\u7a0b\u5ea6', 'level', '#FFA726', max_points=40)

        charts_layout.addWidget(self.temp_chart, 1)
        charts_layout.addWidget(self.hr_chart, 1)
        charts_layout.addWidget(self.crying_chart, 1)

        parent.addWidget(charts, 1)

    def _build_bottom_panels(self, parent):
        panels = QHBoxLayout()
        panels.setSpacing(14)

        self.alert_panel = CollapsiblePanel('\u544a\u8b66\u8bb0\u5f55', '\U0001f514')
        self.alert_scroll = QScrollArea()
        self.alert_scroll.setWidgetResizable(True)
        self.alert_scroll.setMinimumHeight(130)
        self.alert_scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
        self.alert_container = QWidget()
        self.alert_container_layout = QVBoxLayout(self.alert_container)
        self.alert_container_layout.setSpacing(6)
        self.alert_container_layout.addStretch()
        self.alert_scroll.setWidget(self.alert_container)

        self.no_alert_lbl = QLabel('\u6682\u65e0\u544a\u8b66\u8bb0\u5f55\uff0c\u5b9d\u5b9d\u72b6\u6001\u826f\u597d~')
        self.no_alert_lbl.setFont(QFont('Microsoft YaHei', 10))
        self.no_alert_lbl.setAlignment(Qt.AlignCenter)
        self.no_alert_lbl.setStyleSheet("color: #A1887F; padding: 20px;")
        self.alert_container_layout.insertWidget(0, self.no_alert_lbl)

        self.alert_panel.get_content_layout().addWidget(self.alert_scroll)
        panels.addWidget(self.alert_panel, 1)

        self.suggestion_panel = CollapsiblePanel('\u667a\u80fd\u7167\u62a4\u5efa\u8bae', '\U0001f4a1')
        self.suggestion_scroll = QScrollArea()
        self.suggestion_scroll.setWidgetResizable(True)
        self.suggestion_scroll.setMinimumHeight(130)
        self.suggestion_scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
        sug_container = QWidget()
        sug_layout = QVBoxLayout(sug_container)
        sug_layout.setContentsMargins(0, 0, 0, 0)

        self.suggestion_lbl = QLabel('\u6b63\u5728\u5206\u6790\u5b9d\u5b9d\u6570\u636e\uff0c\u8bf7\u7a0d\u5019...')
        self.suggestion_lbl.setFont(QFont('Microsoft YaHei', 11))
        self.suggestion_lbl.setStyleSheet("color: #6D4C41; line-height: 190%;")
        self.suggestion_lbl.setWordWrap(True)
        self.suggestion_lbl.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        sug_layout.addWidget(self.suggestion_lbl)
        self.suggestion_scroll.setWidget(sug_container)

        self.suggestion_panel.get_content_layout().addWidget(self.suggestion_scroll)
        panels.addWidget(self.suggestion_panel, 1)

        parent.addLayout(panels)

    def _update_clock(self):
        from datetime import datetime
        self.time_lbl.setText(datetime.now().strftime('%Y\u5e74%m\u6708%d\u65e5 %H:%M:%S'))

    def _show_alert_history(self):
        dialog = AlertHistoryDialog(self)
        dialog.add_alerts(self.alert_history)
        dialog.exec_()

    def _on_sound_signal(self, level):
        if level == 'critical':
            now = time.time()
            if now - self._last_critical_sound < 8:
                return
            self._last_critical_sound = now
            self._play_alarm()
        elif level == 'warning':
            now = time.time()
            if now - self._last_warning_sound < 15:
                return
            self._last_warning_sound = now
            self._play_beep()

    def _play_alarm(self):
        try:
            import winsound
            for freq, dur in [(880, 300), (440, 200), (880, 300), (440, 200), (880, 400)]:
                winsound.Beep(freq, dur)
        except Exception:
            pass

    def _play_beep(self):
        try:
            import winsound
            winsound.Beep(660, 200)
        except Exception:
            pass

    def start_data_collector(self):
        self.collector = DataCollector()
        self.collector.data_ready.connect(self.on_data_update)
        self.collector.start()

    def on_data_update(self, records):
        if not records:
            return

        danger_level = 0
        for record in records:
            stype = record.get('sensor_type')
            value = record.get('value', 0)
            alerts = record.get('alerts', [])

            if alerts:
                max_sev = max((a.get('level', 'info') for a in alerts), default='info')
                if max_sev == 'critical':
                    danger_level = 2
                elif max_sev == 'warning' and danger_level < 2:
                    danger_level = 1

            try:
                if stype == 'temperature':
                    self.temp_buffer.append(value)
                    self.temp_chart.update_plot(value)
                    self.temp_card.update_value(value)
                elif stype == 'heart_rate':
                    self.hr_buffer.append(value)
                    self.hr_chart.update_plot(value)
                    self.hr_card.update_value(value)
                elif stype == 'crying':
                    self.crying_buffer.append(value * 100)
                    self.crying_chart.update_plot(value * 100)
                    self.crying_card.update_value(value * 100)
                elif stype == 'body_position':
                    self.pos_card.update_position(value)
            except Exception:
                pass

        # 仅在 normal -> warning/critical 状态转换时记录告警
        if danger_level > 0 and self._prev_danger_level == 0:
            # 收集本轮所有告警（每传感器取最高级别的一条）
            seen = set()
            alerts_to_record = []
            for record in records:
                for a in record.get('alerts', []):
                    key = (record.get('sensor_type'), a.get('level'))
                    if key not in seen:
                        seen.add(key)
                        alerts_to_record.append(a)
            if alerts_to_record:
                self._process_alerts(alerts_to_record)

        self._prev_danger_level = danger_level

        if danger_level == 2:
            self._set_banner_danger()
        elif danger_level == 1:
            self._set_banner_warning()
        else:
            self._set_banner_normal()

        self._fetch_suggestion()

    def _process_alerts(self, alerts):
        self.no_alert_lbl.hide()
        for alert in reversed(alerts):
            card = AlertCard(alert)
            self.alert_container_layout.insertWidget(0, card)
            self.alert_history.append(alert)
        self.alert_count += len(alerts)
        self.alert_btn.setText(f'\u67e5\u770b\u5386\u53f2\u544a\u8b66 ({self.alert_count})')
        self.alert_panel.set_badge(self.alert_count)
        while self.alert_container_layout.count() > 30:
            item = self.alert_container_layout.takeAt(
                self.alert_container_layout.count() - 2)
            if item and item.widget():
                item.widget().deleteLater()

        has_critical = any(a.get('level') == 'critical' for a in alerts)
        if has_critical:
            self._on_sound_signal('critical')
        elif alerts:
            self._on_sound_signal('warning')

    def _fetch_suggestion(self):
        try:
            import requests
            r = requests.get('http://127.0.0.1:5000/api/analytics/suggestion', timeout=2)
            if r.status_code == 200:
                d = r.json()
                suggestion = d.get('suggestion', '\u6570\u636e\u6b63\u5e38\uff0c\u4fdd\u6301\u89c2\u5bdf')
                patterns = d.get('patterns_detected', [])
                lines = [suggestion]
                for p in patterns:
                    lines.append(f'\n[\u6a21\u5f0f] {p.get("suggestion", "")}')
                self.suggestion_lbl.setText(''.join(lines))
        except Exception:
            pass

    def on_connection_change(self, connected):
        if not connected:
            self.status_main_lbl.setText('\u8fde\u63a5\u4e2d\u65ad')
            self.status_main_lbl.setStyleSheet("color: #FFFFFF; font-weight: bold;")
            self.status_sub_lbl.setText('\u6b63\u5728\u5c1d\u8bd5\u91cd\u65b0\u8fde\u63a5...')

    def closeEvent(self, event):
        if hasattr(self, 'collector'):
            self.collector.stop()
        event.accept()


# ============================================================
# Entry Point
# ============================================================

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    app.setApplicationName('\u5b9d\u5b9d\u667a\u80fd\u76d1\u62a4\u7cfb\u7edf')

    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
