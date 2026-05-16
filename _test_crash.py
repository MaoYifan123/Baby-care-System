import sys, time as time_module, threading
sys.path.insert(0, r'D:\PyCharm\Vegetable-greenhouse')

print('[1/4] Edge node...')
from edge_node import EdgeNodeSimulator
edge_node = EdgeNodeSimulator()
edge_node.start(interval=2.0)
print('    OK')

print('[2/4] Cloud server + Flask...')
import cloud_server as cs

def run_server():
    cs.app.run(host='127.0.0.1', port=5010, debug=False, threaded=True, use_reloader=False)

flask_thread = threading.Thread(target=run_server, daemon=True)
flask_thread.start()
time_module.sleep(1.0)
cs.start_simulation()
print('    OK')

print('[3/4] QApplication...')
from PyQt5 import QtWidgets, QtCore
QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
qt_app = QtWidgets.QApplication(sys.argv)
qt_app.setStyle('Fusion')
print('    OK')

print('[4/4] MainWindow...')
from gui_monitor import MainWindow
window = MainWindow()
window.show()
print('    show() called')

print()
print('Starting Qt event loop...')
import requests
try:
    r = requests.get('http://127.0.0.1:5010/api/health', timeout=2)
    print(f'    Flask health: {r.status_code}')
except Exception as e:
    print(f'    Flask health failed: {e}')

time_module.sleep(5)
qt_app.quit()
print('SUCCESS - system ran for 5 seconds')
