import sys, time
sys.path.insert(0, r'D:\PyCharm\Vegetable-greenhouse')

print('Running main.py in-process...')
import main as m
import argparse

# Mock sys.argv
sys.argv = ['main.py', '--port', '5011']
args = m.parser.parse_args(sys.argv)

# Patch the full system to run for limited time
original_run = None

print('Starting full system...')
# Just run the full system - the cleanup should work
