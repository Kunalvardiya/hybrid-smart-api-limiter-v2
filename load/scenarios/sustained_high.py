"""Locust scenario: Sustained high traffic.  Details and expectations: load/definitions.py

    locust -f load/scenarios/sustained_high.py --host http://127.0.0.1:8000 --headless `
           -u 3 -r 3 -t 170s --csv load/results/sustained_high-r1 
    (set $env:SCN_RUN_ID="r1" first; use a NEW id for every run)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from load.common import build_user_classes  # noqa: E402

globals().update(build_user_classes("sustained_high"))

