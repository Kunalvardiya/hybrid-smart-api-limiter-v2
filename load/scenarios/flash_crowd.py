"""Locust scenario: Legitimate flash crowd.  Details and expectations: load/definitions.py

    locust -f load/scenarios/flash_crowd.py --host http://127.0.0.1:8000 --headless `
           -u 35 -r 35 -t 180s --csv load/results/flash_crowd-r1 
    (set $env:SCN_RUN_ID="r1" first; use a NEW id for every run)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from load.common import build_user_classes  # noqa: E402

globals().update(build_user_classes("flash_crowd"))

