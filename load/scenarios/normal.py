"""Locust scenario: Normal client behaviour.  Details and expectations: load/definitions.py

    locust -f load/scenarios/normal.py --host http://127.0.0.1:8000 --headless `
           -u 5 -r 5 -t 150s --csv load/results/normal-r1 
    (set $env:SCN_RUN_ID="r1" first; use a NEW id for every run)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from load.common import build_user_classes  # noqa: E402

globals().update(build_user_classes("normal"))

