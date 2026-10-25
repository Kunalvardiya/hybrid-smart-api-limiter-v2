"""Print '<users> <seconds>' for a scenario (used by run_scenario.ps1): users to spawn, run time incl. margin."""
import sys

from load.definitions import SCENARIOS, time_scale

s = SCENARIOS[sys.argv[1]]
print(s.users, int(s.duration_s * time_scale() + 5))
