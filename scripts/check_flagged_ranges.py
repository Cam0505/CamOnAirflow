import json
from datetime import date


def parse_date(d):
    return date.fromisoformat(d)


with open("/workspaces/CamOnAirFlow/pipelines/.dlt/air_quality_pipeline/state.json") as f:
    state = json.load(f)


flagged = state["sources"]["openaq_source"]["open_aq"]["Flagged_Requests"]


def check_overlaps(ranges):
    # Convert to tuples of (start, end) as date objects and sort by start
    tuples = sorted([(parse_date(r["start"]), parse_date(r["end"])) for r in ranges], key=lambda x: x[0])
    overlaps = []
    for i in range(1, len(tuples)):
        prev_end = tuples[i-1][1]
        curr_start = tuples[i][0]
        if curr_start <= prev_end:
            overlaps.append((tuples[i-1], tuples[i]))
    return overlaps


for sensor_id, ranges in flagged.items():
    if not ranges:
        print(f"Sensor {sensor_id}: No flagged ranges")
        continue
    overlaps = check_overlaps(ranges)
    if overlaps:
        print(f"Sensor {sensor_id}: Overlapping ranges found:")
        for a, b in overlaps:
            print(f"  {a} overlaps with {b}")
    else:
        print(f"Sensor {sensor_id}: No overlapping ranges")