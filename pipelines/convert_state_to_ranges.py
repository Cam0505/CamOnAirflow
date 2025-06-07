import json
from datetime import datetime, timedelta

def dates_to_ranges(dates):
    """Convert a sorted list of YYYY-MM-DD strings to a list of contiguous ranges."""
    if not dates:
        return []
    dates = sorted(dates)
    ranges = []
    start = prev = datetime.fromisoformat(dates[0])
    for d in dates[1:]:
        curr = datetime.fromisoformat(d)
        if curr == prev + timedelta(days=1):
            prev = curr
        else:
            ranges.append({"start": start.date().isoformat(), "end": prev.date().isoformat()})
            start = prev = curr
    ranges.append({"start": start.date().isoformat(), "end": prev.date().isoformat()})
    return ranges

with open("/workspaces/CamOnAirFlow/pipelines/.dlt/air_quality_pipeline/state.json") as f:
    state = json.load(f)

flagged = state["sources"]["openaq_source"]["open_aq"]["Flagged_Requests"]
for sensor_id, date_list in flagged.items():
    # Only convert if it's a list of dates (not already ranges)
    if date_list and isinstance(date_list[0], str):
        flagged[sensor_id] = dates_to_ranges(date_list)

with open("/workspaces/CamOnAirFlow/pipelines/.dlt/air_quality_pipeline/state.json", "w") as f:
    json.dump(state, f, indent=2)

print("State file converted to use date ranges for Flagged_Requests.")