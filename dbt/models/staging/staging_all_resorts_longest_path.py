"""
dbt Python model: Find the longest continuous downhill ski path per resort
for ALL resorts worldwide.

Uses the same segment-level directed graph as staging_northisland_paths /
staging_southisland_paths:
  - Nodes  : "{run_osm_id}_{segment_index}" — one node per OSM segment
  - Edges  : seg A -> seg B when A.to_node_id == B.from_node_id
             (naturally enforces downhill-only traversal; backtracking within
             the same run is blocked by the seg_idx guard)

This correctly handles run merges: if run B merges into run A at segment 5 of A,
you can only continue from segment 5 of A — not from the top of A.

Starting points: segment 0 of every run served by a lift top
(lift_services_run in base_lift_run_mapping), matching the NI/SI model logic.

A single DFS pass finds the LONGEST path by total distance (not all paths).
O(n) graph construction (from_node index dict) avoids the O(n²) nested loop
so that large resorts like Whistler are tractable.

A per-resort MAX_STATES cap prevents runaway computation on extremely dense
resort graphs.

Output: one row per resort — the single longest path found.
"""

import math
import pandas as pd
import json
from typing import Dict, Optional

OUTPUT_COLUMNS = [
    "resort",
    "starting_lift",
    "starting_lift_id",
    "run_count",
    "total_distance_m",
    "total_vertical_m",
    "run_path",
    "node_ids",
]

# DFS state cap per resort — prevents runaway on huge/dense resorts.
# At the cap the best path found so far is returned.
_MAX_STATES = 2_000_000

# Proximity fallback threshold — used for NZ resorts where OSM ways have
# gaps between run endpoints that don't share a node ID.
_NZ_PROXIMITY_THRESHOLD_M = 50.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate great-circle distance in metres using the equirectangular formula."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    lat_mid = math.radians((lat1 + lat2) / 2.0)
    return math.sqrt(dlat ** 2 + (math.cos(lat_mid) * dlon) ** 2) * 6_371_000


def build_segment_graph(
    ski_segments: pd.DataFrame,
    use_proximity_fallback: bool = False,
) -> Dict[str, Dict]:
    """
    Build a segment-level directed graph.

    Each node is "{run_osm_id}_{segment_index}".
    Edges connect A -> B when A.to_node_id == B.from_node_id, subject to:
      - B != A (no self-loops)
      - not (same run AND B.seg_idx <= A.seg_idx)  — no backward same-run travel

    Uses a from_node index for O(n) construction instead of O(n²).

    When use_proximity_fallback=True, any segment whose to_node matches no
    from_node exactly is also checked against all segment start-points within
    _NZ_PROXIMITY_THRESHOLD_M.  The elevation guard (candidate start elevation
    >= current end elevation) prevents uphill proximity connections.
    A spatial bucket grid (1° cells ≈ 111 km) keeps the fallback O(n) rather
    than O(n²) for large resorts.
    """
    segment_graph: Dict[str, Dict] = {}
    from_node_index: Dict = {}  # from_node_id -> [seg_id, ...]

    for _, seg in ski_segments.iterrows():
        seg_id = f"{seg['run_osm_id']}_{seg['segment_index']}"
        fn = seg["from_node_id"] if pd.notna(seg["from_node_id"]) else None
        tn = seg["to_node_id"] if pd.notna(seg["to_node_id"]) else None

        to_lat = seg["to_lat"] if "to_lat" in seg.index and pd.notna(seg["to_lat"]) else None
        to_lon = seg["to_lon"] if "to_lon" in seg.index and pd.notna(seg["to_lon"]) else None
        from_lat = seg["from_lat"] if "from_lat" in seg.index and pd.notna(seg["from_lat"]) else None
        from_lon = seg["from_lon"] if "from_lon" in seg.index and pd.notna(seg["from_lon"]) else None
        from_elev = seg["from_elev_m"] if "from_elev_m" in seg.index and pd.notna(seg["from_elev_m"]) else None
        to_elev = seg["to_elev_m"] if "to_elev_m" in seg.index and pd.notna(seg["to_elev_m"]) else None

        segment_graph[seg_id] = {
            "run_id": seg["run_osm_id"],
            "seg_idx": seg["segment_index"],
            "from_node": fn,
            "to_node": tn,
            "from_lat": from_lat,
            "from_lon": from_lon,
            "to_lat": to_lat,
            "to_lon": to_lon,
            "from_elev": from_elev,
            "to_elev": to_elev,
            "length_m": seg["length_m"],
            "vertical_drop": seg["vertical_drop_m"],
            "run_name": seg["run_name"],
            "piste_type": seg["piste_type"] if "piste_type" in seg.index and pd.notna(seg["piste_type"]) else "downhill",
            "next_segments": set(),
        }
        if fn is not None:
            from_node_index.setdefault(fn, []).append(seg_id)

    # Wire edges using the index (O(n) total)
    for seg_id, data in segment_graph.items():
        to_node = data["to_node"]
        if not to_node:
            continue
        for other_id in from_node_index.get(to_node, []):
            if other_id == seg_id:
                continue
            other = segment_graph[other_id]
            # block backward travel within the same run
            if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                continue
            data["next_segments"].add(other_id)

    # Proximity fallback: for NZ resorts where OSM gaps exist between run endpoints
    if use_proximity_fallback:
        # Build a coarse spatial bucket (1° ≈ 111 km) so the fallback stays O(n)
        bucket_index: Dict = {}  # (int_lat, int_lon) -> [seg_id, ...]
        for seg_id, data in segment_graph.items():
            if data["from_lat"] is not None and data["from_lon"] is not None:
                cell = (int(data["from_lat"]), int(data["from_lon"]))
                bucket_index.setdefault(cell, []).append(seg_id)

        # ±1° search radius covers any gap within ~111 km — far more than needed
        for seg_id, data in segment_graph.items():
            # Only apply fallback when exact-node wiring found nothing
            if data["next_segments"]:
                continue
            if data["to_lat"] is None or data["to_lon"] is None:
                continue

            tlat, tlon = data["to_lat"], data["to_lon"]
            cell = (int(tlat), int(tlon))

            candidates = []
            for dlat in (-1, 0, 1):
                for dlon in (-1, 0, 1):
                    candidates.extend(bucket_index.get((cell[0] + dlat, cell[1] + dlon), []))

            for other_id in candidates:
                if other_id == seg_id:
                    continue
                other = segment_graph[other_id]
                if other["from_lat"] is None or other["from_lon"] is None:
                    continue
                # Block backward same-run travel
                if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                    continue
                # Elevation guard: don't allow uphill proximity connections
                if (
                    data["to_elev"] is not None
                    and other["from_elev"] is not None
                    and other["from_elev"] > data["to_elev"] + 5  # 5m tolerance for flat connectors
                ):
                    continue
                dist = _haversine_m(tlat, tlon, other["from_lat"], other["from_lon"])
                if dist <= _NZ_PROXIMITY_THRESHOLD_M:
                    data["next_segments"].add(other_id)

    return segment_graph


def find_longest_path_for_resort(
    segment_graph: Dict[str, Dict],
    lift_run_mapping: pd.DataFrame,
) -> Optional[Dict]:
    """
    DFS from the top of every lift-served run; return the single longest path.

    Path state carried per DFS frame:
      segments       — ordered list of seg_ids visited
      runs           — ordered list of run_osm_ids (parallel to segments)
      run_names      — ordered list of run names (parallel to segments)
      visited        — set of seg_ids already on this path (cycle guard)
      distance       — cumulative distance in metres
      vertical       — cumulative vertical drop in metres
      lift_id/name   — starting lift metadata

    A path is terminal when the current segment has no unvisited next_segments.
    The best terminal path (by distance) is returned.
    """
    lift_served = lift_run_mapping[
        lift_run_mapping["connection_type"] == "lift_services_run"
    ]

    best_dist: float = -1.0
    best: Optional[Dict] = None
    explored = 0

    for _, lift_row in lift_served.iterrows():
        if explored >= _MAX_STATES:
            break

        lift_id = lift_row["lift_osm_id"]
        lift_name = lift_row["lift_name"]
        run_id = lift_row["run_osm_id"]
        start_seg_id = f"{run_id}_0"

        if start_seg_id not in segment_graph:
            continue

        seg = segment_graph[start_seg_id]
        vert = seg["vertical_drop"] if pd.notna(seg["vertical_drop"]) else 0.0

        stack = [(
            start_seg_id,
            {
                "segments": [start_seg_id],
                "runs": [seg["run_id"]],
                "run_names": [seg["run_name"]],
                "visited": {start_seg_id},
                "hike_runs_used": set(),
                "distance": seg["length_m"],
                "vertical": vert,
                "lift_id": lift_id,
                "lift_name": lift_name,
            },
        )]

        while stack:
            if explored >= _MAX_STATES:
                break
            seg_id, state = stack.pop()
            explored += 1
            seg_data = segment_graph[seg_id]

            nexts = seg_data["next_segments"] - state["visited"]

            # Filter out hike segments whose bootpack run has already been traversed
            nexts = {
                nid for nid in nexts
                if not (
                    segment_graph[nid]["piste_type"] == "hike"
                    and segment_graph[nid]["run_id"] in state["hike_runs_used"]
                )
            }

            if not nexts:
                # terminal — compare to best
                if state["distance"] > best_dist:
                    best_dist = state["distance"]
                    best = state
                continue

            for next_id in nexts:
                ns = segment_graph[next_id]
                nv = ns["vertical_drop"] if pd.notna(ns["vertical_drop"]) else 0.0
                next_hike_used = state["hike_runs_used"]
                if ns["piste_type"] == "hike":
                    next_hike_used = state["hike_runs_used"] | {ns["run_id"]}
                stack.append((next_id, {
                    "segments": state["segments"] + [next_id],
                    "runs": state["runs"] + [ns["run_id"]],
                    "run_names": state["run_names"] + [ns["run_name"]],
                    "visited": state["visited"] | {next_id},
                    "hike_runs_used": next_hike_used,
                    "distance": state["distance"] + ns["length_m"],
                    "vertical": state["vertical"] + nv,
                    "lift_id": state["lift_id"],
                    "lift_name": state["lift_name"],
                }))

    if best is None:
        return None

    # Deduplicate consecutive identical run names for readable path string
    names = best["run_names"]
    run_path = " → ".join(
        n for i, n in enumerate(names) if i == 0 or n != names[i - 1]
    )

    return {
        "starting_lift": best["lift_name"],
        "starting_lift_id": best["lift_id"],
        "run_count": len(set(best["runs"])),
        "total_distance_m": round(best_dist, 1),
        "total_vertical_m": round(abs(best["vertical"]), 1),
        "run_path": run_path,
        # segment node IDs on the path — same format as NI/SI models: "{run_osm_id}_{segment_index}"
        "node_ids": best["segments"],
    }


def model(dbt, session):
    ski_segments: pd.DataFrame = dbt.ref("base_filtered_ski_segments").df()
    lift_run_mapping: pd.DataFrame = dbt.ref("base_lift_run_mapping").df()

    resorts = ski_segments["resort"].unique()

    rows = []
    for resort in resorts:
        resort_segs = ski_segments[ski_segments["resort"] == resort]
        resort_lrm = lift_run_mapping[lift_run_mapping["resort"] == resort]

        if resort_segs.empty:
            continue

        is_nz = resort_segs["country_code"].iloc[0] == "NZ"
        graph = build_segment_graph(resort_segs, use_proximity_fallback=is_nz)
        result = find_longest_path_for_resort(graph, resort_lrm)

        if result is None:
            continue

        row = {"resort": resort}
        row.update(result)
        rows.append(row)

    result_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    result_df["node_ids"] = result_df["node_ids"].apply(
        lambda v: json.dumps(v) if isinstance(v, list) else None
    )

    session.register("__staging_all_resorts_longest_path_df", result_df)
    return session.sql("SELECT * FROM __staging_all_resorts_longest_path_df")
