"""
dbt Python model: Find the steepest continuous downhill ski path per resort
for ALL resorts worldwide.

Uses the same segment-level directed graph as staging_all_resorts_longest_path:
  - Nodes  : "{run_osm_id}_{segment_index}" — one node per OSM segment
  - Edges  : seg A -> seg B when A.to_node_id == B.from_node_id
             (downhill-only; backward same-run traversal blocked)

Steepness is measured as avg_gradient = total_vertical_m / total_distance_m
(equivalent to mean gradient fraction along the path).

To avoid trivially short paths dominating (a single steep segment averages
higher than any multi-run descent), a per-resort minimum path length is
enforced at the 25th percentile of all reachable terminal path distances.

Algorithm — two DFS passes:
  Pass 1 (lightweight): traverse the graph collecting only the total distance
    of every terminal path (no path data stored).  This gives the length
    distribution used to compute the p25 threshold.
  Pass 2 (full):  traverse again carrying full path state; at each terminal
    node compare against the p25 threshold and track the steepest qualifying
    path found so far.

Each pass is capped at _MAX_STATES DFS node expansions to keep large/dense
resorts tractable.  The p25 from pass 1 is a slight over-estimate when the
cap fires (shorter, earlier paths are under-represented), but it remains a
meaningful minimum that rules out trivially short paths.

Starting points and proximity-fallback logic are identical to
staging_all_resorts_longest_path.
"""

import math
import pandas as pd
import json
from typing import Dict, List, Optional

OUTPUT_COLUMNS = [
    "resort",
    "starting_lift",
    "starting_lift_id",
    "run_count",
    "total_distance_m",
    "total_vertical_m",
    "avg_gradient_pct",
    "min_length_threshold_m",
    "run_path",
    "node_ids",
]

# DFS state cap per pass per resort.
_MAX_STATES = 2_000_000

# Proximity fallback threshold for NZ resorts.
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
            if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                continue
            data["next_segments"].add(other_id)

    if use_proximity_fallback:
        bucket_index: Dict = {}
        for seg_id, data in segment_graph.items():
            if data["from_lat"] is not None and data["from_lon"] is not None:
                cell = (int(data["from_lat"]), int(data["from_lon"]))
                bucket_index.setdefault(cell, []).append(seg_id)

        for seg_id, data in segment_graph.items():
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
                if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                    continue
                if (
                    data["to_elev"] is not None
                    and other["from_elev"] is not None
                    and other["from_elev"] > data["to_elev"] + 5
                ):
                    continue
                dist = _haversine_m(tlat, tlon, other["from_lat"], other["from_lon"])
                if dist <= _NZ_PROXIMITY_THRESHOLD_M:
                    data["next_segments"].add(other_id)

    return segment_graph


def _collect_terminal_distances(
    segment_graph: Dict[str, Dict],
    lift_served: pd.DataFrame,
) -> List[float]:
    """
    Lightweight DFS pass: collect the total distance of every terminal path
    without storing full path state.  Used to compute the p25 length threshold.
    """
    distances: List[float] = []
    explored = 0

    for _, lift_row in lift_served.iterrows():
        if explored >= _MAX_STATES:
            break

        run_id = lift_row["run_osm_id"]
        start_seg_id = f"{run_id}_0"
        if start_seg_id not in segment_graph:
            continue

        seg = segment_graph[start_seg_id]

        # Lightweight state: (seg_id, visited_frozenset, hike_runs_frozenset, distance)
        stack = [(
            start_seg_id,
            frozenset([start_seg_id]),
            frozenset(),
            seg["length_m"],
        )]

        while stack:
            if explored >= _MAX_STATES:
                break
            seg_id, visited, hike_used, dist = stack.pop()
            explored += 1

            nexts = segment_graph[seg_id]["next_segments"] - visited
            nexts = {
                nid for nid in nexts
                if not (
                    segment_graph[nid]["piste_type"] == "hike"
                    and segment_graph[nid]["run_id"] in hike_used
                )
            }

            if not nexts:
                distances.append(dist)
                continue

            for next_id in nexts:
                ns = segment_graph[next_id]
                next_hike = hike_used | {ns["run_id"]} if ns["piste_type"] == "hike" else hike_used
                stack.append((
                    next_id,
                    visited | {next_id},
                    next_hike,
                    dist + ns["length_m"],
                ))

    return distances


def find_steepest_path_for_resort(
    segment_graph: Dict[str, Dict],
    lift_run_mapping: pd.DataFrame,
) -> Optional[Dict]:
    """
    Two-pass DFS to find the steepest qualifying path for a resort.

    Pass 1: collect all terminal path distances → compute p25 min_length.
    Pass 2: full DFS with path state; at terminals filter by min_length,
            track the path with the highest avg_gradient (vertical / distance).
    """
    lift_served = lift_run_mapping[
        lift_run_mapping["connection_type"] == "lift_services_run"
    ]

    if lift_served.empty:
        return None

    # ── Pass 1: length distribution ─────────────────────────────────────────
    terminal_distances = _collect_terminal_distances(segment_graph, lift_served)

    if not terminal_distances:
        return None

    terminal_distances_sorted = sorted(terminal_distances)
    p25_idx = max(0, int(len(terminal_distances_sorted) * 0.25) - 1)
    min_length = terminal_distances_sorted[p25_idx]

    # ── Pass 2: full DFS, steepest terminal path >= min_length ──────────────
    best_gradient: float = -1.0
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
            nexts = {
                nid for nid in nexts
                if not (
                    segment_graph[nid]["piste_type"] == "hike"
                    and segment_graph[nid]["run_id"] in state["hike_runs_used"]
                )
            }

            if not nexts:
                # Terminal: apply min_length filter then check steepness
                dist = state["distance"]
                if dist >= min_length and dist > 0:
                    gradient = abs(state["vertical"]) / dist
                    if gradient > best_gradient:
                        best_gradient = gradient
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

    names = best["run_names"]
    run_path = " → ".join(
        n for i, n in enumerate(names) if i == 0 or n != names[i - 1]
    )

    dist = best["distance"]
    avg_gradient_pct = round((abs(best["vertical"]) / dist) * 100, 1) if dist > 0 else 0.0

    return {
        "starting_lift": best["lift_name"],
        "starting_lift_id": best["lift_id"],
        "run_count": len(set(best["runs"])),
        "total_distance_m": round(dist, 1),
        "total_vertical_m": round(abs(best["vertical"]), 1),
        "avg_gradient_pct": avg_gradient_pct,
        "min_length_threshold_m": round(min_length, 1),
        "run_path": run_path,
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
        result = find_steepest_path_for_resort(graph, resort_lrm)

        if result is None:
            continue

        row = {"resort": resort}
        row.update(result)
        rows.append(row)

    result_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    result_df["node_ids"] = result_df["node_ids"].apply(
        lambda v: json.dumps(v) if isinstance(v, list) else None
    )

    session.register("__staging_all_resorts_steepest_path_df", result_df)
    return session.sql("SELECT * FROM __staging_all_resorts_steepest_path_df")
