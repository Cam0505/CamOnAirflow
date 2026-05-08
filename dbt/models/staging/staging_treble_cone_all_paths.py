"""
dbt Python model: Enumerate every possible downhill route through Treble Cone Ski Area.

One row per complete, terminal route — starting from a lift-served run top and
ending when no unvisited downhill segment is reachable.

Uses the same segment-level directed graph as staging_all_resorts_steepest_path:
  - Nodes  : "{run_osm_id}_{segment_index}"
  - Edges  : seg A -> seg B when A.to_node_id == B.from_node_id
  - NZ proximity fallback bridges OSM gaps within 50 m
  - Hike runs may only be traversed once per path (bootpack anti-loop rule)
  - Same-run backward traversal is blocked (seg_idx guard)

Output columns per route
────────────────────────
  path_id              Sequential integer (1-based, ordered by DFS discovery)
  starting_lift        Name of the lift that deposits skiers at the route start
  starting_lift_id     OSM ID of that lift
  run_count            Number of distinct runs touched
  n_segments           Total segments traversed
  total_distance_m     Sum of segment lengths (m)
  total_vertical_m     Total elevation drop (m, positive)
  avg_gradient_pct     total_vertical_m / total_distance_m × 100
  max_gradient_pct     Rolling 3-segment window max of absolute gradient (%)
  avg_gradient_deg     atan(avg_gradient_pct / 100) in degrees
  max_gradient_deg     atan(max_gradient_pct / 100) in degrees
  run_path             "Run A → Run B → Run C" (consecutive duplicates removed)
  runs_touched         JSON array of unique run names (sorted alphabetically)
  node_ids             JSON array of "{run_osm_id}_{seg_idx}" for every segment
  difficulty_sequence  "advanced → intermediate → easy" (consecutive dupes removed)
  ending_type          run_end | same_lift | different_lift
  ending_name          Name of the terminal lift (NULL when ending_type=run_end)

Capped at _MAX_PATHS total routes to prevent memory explosion on dense graphs.
"""

import math
import json
import pandas as pd
from typing import Dict, List

RESORT = "Treble Cone Ski Area"

# Hard cap — Treble Cone is ~25 runs so this is effectively unlimited in practice,
# but guards against edge cases in graph topology.
_MAX_PATHS = 500_000

# NZ proximity fallback — bridges OSM endpoint gaps up to this distance.
_NZ_PROXIMITY_M = 50.0

OUTPUT_COLUMNS = [
    "path_id",
    "starting_lift",
    "starting_lift_id",
    "run_count",
    "n_segments",
    "total_distance_m",
    "total_vertical_m",
    "avg_gradient_pct",
    "max_gradient_pct",
    "avg_gradient_deg",
    "max_gradient_deg",
    "run_path",
    "runs_touched",
    "node_ids",
    "difficulty_sequence",
    "ending_type",
    "ending_name",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    lat_mid = math.radians((lat1 + lat2) / 2.0)
    return math.sqrt(dlat ** 2 + (math.cos(lat_mid) * dlon) ** 2) * 6_371_000


def _rolling_max_gradient(gradients: List[float], window: int = 3) -> float:
    """Max steepness via rolling mean of absolute gradients over `window` segments."""
    if not gradients:
        return 0.0
    if len(gradients) < window:
        return abs(sum(gradients) / len(gradients))
    return max(
        abs(sum(gradients[i: i + window]) / window)
        for i in range(len(gradients) - window + 1)
    )


def _dedup_consecutive(items: List) -> List:
    """Return list with consecutive duplicate values collapsed to one."""
    out = []
    for item in items:
        if not out or item != out[-1]:
            out.append(item)
    return out


# ── graph construction ────────────────────────────────────────────────────────

def build_segment_graph(ski_segments: pd.DataFrame) -> Dict[str, Dict]:
    """
    Build a directed segment graph for a single resort.

    Applies NZ proximity fallback so that runs with OSM endpoint gaps
    (no shared node ID) are still connected when within _NZ_PROXIMITY_M metres.
    """
    segment_graph: Dict[str, Dict] = {}
    from_node_index: Dict = {}

    for _, seg in ski_segments.iterrows():
        seg_id = f"{seg['run_osm_id']}_{seg['segment_index']}"

        def _f(col):
            return seg[col] if col in seg.index and pd.notna(seg[col]) else None

        fn = _f("from_node_id")
        tn = _f("to_node_id")

        segment_graph[seg_id] = {
            "run_id": int(seg["run_osm_id"]),
            "seg_idx": int(seg["segment_index"]),
            "from_node": fn,
            "to_node": tn,
            "from_lat": _f("from_lat"),
            "from_lon": _f("from_lon"),
            "to_lat": _f("to_lat"),
            "to_lon": _f("to_lon"),
            "from_elev": _f("from_elev_m"),
            "to_elev": _f("to_elev_m"),
            "length_m": float(seg["length_m"] or 0),
            "vertical_drop": float(_f("vertical_drop_m") or 0),
            "gradient": float(_f("gradient") or 0),  # smoothed %, downhill negative
            "run_name": str(seg["run_name"]) if pd.notna(seg["run_name"]) else "Unnamed Run",
            "difficulty": str(seg["difficulty"]) if pd.notna(seg["difficulty"]) else None,
            "piste_type": str(_f("piste_type") or "downhill"),
            "next_segments": set(),
        }
        if fn is not None:
            from_node_index.setdefault(fn, []).append(seg_id)

    # Wire edges via shared node IDs  (O(n))
    for seg_id, data in segment_graph.items():
        tn = data["to_node"]
        if not tn:
            continue
        for other_id in from_node_index.get(tn, []):
            if other_id == seg_id:
                continue
            other = segment_graph[other_id]
            # Block backward traversal within the same run
            if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                continue
            data["next_segments"].add(other_id)

    # NZ proximity fallback — bucket by 1° cells (~111 km) for O(n) lookup
    bucket: Dict = {}
    for seg_id, data in segment_graph.items():
        if data["from_lat"] is not None and data["from_lon"] is not None:
            cell = (int(data["from_lat"]), int(data["from_lon"]))
            bucket.setdefault(cell, []).append(seg_id)

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
                candidates.extend(bucket.get((cell[0] + dlat, cell[1] + dlon), []))
        for other_id in candidates:
            if other_id == seg_id:
                continue
            other = segment_graph[other_id]
            if other["from_lat"] is None or other["from_lon"] is None:
                continue
            if other["run_id"] == data["run_id"] and other["seg_idx"] <= data["seg_idx"]:
                continue
            # Elevation guard: don't connect uphill
            if (
                data["to_elev"] is not None
                and other["from_elev"] is not None
                and other["from_elev"] > data["to_elev"] + 5
            ):
                continue
            if _haversine_m(tlat, tlon, other["from_lat"], other["from_lon"]) <= _NZ_PROXIMITY_M:
                data["next_segments"].add(other_id)

    return segment_graph


# ── path enumeration ──────────────────────────────────────────────────────────

def enumerate_all_paths(
    segment_graph: Dict[str, Dict],
    lift_run_mapping: pd.DataFrame,
) -> List[Dict]:
    """
    DFS from each lift-served run top. Records every terminal path reached.

    Terminal = no unvisited, reachable downhill segments remain.
    Capped at _MAX_PATHS total routes.
    """
    lift_served = lift_run_mapping[lift_run_mapping["connection_type"] == "lift_services_run"]
    run_feeds_lift = lift_run_mapping[lift_run_mapping["connection_type"] == "run_feeds_lift"]

    # Build lookup: run_osm_id -> list of {lift_id, lift_name} that run feeds into
    feeds_index: Dict[int, List[Dict]] = {}
    for _, row in run_feeds_lift.iterrows():
        feeds_index.setdefault(int(row["run_osm_id"]), []).append({
            "lift_id": int(row["lift_osm_id"]),
            "lift_name": row["lift_name"],
        })

    all_paths: List[Dict] = []
    path_count = 0

    for _, lift_row in lift_served.iterrows():
        if path_count >= _MAX_PATHS:
            break

        lift_id = int(lift_row["lift_osm_id"])
        lift_name = str(lift_row["lift_name"])
        run_id = int(lift_row["run_osm_id"])
        start_id = f"{run_id}_0"

        if start_id not in segment_graph:
            continue

        seg0 = segment_graph[start_id]

        # DFS state: (current_seg_id, path_state_dict)
        # Immutable-style copies on every branch to avoid shared-state bugs.
        stack = [(start_id, {
            "seg_ids": [start_id],
            "visited": {start_id},
            "hike_used": set(),
            "run_ids": [run_id],
            "run_names": [seg0["run_name"]],
            "difficulties": [seg0["difficulty"]],
            "gradients": [seg0["gradient"]],
            "distance": seg0["length_m"],
            "vertical": seg0["vertical_drop"],
            "lift_id": lift_id,
            "lift_name": lift_name,
        })]

        while stack and path_count < _MAX_PATHS:
            seg_id, state = stack.pop()
            seg_data = segment_graph[seg_id]

            # Candidate next segments: unvisited, not a repeated hike run
            nexts = seg_data["next_segments"] - state["visited"]
            nexts = {
                nid for nid in nexts
                if not (
                    segment_graph[nid]["piste_type"] == "hike"
                    and segment_graph[nid]["run_id"] in state["hike_used"]
                )
            }

            if not nexts:
                # ── Terminal path ─────────────────────────────────────────────
                dist = state["distance"]
                vert = abs(state["vertical"])
                avg_g_pct = (vert / dist * 100.0) if dist > 0 else 0.0
                max_g_pct = _rolling_max_gradient(state["gradients"])

                # Ending type: does the terminal run feed a lift?
                terminal_run_id = seg_data["run_id"]
                feeds = feeds_index.get(terminal_run_id, [])
                if not feeds:
                    ending_type = "run_end"
                    ending_name = None
                elif any(f["lift_id"] == state["lift_id"] for f in feeds):
                    ending_type = "same_lift"
                    ending_name = next(
                        f["lift_name"] for f in feeds if f["lift_id"] == state["lift_id"]
                    )
                else:
                    ending_type = "different_lift"
                    ending_name = feeds[0]["lift_name"]

                # Deduplicated consecutive sequences
                run_path = " → ".join(_dedup_consecutive(state["run_names"]))
                diff_seq = " → ".join(
                    str(d) for d in _dedup_consecutive(state["difficulties"])
                )
                unique_runs = sorted({n for n in state["run_names"] if n})

                path_count += 1
                all_paths.append({
                    "path_id": path_count,
                    "starting_lift": state["lift_name"],
                    "starting_lift_id": state["lift_id"],
                    "run_count": len(set(state["run_ids"])),
                    "n_segments": len(state["seg_ids"]),
                    "total_distance_m": round(dist, 1),
                    "total_vertical_m": round(vert, 1),
                    "avg_gradient_pct": round(avg_g_pct, 2),
                    "max_gradient_pct": round(max_g_pct, 2),
                    "avg_gradient_deg": round(math.degrees(math.atan(avg_g_pct / 100.0)), 2),
                    "max_gradient_deg": round(math.degrees(math.atan(max_g_pct / 100.0)), 2),
                    "run_path": run_path,
                    "runs_touched": json.dumps(unique_runs),
                    "node_ids": json.dumps(state["seg_ids"]),
                    "difficulty_sequence": diff_seq,
                    "ending_type": ending_type,
                    "ending_name": ending_name,
                })
                continue

            # Push branches for each onward segment
            for next_id in nexts:
                ns = segment_graph[next_id]
                next_hike_used = (
                    state["hike_used"] | {ns["run_id"]}
                    if ns["piste_type"] == "hike"
                    else state["hike_used"]
                )
                stack.append((next_id, {
                    "seg_ids": state["seg_ids"] + [next_id],
                    "visited": state["visited"] | {next_id},
                    "hike_used": next_hike_used,
                    "run_ids": state["run_ids"] + [ns["run_id"]],
                    "run_names": state["run_names"] + [ns["run_name"]],
                    "difficulties": state["difficulties"] + [ns["difficulty"]],
                    "gradients": state["gradients"] + [ns["gradient"]],
                    "distance": state["distance"] + ns["length_m"],
                    "vertical": state["vertical"] + ns["vertical_drop"],
                    "lift_id": state["lift_id"],
                    "lift_name": state["lift_name"],
                }))

    return all_paths


# ── dbt entrypoint ────────────────────────────────────────────────────────────

def model(dbt, session):
    ski_segments: pd.DataFrame = dbt.ref("base_filtered_ski_segments").df()
    lift_run_mapping: pd.DataFrame = dbt.ref("base_lift_run_mapping").df()

    tc_segs = ski_segments[ski_segments["resort"] == RESORT].copy()
    tc_lrm = lift_run_mapping[lift_run_mapping["resort"] == RESORT].copy()

    if tc_segs.empty:
        empty_sql = "SELECT " + ", ".join(f"NULL AS {c}" for c in OUTPUT_COLUMNS) + " WHERE 1=0"
        return session.sql(empty_sql)

    graph = build_segment_graph(tc_segs)
    paths = enumerate_all_paths(graph, tc_lrm)

    if not paths:
        empty_sql = "SELECT " + ", ".join(f"NULL AS {c}" for c in OUTPUT_COLUMNS) + " WHERE 1=0"
        return session.sql(empty_sql)

    result_df = pd.DataFrame(paths, columns=OUTPUT_COLUMNS)

    session.register("__treble_cone_all_paths_df", result_df)
    return session.sql("SELECT * FROM __treble_cone_all_paths_df")
