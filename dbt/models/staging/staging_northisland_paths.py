"""
dbt Python model: Find all possible ski paths for South Island regions
(using true segment lengths & gradients from base_filtered_ski_segments)
"""

import pandas as pd
from typing import Dict, List


# --- helpers ---
def build_segment_graph(ski_segments: pd.DataFrame) -> Dict[str, Dict]:
    """Graph keyed by segment_id with true length_m & gradient from ski_segments."""
    segment_graph = {}
    for _, seg in ski_segments.iterrows():
        seg_id = f"{seg['run_osm_id']}_{seg['segment_index']}"
        segment_graph[seg_id] = {
            "run_id": seg["run_osm_id"],
            "seg_idx": seg["segment_index"],
            "from_node": seg["from_node_id"] if pd.notna(seg["from_node_id"]) else None,
            "to_node": seg["to_node_id"] if pd.notna(seg["to_node_id"]) else None,
            "length_m": seg["length_m"],
            "gradient": seg["gradient"],   # % slope, from base_filtered_ski_segments
            "vertical_drop": seg["vertical_drop_m"],
            "run_name": seg["run_name"],
            "next_segments": set(),
        }
    # connect segments forward
    for seg_id, data in segment_graph.items():
        to_node = data["to_node"]
        if not to_node:
            continue
        for other_id, other_data in segment_graph.items():
            if other_data["from_node"] == to_node:
                if other_id == seg_id:
                    continue
                # enforce downhill progression within run
                if other_data["run_id"] == data["run_id"] and other_data["seg_idx"] <= data["seg_idx"]:
                    continue
                data["next_segments"].add(other_id)
    return segment_graph


def rolling_max_gradient(gradients: List[float], window: int = 3) -> float:
    """Compute max of absolute rolling-average gradient (default window=3)."""
    if not gradients:
        return 0
    if len(gradients) < window:
        return abs(sum(gradients) / len(gradients))
    max_val = float("-inf")
    for i in range(len(gradients) - window + 1):
        window_avg = sum(gradients[i:i+window]) / window
        max_val = max(max_val, abs(window_avg))   # ✅ take absolute
    return max_val


def find_paths_from_run(
    segment_graph: Dict[str, Dict],
    lift_run_mapping: pd.DataFrame,
    start_run_id: str,
    lift_id: str,
    lift_name: str,
    resort: str,
) -> List[Dict]:
    """DFS search with true segment lengths & gradients."""
    paths = []

    start_seg_id = f"{start_run_id}_0"
    if start_seg_id not in segment_graph:
        return []

    seg = segment_graph[start_seg_id]
    seg_len = seg["length_m"]
    seg_vert = seg["vertical_drop"] if pd.notna(seg["vertical_drop"]) else 0
    seg_grad = seg["gradient"] if pd.notna(seg["gradient"]) else 0

    stack = [
        (
            start_seg_id,
            {
                "segments": [start_seg_id],
                "runs": [seg["run_id"]],
                "run_names": [seg["run_name"]],
                "visited_segments": {start_seg_id},
                "distance": seg_len,
                "vertical": seg_vert,
                "gradients": [seg_grad],
                "starting_lift": lift_name,
                "starting_lift_id": lift_id,
            },
        )
    ]

    path_count = 0
    while stack:
        seg_id, state = stack.pop()
        seg_data = segment_graph[seg_id]

        # terminal condition
        if not seg_data["next_segments"]:
            current_run_id = seg_data["run_id"]

            # get all lifts that this run feeds
            run_lift_links = lift_run_mapping[
                (lift_run_mapping["run_osm_id"] == current_run_id)
                & (lift_run_mapping["connection_type"] == "run_feeds_lift")
            ]

            if run_lift_links.empty:
                end_type = "run_end"
                end_name, end_id = state["run_names"][-1], state["runs"][-1]
            else:
                lift_ids = set(run_lift_links["lift_osm_id"].unique())
                lift_names = set(run_lift_links["lift_name"].unique())

                if state["starting_lift_id"] in lift_ids:
                    end_type = "same_lift"
                    end_name = state["starting_lift"]
                    end_id = state["starting_lift_id"]
                else:
                    end_type = "different_lift"
                    end_name = list(lift_names)[0]
                    end_id = list(lift_ids)[0]

            # collapse consecutive duplicate run names
            compressed_run_names = [
                name for i, name in enumerate(state["run_names"])
                if i == 0 or name != state["run_names"][i - 1]
            ]

            avg_gradient_pct = (
                sum(state["gradients"]) / len(state["gradients"])
                if state["gradients"] else 0
            )
            max_gradient_pct = rolling_max_gradient(state["gradients"], window=3)

            paths.append({
                "path_id": f"{lift_id}_{path_count}",
                "resort": resort,
                "starting_lift": state["starting_lift"],
                "starting_lift_id": state["starting_lift_id"],
                "run_count": len(set(state["runs"])),
                "total_distance_m": round(state["distance"], 1),
                "total_vertical_m": round(state["vertical"], 1),
                "avg_gradient_pct": round(avg_gradient_pct, 3),
                "max_gradient_pct": round(max_gradient_pct, 3),
                "run_path": " → ".join(compressed_run_names),
                "node_ids": state["segments"],
                "ending_type": end_type,
                "ending_name": end_name,
                "ending_id": end_id,
            })
            path_count += 1
            continue

        # expand children
        for next_id in seg_data["next_segments"]:
            if next_id in state["visited_segments"]:
                continue
            next_seg = segment_graph[next_id]
            seg_len = next_seg["length_m"]
            seg_vert = next_seg["vertical_drop"] if pd.notna(next_seg["vertical_drop"]) else 0
            seg_grad = next_seg["gradient"] if pd.notna(next_seg["gradient"]) else 0

            new_state = {
                "segments": state["segments"] + [next_id],
                "runs": state["runs"] + [next_seg["run_id"]],
                "run_names": state["run_names"] + [next_seg["run_name"]],
                "visited_segments": state["visited_segments"] | {next_id},
                "distance": state["distance"] + seg_len,
                "vertical": state["vertical"] + seg_vert,
                "gradients": state["gradients"] + [seg_grad],
                "starting_lift": state["starting_lift"],
                "starting_lift_id": state["starting_lift_id"],
            }
            stack.append((next_id, new_state))

    return paths


# === dbt entrypoint ===
def model(dbt, session) -> pd.DataFrame:
    regions = ["Manawatu-Wanganui", "Taranaki"]

    # load sources
    lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    ski_runs = dbt.ref("base_filtered_ski_runs").df()
    ski_segments = dbt.ref("base_filtered_ski_segments").df()

    # filter
    ski_runs = ski_runs[ski_runs["region"].isin(regions)]
    resorts = ski_runs["resort"].unique()
    lift_run_mapping = lift_run_mapping[lift_run_mapping["resort"].isin(resorts)]
    ski_segments = ski_segments[ski_segments["resort"].isin(resorts)]

    all_paths = []
    for resort in resorts:
        segs_resort = ski_segments[ski_segments["resort"] == resort]
        lifts_resort = lift_run_mapping[lift_run_mapping["resort"] == resort]

        segment_graph = build_segment_graph(segs_resort)
        lifts_data = lifts_resort[
            lifts_resort["connection_type"] == "lift_services_run"
        ].drop_duplicates("lift_osm_id")

        for _, lift in lifts_data.iterrows():
            lift_id, lift_name = lift["lift_osm_id"], lift["lift_name"]
            lift_runs = lifts_resort[
                (lifts_resort["lift_osm_id"] == lift_id)
                & (lifts_resort["connection_type"] == "lift_services_run")
            ]
            for _, run in lift_runs.iterrows():
                run_paths = find_paths_from_run(
                    segment_graph,
                    lifts_resort,
                    run["run_osm_id"],
                    lift_id,
                    lift_name,
                    resort,
                )
                all_paths.extend(run_paths)

    return pd.DataFrame(all_paths)
