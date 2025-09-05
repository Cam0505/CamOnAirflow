"""
dbt Python model: Find all possible ski paths for Thredbo Resort
"""

import pandas as pd
import math
from typing import Dict, List
from collections import deque


# --- helpers ---
def build_segment_graph(ski_segments: pd.DataFrame) -> Dict[str, Dict]:
    """Build a graph connecting ski segments through their nodes."""
    segment_graph = {}
    for _, segment in ski_segments.iterrows():
        run_id = segment["run_osm_id"]
        seg_idx = segment["segment_index"]
        seg_id = f"{run_id}_{seg_idx}"
        segment_graph[seg_id] = {
            "run_id": run_id,
            "segment_idx": seg_idx,
            "from_node": segment["from_node_id"] if pd.notna(segment["from_node_id"]) else None,
            "to_node": segment["to_node_id"] if pd.notna(segment["to_node_id"]) else None,
            "next_segments": set(),
        }
    # connect forward
    for seg_id, data in segment_graph.items():
        to_node = data["to_node"]
        if not to_node:
            continue
        for other_id, other_data in segment_graph.items():
            if other_data["from_node"] == to_node:
                if other_id == seg_id:
                    continue
                # downhill only if same run
                if (
                    other_data["run_id"] == data["run_id"]
                    and other_data["segment_idx"] <= data["segment_idx"]
                ):
                    continue
                data["next_segments"].add(other_id)
    return segment_graph


def find_paths_from_run(
    segment_graph: Dict[str, Dict],
    ski_runs: pd.DataFrame,
    lift_run_mapping: pd.DataFrame,
    start_run_id: str,
    lift_id: str,
    lift_name: str,
    resort: str,
) -> List[Dict]:
    """Depth-first search with deque stack to find all possible ski paths from one run."""
    paths = []
    start_run = ski_runs[ski_runs["osm_id"] == start_run_id]
    if start_run.empty:
        return []

    run_data = start_run.iloc[0]
    start_segment_id = f"{start_run_id}_0"
    if start_segment_id not in segment_graph:
        return []

    stack = deque([
        (
            start_segment_id,
            {
                "segments": [start_segment_id],
                "runs": [start_run_id],
                "run_names": [run_data["run_name"]],
                "visited_segments": {start_segment_id},
                "distance": run_data["run_length_m"],
                "vertical": run_data["top_elevation_m"] - run_data["bottom_elevation_m"],
                "starting_lift": lift_name,
                "starting_lift_id": lift_id,
            },
        )
    ])

    path_count = 0
    while stack:
        seg_id, state = stack.pop()
        seg_data = segment_graph.get(seg_id)
        if not seg_data:
            continue

        # terminal condition
        if not seg_data["next_segments"]:
            current_run_id = seg_data["run_id"]
            run_lift_links = lift_run_mapping[
                (lift_run_mapping["run_osm_id"] == current_run_id)
                & (lift_run_mapping["connection_type"] == "run_feeds_lift")
            ]
            if not run_lift_links.empty:
                end_row = run_lift_links.iloc[0]
                end_lift_id = end_row["lift_osm_id"]
                end_lift_name = end_row["lift_name"]
                if end_lift_id == lift_id:
                    end_type = "same_lift"
                else:
                    end_type = "different_lift"
                end_name, end_id = end_lift_name, end_lift_id
            else:
                end_type = "run_end"
                end_name, end_id = state["run_names"][-1], state["runs"][-1]

            compressed_run_names = [
                name
                for i, name in enumerate(state["run_names"])
                if i == 0 or name != state["run_names"][i - 1]
            ]

            avg_gradient_angle = round(
                math.degrees(math.atan(state["vertical"] / state["distance"]))
                if state["distance"] > 0
                else 0,
                1,
            )

            paths.append(
                {
                    "path_id": f"{lift_id}_{path_count}",
                    "resort": resort,
                    "starting_lift": state["starting_lift"],
                    "starting_lift_id": state["starting_lift_id"],
                    "run_count": len(set(state["runs"])),
                    "total_distance_m": round(state["distance"], 1),
                    "total_vertical_m": round(state["vertical"], 1),
                    "avg_gradient": avg_gradient_angle,
                    "run_path": " → ".join(compressed_run_names),
                    "node_ids": state["segments"],
                    "ending_type": end_type,
                    "ending_name": end_name,
                    "ending_id": end_id,
                }
            )
            path_count += 1
            continue

        # expand
        for next_id in seg_data["next_segments"]:
            if next_id in state["visited_segments"]:
                continue
            next_seg = segment_graph[next_id]
            next_run_id = next_seg["run_id"]
            next_run = ski_runs[ski_runs["osm_id"] == next_run_id]
            if next_run.empty:
                continue
            next_data = next_run.iloc[0]
            new_state = {
                "segments": state["segments"] + [next_id],
                "runs": state["runs"] + [next_run_id],
                "run_names": state["run_names"] + [next_data["run_name"]],
                "visited_segments": state["visited_segments"] | {next_id},
                "distance": state["distance"]
                + next_data["run_length_m"] / max(next_data["segment_count"], 1),
                "vertical": state["vertical"]
                + (next_data["top_elevation_m"] - next_data["bottom_elevation_m"])
                / max(next_data["segment_count"], 1),
                "starting_lift": state["starting_lift"],
                "starting_lift_id": state["starting_lift_id"],
            }
            stack.append((next_id, new_state))

    return paths


# === dbt entrypoint ===
def model(dbt, session) -> pd.DataFrame:
    resort = "Thredbo Resort"

    # load sources
    lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    ski_runs = dbt.ref("base_filtered_ski_runs").df()
    ski_segments = dbt.ref("base_filtered_ski_segments").df()

    # filter just this resort
    lift_run_mapping = lift_run_mapping[lift_run_mapping["resort"] == resort]
    ski_runs = ski_runs[ski_runs["resort"] == resort]
    ski_segments = ski_segments[ski_segments["resort"] == resort]

    # add segment count
    seg_counts = ski_segments.groupby("run_osm_id").size().reset_index(name="segment_count")
    ski_runs = ski_runs.merge(seg_counts, left_on="osm_id", right_on="run_osm_id", how="left")
    ski_runs["segment_count"] = ski_runs["segment_count"].fillna(1)

    all_paths = []
    lifts_data = lift_run_mapping[
        lift_run_mapping["connection_type"] == "lift_services_run"
    ].drop_duplicates("lift_osm_id")

    for _, lift in lifts_data.iterrows():
        lift_id, lift_name = lift["lift_osm_id"], lift["lift_name"]
        lift_runs = lift_run_mapping[
            (lift_run_mapping["lift_osm_id"] == lift_id)
            & (lift_run_mapping["connection_type"] == "lift_services_run")
        ]
        for _, run in lift_runs.iterrows():
            run_paths = find_paths_from_run(
                segment_graph=build_segment_graph(ski_segments),
                ski_runs=ski_runs,
                lift_run_mapping=lift_run_mapping,
                start_run_id=run["run_osm_id"],
                lift_id=lift_id,
                lift_name=lift_name,
                resort=resort,
            )
            all_paths.extend(run_paths)

    return pd.DataFrame(all_paths)
