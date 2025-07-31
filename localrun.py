# -*- coding: utf-8 -*-
import importlib

import yaml


def get_case(case_slug: str) -> dict:
    case_file = yaml.safe_load(open("localrun.cases.yaml", encoding="utf-8"))

    for case in case_file["cases"]:
        if case["case_slug"] == case_slug:
            return case

    raise ValueError(f"Case {case_slug} not found in localrun.cases.yaml")


if __name__ == "__main__":

    try:
        selected = yaml.safe_load(open("localrun.selected.yaml"))
    except FileNotFoundError:
        raise FileNotFoundError("Please create a localrun.selected.yaml file.")

    # Get the selected case slug
    selected_case = get_case(selected.get("selected_case"))

    # Get params to override
    override_params = selected.get("override_params", {})

    # Import and get flow variable
    flow_name = selected_case.get("flow_name")
    flow_module = importlib.import_module(selected_case.get("flow_path"))
    flow = getattr(flow_module, flow_name)

    # Get original flow parameters
    original_params = selected_case.get("params")

    # Merge original and override parameters
    flow_params = {**original_params, **override_params}

    # Run the flow with the parameters
    flow.run(**flow_params, run_on_schedule=False)
