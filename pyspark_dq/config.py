"""Load dataset data-quality configs from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import List, Union

import yaml

from .models import CheckDefinition, DatasetConfig


def load_config(path: Union[str, Path]) -> DatasetConfig:
    """Load a single YAML file describing one dataset's checks."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw:
        raise ValueError(f"Config file '{path}' is empty")

    dataset = raw["dataset"]
    layer = raw["layer"]
    checks = [CheckDefinition(**check_raw) for check_raw in raw.get("checks", [])]

    if not checks:
        raise ValueError(f"Config file '{path}' defines no checks")

    return DatasetConfig(dataset=dataset, layer=layer, checks=checks)


def load_configs(path: Union[str, Path]) -> List[DatasetConfig]:
    """Load one config file, or every *.yml/*.yaml file in a directory."""
    path = Path(path)
    if path.is_dir():
        files = sorted(list(path.glob("*.yaml")) + list(path.glob("*.yml")))
        if not files:
            raise ValueError(f"No YAML config files found in directory '{path}'")
        return [load_config(f) for f in files]
    return [load_config(path)]
