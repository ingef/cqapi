import sys
from typing import List

this_module = sys.modules[__name__]

this_module.dataset_list = None


def set_dataset_list(dataset_list: List[str]) -> None:
    for dataset in dataset_list:
        if not isinstance(dataset, str):
            raise ValueError(f"dataset entry must be of type str, not {type(dataset)} - {dataset}")

    this_module.dataset_list = list(set(dataset_list))


def _dataset_list_set() -> bool:
    return this_module.dataset_list is not None


def get_dataset_list() -> List[str]:
    if not _dataset_list_set():
        raise RuntimeError("dataset_list variable is not set.")
    return this_module.dataset_list


def add_to_dataset_list(dataset: str) -> None:
    if not isinstance(dataset, str):
        raise ValueError(f"{dataset=} must be of type str, not {type(dataset)}")

    if _dataset_list_set():
        this_module.dataset_list = list({*this_module.dataset_list, dataset})
    else:
        this_module.dataset_list = [dataset]
