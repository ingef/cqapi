from __future__ import annotations
from cqapi.util import check_arg_type
from cqapi.conquery_ids import get_root_concept_id, get_connector_id, is_same_conquery_id, contains_dataset_id
from typing import List, Tuple, Union
import pandas as pd
from cqapi.namespace import Keys


@check_arg_type(["conquery_id_type"], convert_to_list={"conquery_ids": str})
def id_to_label_list(conquery_ids: list, concepts: dict, conquery_id_type: str):
    return [id_to_label(conquery_id, concepts, conquery_id_type) for conquery_id in conquery_ids]


@check_arg_type(["conquery_id", "concepts", "conquery_id_type"])
def id_to_label(conquery_id: str, concepts: dict, conquery_id_type: str):
    conquery_id_type = conquery_id_type.lower()
    conquery_id_list = conquery_id.split(".")
    if len(conquery_id_list) == 1:
        raise ValueError(f"Parameter 'conquery_id' ({conquery_id}) has wrong shape")

    root_concept_id = get_root_concept_id(conquery_id)
    concept_obj = concepts[root_concept_id]
    root_concept_label = concept_obj.get('label')

    if len(conquery_id_list) == 2:
        return root_concept_label

    if conquery_id_type == "concept":
        return " - ".join([root_concept_label, *[conquery_id_list[2:]]])

    # check concept selects
    if conquery_id_type == "concept_select":
        select_obj = [_ for _ in concept_obj.get('selects') if _.get('id') == conquery_id]
        if len(select_obj) > 1:
            raise ValueError(f"Found more than select with id {conquery_id}: \n"
                             f"{concept_obj}")
        if len(select_obj) == 1:
            return " - ".join([root_concept_label, select_obj[0].get('label')])

    connector_id = get_connector_id(conquery_id)
    table_obj = [table for table in concept_obj.get('tables')
                 if table.get('connectorId') == connector_id]
    if len(table_obj) == 0:
        raise ValueError(f"Unknown conquery_id {conquery_id}")
    table_obj = table_obj[0]
    table_label = table_obj.get('label')
    if conquery_id_type == "connector":
        return " - ".join([root_concept_label, table_label])

    # check table selects
    if conquery_id_type == "select":
        select_obj = [_ for _ in table_obj.get('selects') if _.get('id') == conquery_id]
        if len(select_obj) == 0:
            raise ValueError(f"Unknown conquery_id {conquery_id}")
        return " - ".join([root_concept_label, table_label, select_obj[0].get('label')])

    if conquery_id_type == "filter":
        filter_obj = [_ for _ in table_obj.get('filters') if _.get('id') == conquery_id]
        if len(filter_obj) == 0:
            raise ValueError(f"Unknown conquery_id {conquery_id}")
        filter_obj = filter_obj[0]
        return " - ".join([root_concept_label, table_label, filter_obj.get('label')])

    # conquery_id_types selects and filter can be added here
    raise ValueError(f"Unknown conquery_id_type {conquery_id_type}")


class ConqueryId:
    def __init__(self, conquery_id: str, id_type: str = None):
        self.conquery_id_list = conquery_id.split(".")
        if id_type not in ["concept", "concept_select", "connector", "connector_select", "filter", "date"]:
            raise ValueError(f"Unknown {id_type=}")
        self.id_type = id_type

    def get_conquery_id(self):
        return ".".join(self.conquery_id_list)

    def __hash__(self):
        return hash(self.get_conquery_id())

    def __eq__(self, other):
        if isinstance(other, ConqueryId):
            return self.get_conquery_id() == other.get_conquery_id()
        return NotImplemented

    def __repr__(self):
        return f"id={self.get_conquery_id()}, type={self.id_type}"

    @staticmethod
    def is_dataset(dataset) -> bool:
        if dataset.startswith("adb_"):
            return True
        if dataset.startswith("fdb_"):
            return True
        if dataset.startswith("dataset"):
            return True
        return False

    def contains_dataset(self) -> bool:
        return self.is_dataset(self.conquery_id_list[0])

    def get_root_concept_id(self) -> str:
        if self.contains_dataset():
            return ".".join(self.conquery_id_list[:2])
        return ".".join(self.conquery_id_list[:1])

    def get_concept_id(self) -> str:
        if not self.id_type != "concept":
            return self.get_root_concept_id()

        return ".".join(self.conquery_id_list)

    def get_child_concept_id(self) -> Union[str, None]:
        if not self.id_type != "concept":
            return None

        if len(self.conquery_id_list) > (1 + int(self.contains_dataset())):
            return self.conquery_id_list[-1]
        return None

    def get_connector_id(self) -> str:
        if self.contains_dataset():
            return ".".join(self.conquery_id_list[:3])
        return ".".join(self.conquery_id_list[:2])

    def change_dataset(self, dataset: str) -> None:
        if not self.is_dataset(dataset):
            raise ValueError(f"{dataset=} is no valid dataset")
        if self.contains_dataset():
            self.conquery_id_list[0] = dataset
        else:
            self.conquery_id_list = [dataset, *self.conquery_id_list]


class ConqueryIdCollection:
    def __init__(self):
        self.conquery_ids: List[ConqueryId] = list()

    def add(self, removed_id: ConqueryId):
        self.conquery_ids.append(removed_id)

    def create_label_dicts(self, concepts: dict):
        label_dicts = list()
        for conquery_id in self.conquery_ids:
            root_concept_id = conquery_id.get_root_concept_id()
            concept_obj = concepts[root_concept_id]

            label_dict = {"concept": concept_obj[Keys.label]}
            if conquery_id.id_type == "concept":
                child_id = conquery_id.get_child_concept_id()
                if child_id is not None:
                    label_dict["concept"] = " - ".join([label_dict["concept"], child_id])

            elif conquery_id.id_type == "concept_select":
                select_label = [select
                                for select in concept_obj[Keys.selects]
                                if select == conquery_id.get_conquery_id()][0][Keys.label]
                label_dict["concept_select"] = select_label

            else:
                connector_id = conquery_id.get_connector_id()
                table = [table for table in concept_obj[Keys.tables] if table[Keys.connector_id] == connector_id][0]
                label_dict["connector"] = table[Keys.label]

                if conquery_id.id_type == "connector":
                    pass
                elif conquery_id.id_type == "connector_select":
                    select_label = [select_obj[Keys.id]
                                    for select_obj in table[Keys.selects]
                                    if select_obj[Keys.id] == conquery_id.get_conquery_id()][0][Keys.label]

                    label_dict["connector_select"] = select_label

                elif conquery_id.id_type == "filter":
                    filter_label = [filter_obj[Keys.id]
                                    for filter_obj in table[Keys.filters]
                                    if filter_obj[Keys.id] == conquery_id.get_conquery_id()][0][Keys.label]
                    label_dict["filter"] = filter_label

                elif conquery_id.id_type == "date":
                    date_label = [date_obj[Keys.value]
                                  for date_obj in table[Keys.date_column][Keys.options]
                                  if date_obj[Keys.value] == conquery_id.get_conquery_id()][0][Keys.label]
                    label_dict["date"] = date_label

        return label_dicts

    def __eq__(self, other):
        if isinstance(other, ConqueryIdCollection):
            return set(self.conquery_ids) == set(other.conquery_ids)


@check_arg_type(["concept_id"])
def find_concept_id(concept_id: str, concepts: dict, children_ids: List[str]):
    """
    Searches for concept_id in concepts or concept_obj. If concept_id is found True is returned.
    If eva access data is defined, concept_obj is loaded for concept_id level 3 or higher
    :param concept_id: concept_id to search for
    :param concepts: dict with concept_ids
    :param children_ids: List of all children to look for
    """

    concept_id_list = concept_id.split(".")
    root_concept_id = get_root_concept_id(concept_id)

    if len(concept_id_list) == 1:
        raise ValueError(f"{concept_id=} needs to have at least two levels <dataset>.<root_concept>")

    if len(concept_id_list) == 2:
        if root_concept_id in concepts.keys():
            return True
        return False

    if len(concept_id_list) == 3:
        # children of root concepts can be looked up in concepts object
        if concept_id in concepts[root_concept_id].get("children", []):
            return True
        return False

    return concept_id in children_ids
