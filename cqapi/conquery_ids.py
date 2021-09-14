from __future__ import annotations
from typing import Union, Set
from cqapi.namespace import Keys
import cqapi.datasets
from typeguard import typechecked

# conquery id info
conquery_id_separator = "."
dataset_loc = 0
concept_loc = 1
connector_loc = 2
select_loc = 3
filter_loc = 3


@typechecked
def is_dataset_id(dataset_id: str):
    return dataset_id in cqapi.datasets.get_dataset_list()


def change_dataset(new_dataset: str, conquery_id: str):
    eva_id_list = conquery_id.split(".")
    if len(eva_id_list) < 2:
        raise ValueError(f"Eva_id has to be of shape <dataset>.<id>.<child_id>..., not {conquery_id}")

    return ".".join([new_dataset, *eva_id_list[1:]])


@typechecked
def id_elements_to_id(conquery_id_elements: Union[list, str]):
    if isinstance(conquery_id_elements, str):
        conquery_id_elements = [conquery_id_elements]
    return conquery_id_separator.join(conquery_id_elements)


@typechecked()
def get_conquery_id_element(conquery_id: str, index: int = None):
    conquery_id_elements = conquery_id.split(".")

    if index is None:
        return conquery_id_elements

    return conquery_id_elements[index]


def get_conquery_id_slice(conquery_id: str, first_index: int = None, second_index: int = None, until_then: bool = False,
                          from_then_on: bool = False):
    conquery_id_elements = conquery_id.split(".")
    if second_index is not None and first_index is None:
        raise ValueError("First index must be specified if second index is not None")
    if second_index is not None and first_index > second_index:
        raise ValueError("First index must be greater than second index")

    if first_index is None:
        return conquery_id_elements

    if second_index is not None:
        return conquery_id_elements[first_index:second_index]

    if until_then:
        return conquery_id_elements[:first_index]

    if from_then_on:
        return conquery_id_elements[first_index:]

    raise ValueError("Unexpected variable combination: \n"
                     f"{conquery_id=}\n"
                     f"{first_index=}\n"
                     f"{second_index=}\n"
                     f"{until_then=}\n"
                     f"{from_then_on=}\n")


@typechecked()
def contains_dataset_id(conquery_id: str):
    return is_dataset_id(get_conquery_id_element(conquery_id, dataset_loc))


@typechecked()
def add_dataset_id_to_conquery_id(conquery_id: str, dataset_id: str):
    if not is_dataset_id(dataset_id):
        raise ValueError(f"{dataset_id=} is not a valid id.")

    if contains_dataset_id(conquery_id):
        return id_elements_to_id([dataset_id, *get_conquery_id_slice(conquery_id, dataset_loc + 1,
                                                                     from_then_on=True)])

    return id_elements_to_id([dataset_id, *get_conquery_id_slice(conquery_id)])


@typechecked()
def remove_dataset_id_from_conquery_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       concept_loc,
                                                       from_then_on=True))
    return conquery_id


@typechecked()
def get_root_concept_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       concept_loc + 1,
                                                       until_then=True))
    else:
        return get_conquery_id_element(conquery_id, concept_loc - 1)


@typechecked()
def get_connector_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       connector_loc + 1,
                                                       until_then=True))
    else:
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       connector_loc,
                                                       until_then=True))


@typechecked()
def get_dataset(conquery_id: str):
    if not contains_dataset_id(conquery_id):
        raise ValueError(f"Can not extract dataset for id {conquery_id}.")

    return get_conquery_id_element(conquery_id, dataset_loc)


def child_id_in_concept(child_id: str, concept: dict):
    """Searches for child id in concepts dictionary"""
    for children_ids in [concept_element.get('children', []) for concept_element in concept]:
        if child_id in children_ids:
            return True
    return False


def is_in_conquery_ids(conquery_id: str, conquery_ids: list):
    return any(is_same_conquery_id(conquery_id, conquery_id_from_list) for conquery_id_from_list in conquery_ids)


def is_same_conquery_id(conquery_id_1: str, conquery_id_2: str, id_separator=conquery_id_separator,
                        can_diff_in_depth=True):
    """Splits ids by 'id_separator' and iterates over both reversed list.
    If 'can_diff_in_depth' is True, comparison between 'age.age_select' and 'age' will be True"""
    if not can_diff_in_depth and conquery_id_1 != conquery_id_2:
        return False

    for id_section_1, id_section_2 in zip(reversed(conquery_id_1.split(id_separator)),
                                          reversed(conquery_id_2.split(id_separator))):
        if id_section_1 != id_section_2:
            return False
    return True


class ConqueryId:
    def __init__(self, conquery_id: str, id_type: str = None):
        self.conquery_id_list = conquery_id.split(".")
        if id_type is not None and \
                id_type not in ["concept", "concept_select", "connector", "connector_select", "filter", "date"]:
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

    def get_label_dict(self, concepts: dict):
        if self.id_type is None:
            raise ValueError(f"{self} has no id_type")
        root_concept_id = self.get_root_concept_id()
        concept_obj = concepts[root_concept_id]

        label_dict = {"concept": concept_obj[Keys.label]}
        if self.id_type == "concept":
            child_id = self.get_child_concept_id()
            if child_id is not None:
                label_dict["concept"] = " - ".join([label_dict["concept"], child_id])

        elif self.id_type == "concept_select":
            select_label = [select
                            for select in concept_obj[Keys.selects]
                            if select == self.get_conquery_id()][0][Keys.label]
            label_dict["concept_select"] = select_label

        else:
            connector_id = self.get_connector_id()
            table = [table for table in concept_obj[Keys.tables] if table[Keys.connector_id] == connector_id][0]
            label_dict["connector"] = table[Keys.label]

            if self.id_type == "connector":
                pass
            elif self.id_type == "connector_select":
                select_label = [select_obj
                                for select_obj in table[Keys.selects]
                                if select_obj[Keys.id] == self.get_conquery_id()][0][Keys.label]

                label_dict["connector_select"] = select_label

            elif self.id_type == "filter":
                filter_label = [filter_obj
                                for filter_obj in table[Keys.filters]
                                if filter_obj[Keys.id] == self.get_conquery_id()][0][Keys.label]
                label_dict["filter"] = filter_label

            elif self.id_type == "date":
                date_label = [date_obj[Keys.value]
                              for date_obj in table[Keys.date_column][Keys.options]
                              if date_obj[Keys.value] == self.get_conquery_id()][0][Keys.label]
                label_dict["date"] = date_label

        return label_dict


class ConqueryIdCollection:
    def __init__(self, conquery_ids: Set[ConqueryId] = None):
        if conquery_ids is None:
            self.conquery_ids: Set[ConqueryId] = set()
        else:
            self.conquery_ids = conquery_ids

    def is_empty(self):
        return len(self.conquery_ids) == 0

    def add(self, conquery_id: ConqueryId):
        self.conquery_ids.add(conquery_id)

    def update(self, other: ConqueryIdCollection):
        if isinstance(other, ConqueryIdCollection):
            for conquery_id in other.conquery_ids:
                self.add(conquery_id)
        else:
            raise NotImplementedError

    def create_label_dicts(self, concepts: dict):
        label_dicts = list()
        for conquery_id in self.conquery_ids:
            label_dicts.append(conquery_id.get_label_dict(concepts=concepts))
        return label_dicts

    def __eq__(self, other):
        if isinstance(other, ConqueryIdCollection):
            return self.conquery_ids == other.conquery_ids

        raise NotImplementedError

    def print_id_labels_as_table(self, concepts: dict):
        import pandas as pd
        header_mapping = {
            "Konzept": "concept",
            "Zusatzwert (Konzept)": "concept_select",
            "Quelle": "connector",
            "Zusatzwert (Quelle)": "connector_select",
            "Filter": "filter",
            "Relevanter Zeitraum": "date"
        }
        label_dicts = self.create_label_dicts(concepts=concepts)
        table_as_dict = {header: [label_dict.get(key, "") for label_dict in label_dicts]
                         for header, key in header_mapping.items()}
        sort_by_cols = list()
        for header in header_mapping.keys():
            if set(table_as_dict[header]) == {""}:
                table_as_dict.pop(header)
            else:
                sort_by_cols.append(header)

        return pd.DataFrame(table_as_dict).sort_values(by=sort_by_cols)
