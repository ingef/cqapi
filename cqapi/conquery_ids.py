from __future__ import annotations
from typing import List, Union, Set
from abc import ABC, abstractmethod

# Conquery Id Info
conquery_id_separator = "."
dataset_index = 1
concept_index = 2
concept_select_index = 3
connector_index = 3
child_index = 3
connector_select_index = 4
filter_index = 4
date_index = 4


class ConqueryId(ABC):
    def __init__(self, name: str, base: ConqueryId = None):
        self.base = base
        self.name = name

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other_id):
        if isinstance(other_id, ConqueryId):
            return self.id == other_id.id
        return NotImplemented

    def __repr__(self):
        return self.id

    @property
    def id(self):
        """
        Datasets returns its name, all other Ids that build on it add their name to the string.
        """
        if self.base:
            return f"{self.base.id}.{self.name}"
        else:
            return self.name

    @property
    @abstractmethod
    def base(self) -> ConqueryId:
        """
        Getter for base (the conqueryId that the current one builds on
        """
        pass

    @base.setter
    @abstractmethod
    def base(self, new_base: ConqueryId):
        """
        Setter for base, should check if ConqueryId that is set is a valid entry for the Instance
        """
        pass

    def is_dataset(self) -> bool:
        return isinstance(self, DatasetId)

    def is_same_conquery_id(self, other_id):
        return self.__eq__(other_id=other_id)

    @abstractmethod
    def get_concept_id(self) -> str:
        pass

    @abstractmethod
    def get_connector_id(self) -> str:
        pass

    @classmethod
    @abstractmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ConqueryId:
        """
        Method is used in from_str and should validate provided string and recursively initiate objects for each
        inherited class needed for the provided Id
        """
        pass

    @classmethod
    def from_str(cls, id_string: str) -> ConqueryId:
        """
        Splits the string on the separator and initiates instances of conqueryIds to represent the string
        """
        id_list = id_string.split(conquery_id_separator)
        return cls.create_id_objects_recursively(id_list=id_list)

    @abstractmethod
    def change_dataset(self, new_dataset: str):
        pass

    @abstractmethod
    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        """
        Method needed for get_label_dict and adds relevant information about the current class instance to the label
        dict referencing the labels in the concept object
        """
        pass

    @classmethod
    def _filter_concept_obj(cls, filter_obj: list, filter_key: str, compare_string: str, return_key: str) \
            -> Union[str, list, dict]:
        """
        Method used in add_self_label_to_dict in subclasses.
        Filters a list of dictionaries (filter_obj) based on if for each element, the value of the dict for filter_key
        equals the compare_string. If so, return the value of the dict of return_key
        """
        return [element[return_key] for element in filter_obj if element[filter_key] == compare_string][0]

    def get_label_dict(self, concepts: dict) -> dict:
        """
        Get a specific dictionary of instances of the conqueryId and their labels
        """
        concept_id = self.get_concept_id()
        concept_obj = concepts[concept_id]
        label_dict = {"concept": concept_obj[Keys.label]}
        label_dict = self.add_self_label_to_dict(label_dict=label_dict, concept_obj=concept_obj)
        return label_dict


class DatasetId(ConqueryId):
    def __init__(self, name: str):
        super().__init__(name=name, base=None)

    @property
    def base(self) -> ConqueryId:
        return self._base

    @base.setter
    def base(self, new_base: ConqueryId):
        if new_base:
            raise ValueError("Dataset cannot have base")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.name = new_dataset

    def get_concept_id(self):
        return None

    def get_connector_id(self):
        return None

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict):
        return None

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> DatasetId:
        if len(id_list) != dataset_index:
            raise ValueError(f"Provided list of ids for Dataset must be of length {dataset_index}. "
                             f"Provided: {id_list}")
        return DatasetId(name=id_list[0])


class ConceptId(ConqueryId):
    def __init__(self, name: str, base: DatasetId):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> DatasetId:
        return self._base

    @base.setter
    def base(self, new_base: DatasetId):
        if not isinstance(new_base, DatasetId):
            raise ValueError(f"Base of Concept can only be a Dataset. Provided: {new_base.id}")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self) -> str:
        return self.id

    def get_connector_id(self):
        return None

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        dataset_id = self.base.id
        label_dict["concept"] = " - ".join([label_dict["concept"], dataset_id])
        return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ConceptId:
        if len(id_list) != concept_index:
            raise ValueError(f"Provided string for Concept must be of length {concept_index} (dataset and concept). "
                             f"Provided: {id_list}")
        concept_id = id_list.pop(-1)
        base = DatasetId.create_id_objects_recursively(id_list=id_list)
        return ConceptId(name=concept_id, base=base)


class ConnectorId(ConqueryId):
    def __init__(self, name: str, base: ConceptId):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> ConceptId:
        return self._base

    @base.setter
    def base(self, new_base: ConceptId):
        if not isinstance(new_base, ConceptId):
            raise ValueError(f"Base of Connector can only be a Concept. Provided: {new_base}")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self) -> str:
        return self.base.get_concept_id()

    def get_connector_id(self) -> str:
        return self.id

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        connector_id = self.get_connector_id()
        connector_label = self._filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.connector_id,
                                                   compare_string=connector_id, return_key=Keys.label)
        label_dict["connector"] = connector_label
        return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ConnectorId:
        if len(id_list) != connector_index:
            raise ValueError(f"Provided string for Connector must be of length {connector_index} "
                             f"(dataset, concept and connector). "
                             f"Provided: {id_list}")
        connector_id = id_list.pop(-1)
        base = ConceptId.create_id_objects_recursively(id_list=id_list)
        return ConnectorId(name=connector_id, base=base)


class ChildId(ConqueryId):
    def __init__(self, name: str, base: Union[ChildId, ConceptId]):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> Union[ChildId, ConceptId]:
        return self._base

    @base.setter
    def base(self, new_base: Union[ConceptId, ChildId]):
        if not isinstance(new_base, ConceptId) and not isinstance(new_base, ChildId):
            raise ValueError("Base of Child can only be a Concept or Child")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self) -> str:
        return self.base.get_concept_id()

    def get_connector_id(self) -> str:
        return self.base.get_connector_id()

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ChildId:
        if len(id_list) > child_index:
            base = ChildId.create_id_objects_recursively(id_list=id_list)
        elif len(id_list) == child_index:
            base = ConceptId.create_id_objects_recursively(id_list=id_list)
        else:
            raise ValueError(f"Provided string for Child must be of minimum length {child_index} "
                             f"(dataset, concept and child/children). "
                             f"Provided: {id_list}")

        child_id = id_list.pop(-1)
        return ChildId(name=child_id, base=base)


class SelectId(ConqueryId):
    def __init__(self, name: str, base: Union[ConceptId, ConnectorId]):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> Union[ConceptId, ConnectorId]:
        return self._base

    @base.setter
    def base(self, new_base: Union[ConceptId, ConnectorId]):
        if not isinstance(new_base, ConceptId) and not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Select can only be a Concept or Connector")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self):
        return self.base.get_concept_id()

    def get_connector_id(self) -> str:
        return self.base.get_connector_id()

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        if isinstance(self.base, ConnectorId):
            label_dict = self.base.add_self_label_to_dict(label_dict=label_dict, concept_obj=concept_obj)

            table_selects = self._filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.label,
                                                     compare_string=label_dict['connector'], return_key=Keys.selects)

            select_label = self._filter_concept_obj(filter_obj=table_selects, filter_key=Keys.id,
                                                    compare_string=self.id, return_key=Keys.label)

            label_dict["connector_select"] = select_label
            return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> SelectId:
        select_id = id_list.pop(-1)
        if len(id_list) == concept_select_index:
            base = ConceptId.create_id_objects_recursively(id_list=id_list)
        elif len(id_list) == connector_select_index:
            base = ConnectorId.create_id_objects_recursively(id_list=id_list)
        else:
            raise ValueError(f"Provided string for Select must be of length {concept_select_index} or "
                             f"{connector_select_index} (dataset, concept, (connector) and select. "
                             f"Provided: {id_list}")

        return SelectId(name=select_id, base=base)


class FilterId(ConqueryId):
    def __init__(self, name: str, base: ConnectorId):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> ConnectorId:
        return self._base

    @base.setter
    def base(self, new_base: ConnectorId):
        if not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Filter can only be a Connector")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self):
        return self.base.get_concept_id()

    def get_connector_id(self) -> str:
        return self.base.get_connector_id()

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        label_dict = self.base.add_self_label_to_dict(label_dict=label_dict, concept_obj=concept_obj)

        table_filters = self._filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.label,
                                                 compare_string=label_dict['connector'], return_key=Keys.filters)

        filter_label = self._filter_concept_obj(filter_obj=table_filters, filter_key=Keys.id,
                                                compare_string=self.id, return_key=Keys.label)

        label_dict["filter"] = filter_label
        return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> FilterId:
        if not len(id_list) == filter_index:
            raise ValueError(f"Provided string for Filter must be of length {filter_index} "
                             f"(dataset, concept, connector and filter). "
                             f"Provided: {id_list}")
        filter_id = id_list.pop(-1)
        base = ConnectorId.create_id_objects_recursively(id_list=id_list)
        return FilterId(name=filter_id, base=base)


class DateId(ConqueryId):
    def __init__(self, name: str, base: ConnectorId):
        super().__init__(name=name, base=base)

    @property
    def base(self) -> ConnectorId:
        return self._base

    @base.setter
    def base(self, new_base: ConnectorId):
        if not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Date can only be a Connector")
        self._base = new_base

    def change_dataset(self, new_dataset: str):
        self.base.change_dataset(new_dataset=new_dataset)

    def get_concept_id(self):
        return self.base.get_concept_id()

    def get_connector_id(self) -> str:
        return self.base.get_connector_id()

    def add_self_label_to_dict(self, label_dict: dict, concept_obj: dict) -> dict:
        label_dict = self.base.add_self_label_to_dict(label_dict=label_dict, concept_obj=concept_obj)

        table_dates = self._filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.label,
                                               compare_string=label_dict['connector'], return_key=Keys.date_column)

        date_label = self._filter_concept_obj(filter_obj=table_dates[Keys.options], filter_key=Keys.value,
                                              compare_string=self.id, return_key=Keys.label)

        label_dict["date"] = date_label
        return label_dict

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> DateId:
        if not len(id_list) == date_index:
            raise ValueError(f"Provided string for Date must be of length {date_index} "
                             f"(dataset, concept, connector and date). "
                             f"Provided: {id_list}")
        date_id = id_list.pop(-1)
        base = ConnectorId.create_id_objects_recursively(id_list=id_list)
        return DateId(name=date_id, base=base)


class ConqueryIdCollection:
    def __init__(self, conquery_ids: Set[ConqueryId] = None):
        if conquery_ids is None:
            self.conquery_ids: Set[ConqueryId] = set()
        else:
            self.conquery_ids = conquery_ids

    def __eq__(self, other):
        if isinstance(other, ConqueryIdCollection):
            return self.conquery_ids == other.conquery_ids
        raise NotImplementedError

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
