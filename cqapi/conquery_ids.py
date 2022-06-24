from __future__ import annotations
from typing import List, Union, Set, Optional
from abc import ABC, abstractmethod
from cqapi.namespace import Keys
from copy import deepcopy

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
    def __init__(self, name: str, base: Optional[ConqueryId] = None):
        if type(self) == ConqueryId:
            raise ValueError("Only subclasses of ConqueryId can be initiated")

        self._base: Optional[ConqueryId] = base
        self.name = name

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other_id):
        if isinstance(other_id, ConqueryId):
            return self.id == other_id.id
        raise ValueError("Can only compare to another instance of ConqueryId or its subclasses")

    def __repr__(self):
        return self.id

    def deepcopy(self) -> ConqueryId:
        return deepcopy(self)

    @property
    def id(self):
        """
        Datasets returns its name, all other Ids that build on it add their name to the string.
        """
        if not isinstance(self, DatasetId):
            return f"{self.base.id}{conquery_id_separator}{self.name}"
        else:
            return self.name

    @property
    def base(self) -> ConqueryId:
        """
        Getter for base
        """
        if isinstance(self._base, ConqueryId):
            return self._base

        raise ValueError(f"Cannot retrieve base")

    @base.setter
    def base(self, new_base: Optional[ConqueryId]):
        self._check_valid_base(new_base=new_base)
        self._base = new_base

    @abstractmethod
    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        pass

    def get_concept_id(self) -> ConceptId:
        """
        Returns ConceptId instance of ConqueryId
        """
        if isinstance(self, DatasetId):
            raise ValueError("Cannot get concept id for dataset")
        elif isinstance(self, ConceptId):
            return self
        else:
            return self.base.get_concept_id()

    def rename_concept_id(self, new_concept: str):
        """
        Renames the concept_id base of a ConqueryId
        """
        if isinstance(self, DatasetId):
            raise ValueError("Cannot rename concept for dataset")
        elif isinstance(self, ConceptId):
            self.name = new_concept
        else:
            self.base.rename_concept_id(new_concept=new_concept)

    def get_connector_id(self) -> ConnectorId:
        """
        Returns ConnectorId instance of ConqueryId
        """
        if isinstance(self, DatasetId) or isinstance(self, ConceptId):
            raise ValueError(f"Cannot get connector id for {type(self)}")
        elif isinstance(self, ConnectorId):
            return self
        else:
            return self.base.get_connector_id()

    def get_dataset(self) -> str:
        """
        Get string value of dataset
        """
        if isinstance(self, DatasetId):
            return self.name
        else:
            return self.base.get_dataset()

    def change_dataset(self, new_dataset: str):
        """
        Change the name of the base dataset
        """
        if isinstance(self, DatasetId):
            self.name = new_dataset
        else:
            self.base.change_dataset(new_dataset=new_dataset)

    def is_in_id_list(self, other_ids: List[ConqueryId]) -> bool:
        return any(self == other_id for other_id in other_ids)

    def is_child_of(self, other_id: ConqueryId) -> bool:
        return self.id.startswith(other_id.id)

    def is_child_of_any(self, other_ids: List[ConqueryId]) -> bool:
        return any(self.is_child_of(other_id=other_id) for other_id in other_ids)

    @abstractmethod
    def get_id_label(self, concepts: dict) -> str:
        pass

    @classmethod
    @abstractmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ConqueryId:
        """
        Method is used in from_str and should validate provided string and recursively initiate objects for each
        inherited class needed for the provided Id
        """
        raise ValueError("Without type hint, from_str can only be called on subclasses of ConqueryId")

    @classmethod
    def from_str(cls, id_string: str, type_hint: str = None) -> ConqueryId:
        """
        Splits the string on the separator and initiates instances of conqueryIds to represent the string.
        If type hint provided, calls from string method on the corresponding subclass
        """
        if not type_hint:
            id_list = id_string.split(conquery_id_separator)
            return cls.create_id_objects_recursively(id_list=id_list)
        if type_hint == "dataset":
            return DatasetId.from_str(id_string=id_string)
        elif type_hint == "concept":
            return ConceptId.from_str(id_string=id_string)
        elif type_hint == "connector":
            return ConnectorId.from_str(id_string=id_string)
        elif type_hint == "child":
            return ChildId.from_str(id_string=id_string)
        elif type_hint == "select" or type_hint == "concept_select" or type_hint == "connector_select":
            return SelectId.from_str(id_string=id_string)
        elif type_hint == "filter":
            return FilterId.from_str(id_string=id_string)
        elif type_hint == "date":
            return DateId.from_str(id_string=id_string)
        else:
            raise ValueError(f"Invalid type hint, provided: {type_hint}")

    @staticmethod
    def filter_concept_obj(filter_obj: list, filter_key: str, compare_string: str, return_key: str) \
            -> Union[str, list]:
        """
        Method used in add_self_label_to_dict in subclasses.
        Filters a list of dictionaries (filter_obj) based on if for each element, the value of the dict for filter_key
        equals the compare_string. If so, return the value of the dict of return_key
        """
        return [element[return_key] for element in filter_obj if element[filter_key] == compare_string][0]


class DatasetId(ConqueryId):
    def __init__(self, name: str):
        super().__init__(name=name, base=None)

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if new_base:
            raise ValueError("Dataset cannot have base")

    def get_id_label(self, concepts: dict):
        return self.name.upper()

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> DatasetId:
        if len(id_list) != dataset_index:
            raise ValueError(f"Provided list of ids for Dataset must be of length {dataset_index}. "
                             f"Provided: {id_list}")
        return DatasetId(name=id_list[0])


class ConceptId(ConqueryId):
    def __init__(self, name: str, base: DatasetId):
        super().__init__(name=name, base=base)

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Concept cannot be None")
        if not isinstance(new_base, DatasetId):
            raise ValueError(f"Base of Concept can only be a Dataset. Provided: {new_base.id}")

    def get_id_label(self, concepts: dict):
        concept_id = self.id
        concept_obj = concepts[concept_id]
        concept_label = concept_obj[Keys.label]
        return concept_label

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

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Connector cannot be None")
        if not isinstance(new_base, ConceptId):
            raise ValueError(f"Base of Connector can only be a Concept. Provided: {new_base}")

    def get_id_label(self, concepts: dict):
        base_label = self.base.get_id_label(concepts=concepts)
        concept_id = self.get_concept_id().id
        concept_obj = concepts[concept_id]
        connector_id = self.id
        connector_label = self.filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.connector_id,
                                                  compare_string=connector_id, return_key=Keys.label)
        return " - ".join([base_label, connector_label])

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

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Child cannot be None")
        if not isinstance(new_base, ConceptId) and not isinstance(new_base, ChildId):
            raise ValueError("Base of Child can only be a Concept or Child")

    def get_id_label(self, concepts: dict):
        base_label = self.get_concept_id().get_id_label(concepts=concepts)
        child_label = self.name.upper()
        return " - ".join([base_label, child_label])

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> ChildId:
        child_id = id_list.pop(-1)
        if len(id_list) >= child_index:
            base = ChildId.create_id_objects_recursively(id_list=id_list)  # type: ignore
        elif len(id_list) == concept_index:
            base = ConceptId.create_id_objects_recursively(id_list=id_list)  # type: ignore
        else:
            raise ValueError(f"Provided string for Child must be of minimum length {child_index + 1} "
                             f"(dataset, concept and child/children). "
                             f"Provided: {id_list}")

        return ChildId(name=child_id, base=base)


class SelectId(ConqueryId):
    def __init__(self, name: str, base: Union[ConceptId, ConnectorId]):
        super().__init__(name=name, base=base)

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Select cannot be None")
        if not isinstance(new_base, ConceptId) and not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Select can only be a Concept or Connector")

    def get_id_label(self, concepts: dict):
        base_label = self.base.get_id_label(concepts=concepts)
        concept_id = self.get_concept_id().id
        concept_obj = concepts[concept_id]

        if isinstance(self.base, ConnectorId):
            connector_id = self.get_connector_id().id

            table_selects = self.filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.connector_id,
                                                    compare_string=connector_id, return_key=Keys.selects) \
                # type: ignore
            select_label = self.filter_concept_obj(filter_obj=table_selects, filter_key=Keys.id,
                                                   compare_string=self.id, return_key=Keys.label)  # type: ignore

        else:
            select_label = self.filter_concept_obj(filter_obj=concept_obj[Keys.selects], filter_key=Keys.id,
                                                   compare_string=self.id, return_key=Keys.label)  # type: ignore
        return " - ".join([base_label, select_label])

    @classmethod
    def create_id_objects_recursively(cls, id_list: List[str]) -> SelectId:
        select_id = id_list.pop(-1)
        if len(id_list) == concept_index:
            base = ConceptId.create_id_objects_recursively(id_list=id_list)  # type: ignore
        elif len(id_list) == connector_index:
            base = ConnectorId.create_id_objects_recursively(id_list=id_list)  # type: ignore
        else:
            raise ValueError(f"Provided string for Select must be of length {concept_select_index} or "
                             f"{connector_select_index} (dataset, concept, (connector) and select. "
                             f"Provided: {id_list}")

        return SelectId(name=select_id, base=base)


class FilterId(ConqueryId):
    def __init__(self, name: str, base: ConnectorId):
        super().__init__(name=name, base=base)

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Filter cannot be None")
        if not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Filter can only be a Connector")

    def get_id_label(self, concepts: dict):
        base_label = self.base.get_id_label(concepts=concepts)
        concept_id = self.get_concept_id().id
        concept_obj = concepts[concept_id]
        connector_id = self.get_connector_id().id

        table_filters = self.filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.connector_id,
                                                compare_string=connector_id, return_key=Keys.filters) \
            # type: ignore
        filter_label = self.filter_concept_obj(filter_obj=table_filters, filter_key=Keys.id,
                                               compare_string=self.id, return_key=Keys.label)  # type: ignore
        return " - ".join([base_label, filter_label])

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

    def _check_valid_base(self, new_base: Optional[ConqueryId]):
        if not new_base:
            raise ValueError("Base of Date cannot be None")
        if not isinstance(new_base, ConnectorId):
            raise ValueError("Base of Date can only be a Connector")

    def get_id_label(self, concepts: dict):
        base_label = self.base.get_id_label(concepts=concepts)
        concept_id = self.get_concept_id().id
        concept_obj = concepts[concept_id]
        connector_id = self.get_connector_id().id

        table_dates = self.filter_concept_obj(filter_obj=concept_obj[Keys.tables], filter_key=Keys.connector_id,
                                              compare_string=connector_id, return_key=Keys.date_column) \
            # type: ignore

        date_label = self.filter_concept_obj(filter_obj=table_dates[Keys.options], filter_key=Keys.value,
                                             compare_string=self.id, return_key=Keys.label)  # type: ignore
        return " - ".join([base_label, date_label])

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
        # TODO implement
        pass

    def print_id_labels_as_table(self, concepts: dict):
        # TODO implement
        pass


def get_copy_of_id_with_changed_dataset(new_dataset: str, conquery_id: ConqueryId):
    """
    Creates a copy and then changes the dataset and returns the new ConqueryId
    """
    new_conquery_id = conquery_id.deepcopy()
    new_conquery_id.change_dataset(new_dataset=new_dataset)
    return new_conquery_id


def get_dataset_from_id_string(id_string: str) -> str:
    """
    From an id representation of a ConqueryId, retrieve the dataset name.
    """
    return id_string.split(conquery_id_separator)[0]


def get_concept_id_from_id_string(id_string: str) -> str:
    """
    From an id representation of a ConqueryId, retrieve the dataset name.
    """
    return conquery_id_separator.join([id_string.split(conquery_id_separator)[0],
                                       id_string.split(conquery_id_separator)[1]])


def get_root_concept_name(id_string: str) -> str:
    return id_string.split(conquery_id_separator)[1]
