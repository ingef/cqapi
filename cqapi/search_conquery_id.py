from __future__ import annotations
from cqapi.conquery_ids import ConqueryId, ConceptId, ChildId
from typing import List, Union
from typeguard import typechecked


@typechecked()
def id_to_label_list(eva_ids: Union[List[str], str], concepts: dict, eva_id_type: str):
    if isinstance(eva_ids, str):
        eva_ids = [eva_ids]
    return [id_to_label(eva_id, concepts, eva_id_type) for eva_id in eva_ids]


@typechecked()
def id_to_label(eva_id: str, concepts: dict, eva_id_type: str):
    eva_id_type = eva_id_type.lower()
    eva_conquery_id = ConqueryId.from_str(id_string=eva_id, type_hint=eva_id_type)

    root_concept_id = eva_conquery_id.get_concept_id()
    concept_obj = concepts[root_concept_id]
    root_concept_label = concept_obj.get('label')

    if eva_id_type == "concept":
        return root_concept_label

    if eva_id_type == "child":
        eva_id_list = eva_conquery_id.id.split('.')
        return " - ".join([root_concept_label, *[eva_id_list[2:]]])

    # check concept selects
    if eva_id_type == "concept_select":
        select_obj = [_ for _ in concept_obj.get('selects') if _.get('id') == eva_id]
        if len(select_obj) > 1:
            raise ValueError(f"Found more than select with id {eva_id}: \n"
                             f"{concept_obj}")
        if len(select_obj) == 1:
            return " - ".join([root_concept_label, select_obj[0].get('label')])

    connector_id = eva_conquery_id.get_connector_id()
    table_obj = [table for table in concept_obj.get('tables')
                 if table.get('connectorId') == connector_id]
    if len(table_obj) == 0:
        raise ValueError(f"Unknown eva_id {eva_id}")
    table_obj = table_obj[0]
    table_label = table_obj.get('label')
    if eva_id_type == "connector":
        return " - ".join([root_concept_label, table_label])

    # check table selects
    if eva_id_type == "select":
        select_obj = [_ for _ in table_obj.get('selects') if _.get('id') == eva_id]
        if len(select_obj) == 0:
            raise ValueError(f"Unknown eva_id {eva_id}")
        return " - ".join([root_concept_label, table_label, select_obj[0].get('label')])

    if eva_id_type == "filter":
        filter_obj = [_ for _ in table_obj.get('filters') if _.get('id') == eva_id]
        if len(filter_obj) == 0:
            raise ValueError(f"Unknown eva_id {eva_id}")
        filter_obj = filter_obj[0]
        return " - ".join([root_concept_label, table_label, filter_obj.get('label')])

    # eva_id_types selects and filter can be added here
    raise ValueError(f"Unknown eva_id_type {eva_id_type}")


@typechecked()
def find_concept_id(concept_id: ConqueryId, concepts: dict, children_ids: List[ChildId, ConceptId]):
    """
    Searches for conquery_id in concepts or concept_obj. If concept_id is found True is returned.
    If eva access data is defined, concept_obj is loaded for concept_id level 3 or higher

    :param concept_id: concept_id to search for
    :param concepts: dict with concept_ids
    :param children_ids: List of all children to look for
    """

    root_concept_id = concept_id.get_concept_id()

    if isinstance(concept_id, ConceptId):
        if root_concept_id.id in concepts.keys():
            return True
        return False

    if isinstance(concept_id, ChildId) and isinstance(concept_id.base, ConceptId):
        # children of root concepts can be looked up in concepts object
        if concept_id.id in concepts[root_concept_id.id].get("children", []):
            return True
        return False

    return concept_id in children_ids
