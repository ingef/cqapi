from __future__ import annotations
from cqapi.util import check_arg_type
from cqapi.conquery_ids import get_root_concept_id, get_connector_id
from typing import List


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
