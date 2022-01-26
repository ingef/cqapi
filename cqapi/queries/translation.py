from typing import Union, Tuple, List

from cqapi.api import ConqueryConnection
from cqapi.conquery_ids import ConqueryIdCollection, ConceptId, ChildId, get_copy_of_id_with_changed_dataset, \
    get_dataset_from_id_string
from cqapi.exceptions import QueryTranslationError
from cqapi.namespace import Keys
from cqapi.queries.base_elements import create_query_obj, QueryObject


def translate_query(query: Union[QueryObject, dict], concepts: dict, conquery_conn: ConqueryConnection,
                    return_removed_ids: bool = False) -> \
        Union[Tuple[Union[QueryObject, dict, None], Union[QueryObject, dict, None], ConqueryIdCollection],
              Tuple[Union[QueryObject, dict, None], Union[QueryObject, dict, None]]]:
    new_dataset = get_dataset_from_id_string(next(iter(concepts)))

    # translate
    conquery_ids = ConqueryIdCollection()

    # get children ids that exist
    all_concept_ids = query.get_concept_ids()
    # don't ask for children concepts for concepts that are not available for new dataset
    concept_ids = list()
    for concept_id in all_concept_ids:
        new_concept_id = concept_id.get_concept_id()
        get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=new_concept_id)
        if new_concept_id.id in concepts.keys():
            concept_ids.append(concept_id)

    children_ids = check_concept_ids_in_concepts_for_new_dataset(concept_ids=concept_ids,
                                                                 new_dataset=new_dataset,
                                                                 conquery_conn=conquery_conn)

    new_query, query = query.translate(concepts=concepts, removed_ids=conquery_ids, children_ids=children_ids)

    if return_removed_ids:
        return new_query, query, conquery_ids

    return new_query, query


def translate_queries(queries: List[QueryObject], concepts: dict, conquery_conn: ConqueryConnection,
                      return_removed_ids: bool = False) -> \
        Union[Tuple[List[QueryObject], List[QueryObject], ConqueryIdCollection],
              Tuple[List[QueryObject], List[QueryObject]]]:
    new_queries = list()
    remaining_queries = list()
    removed_ids = ConqueryIdCollection()
    for query in queries:
        new_query, remaining_query, removed_ids_query = translate_query(query=query,
                                                                        concepts=concepts,
                                                                        conquery_conn=conquery_conn,
                                                                        return_removed_ids=True)
        removed_ids.update(removed_ids_query)
        if new_query is not None:
            new_queries.append(new_query)
            remaining_queries.append(remaining_query)

    if return_removed_ids:
        return new_queries, remaining_queries, removed_ids

    return new_queries, remaining_queries


def translate_and_execute_stored_query(query_id: str, new_dataset: str, conquery_conn: ConqueryConnection,
                                       concepts_new_dataset: dict = None, return_removed_ids: bool = False) -> \
        Union[Tuple[str, ConqueryIdCollection], str]:
    """Gets stored query, translates it to new dataset and executes the query."""

    if concepts_new_dataset is None:
        concepts_new_dataset = conquery_conn.get_concepts(dataset=new_dataset)

    # translate queryGroup
    original_query = conquery_conn.get_query(query_id)
    original_query = create_query_obj(original_query)

    translated_query, original_query, removed_ids = \
        translate_query(query=original_query,
                        concepts=concepts_new_dataset,
                        conquery_conn=conquery_conn,
                        return_removed_ids=True)

    if translated_query is None:
        raise QueryTranslationError()
    new_query_id = conquery_conn.execute_query(translated_query, dataset=new_dataset)

    if return_removed_ids:
        return new_query_id, removed_ids

    return new_query_id


def check_concept_ids_in_concepts_for_new_dataset(concept_ids: List[Union[ConceptId, ChildId]],
                                                  new_dataset: str, conquery_conn: ConqueryConnection):
    """
    For each concept_id in concept_ids it checks if the concept_id exist in the concept-object of the new dataset.
    This ist needed for translating children concepts that are on level 3 or higher
    :param concept_ids:
    :param new_dataset:
    :param conquery_conn:
    :return:
    """

    # group concept_ids by root_concept_id
    concept_ids_dict = dict()
    for concept_id in concept_ids:
        root_concept_id = concept_id.get_concept_id()
        concept_ids_dict[root_concept_id] = [concept_id, *concept_ids_dict.get(root_concept_id, [])]

    # for each root concept_id get the concept and check if concept_ids are in there
    children_ids = []
    for root_concept_id, child_concept_ids in concept_ids_dict.items():
        new_root_concept_id = get_copy_of_id_with_changed_dataset(new_dataset=new_dataset, conquery_id=root_concept_id)
        new_child_concept_ids = [get_copy_of_id_with_changed_dataset(new_dataset=new_dataset,
                                                                     conquery_id=child_concept_id)
                                 for child_concept_id in child_concept_ids]

        concept = conquery_conn.get_concept(new_root_concept_id.id)
        concept_ids_in_concept = [child_id for child in concept for child_id in child[Keys.ids]]

        children_ids.extend([child_concept_id for child_concept_id in new_child_concept_ids
                             if child_concept_id.id in concept_ids_in_concept])

    return children_ids
