from typing import List, Union, Tuple
from cqapi.conquery_ids import get_root_concept_id, ConqueryIdCollection, get_dataset, change_dataset
from cqapi.namespace import Keys
from cqapi.queries.elements import QueryObject, ConceptElement, DateRestriction, ConceptQuery, convert_query_dict
from cqapi import ConqueryConnection


def create_query(concept_id: Union[str, List[str]], concepts: dict, concept_query: bool = False, connector_ids: List[str] = None,
                 concept_select_ids: List[str] = None, connector_select_ids: List[str] = None,
                 filter_objs: List[dict] = None,
                 exclude_from_secondary_id: bool = None, exclude_from_time_aggregation: bool = None,
                 date_aggregation_mode: str = None,
                 start_date: str = None, end_date: str = None,
                 label: str = None) -> QueryObject:

    if isinstance(concept_id, list):
        root_concept_id = get_root_concept_id(concept_id[0])
        concept_ids = concept_id
    elif isinstance(concept_id, str):
        root_concept_id = get_root_concept_id(concept_id)
        concept_ids = [concept_id]
    else:
        raise ValueError(f"{concept_id=} must be of type List[str] or str")

    query = ConceptElement(ids=concept_ids, concept=concepts[root_concept_id],
                           connector_ids=connector_ids,
                           concept_selects=concept_select_ids,
                           connector_selects=connector_select_ids,
                           filter_objs=filter_objs,
                           exclude_from_secondary_id=exclude_from_secondary_id,
                           exclude_from_time_aggregation=exclude_from_time_aggregation,
                           label=label
                           )

    if start_date is not None or end_date is not None:
        query = DateRestriction(child=query, start_date=start_date, end_date=end_date)

    if concept_query:
        return ConceptQuery(root=query, date_aggregation_mode=date_aggregation_mode)

    return query


def translate_query(query: QueryObject, concepts: dict, conquery_conn: ConqueryConnection,
                    return_removed_ids: bool = False) -> \
        Union[Tuple[Union[QueryObject, None], Union[QueryObject, None], ConqueryIdCollection],
              Tuple[Union[QueryObject, None], Union[QueryObject, None]]]:
    new_dataset = get_dataset(next(iter(concepts)))

    # translate
    conquery_ids = ConqueryIdCollection()

    # get children ids that exist
    concept_ids = query.get_concept_ids()
    # don't ask for children concepts for concepts that are not available for new dataset
    concept_ids = [concept_id for concept_id in concept_ids if get_root_concept_id(concept_id) in concepts.keys()]

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
    original_query = convert_query_dict(original_query)

    translated_query, original_query, removed_ids = \
        translate_query(query=original_query,
                        concepts=concepts_new_dataset,
                        conquery_conn=conquery_conn,
                        return_removed_ids=True)

    new_query_id = conquery_conn.execute_query(translated_query, dataset=new_dataset)

    if return_removed_ids:
        return new_query_id, removed_ids

    return new_query_id

def check_concept_ids_in_concepts_for_new_dataset(concept_ids: List[str],
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
        root_concept_id = get_root_concept_id(concept_id)
        concept_ids_dict[root_concept_id] = [concept_id, *concept_ids_dict.get(root_concept_id, [])]

    # for each root concept_id get the concept and check if concept_ids are in there
    children_ids = []
    for root_concept_id, child_concept_ids in concept_ids_dict.items():
        new_root_concept_id = change_dataset(new_dataset=new_dataset, conquery_id=root_concept_id)
        new_child_concept_ids = [change_dataset(new_dataset=new_dataset,
                                                conquery_id=child_concept_id)
                                 for child_concept_id in child_concept_ids]

        concept = conquery_conn.get_concept(new_root_concept_id)
        concept_ids_in_concept = [child_id for child in concept for child_id in child[Keys.ids]]

        children_ids.extend([child_concept_id for child_concept_id in new_child_concept_ids
                             if child_concept_id in concept_ids_in_concept])

    return children_ids
