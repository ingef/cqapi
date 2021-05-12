from copy import deepcopy
from cqapi.util import check_input_list
from cqapi.queries.validate import validate_date
from cqapi.conquery_ids import is_same_conquery_id, is_in_conquery_ids, get_dataset, contains_dataset_id, \
    add_dataset_id_to_conquery_id

cq_element_description = {
    "base_cq_elements": ["CONCEPT", "PERIOD_CONCEPT", "SAVED_QUERY"],
    "cq_element_collections": ["AND", "OR"],
    "cq_element_wrap": ["DATE_RESTRICTION", "NEGATION", "CONCEPT_QUERY"]
}
cq_elements = [__ for _ in cq_element_description.values() for __ in _]


def get_label_from_query(query: dict):
    """Returns label from query. If there are more than one child, only the label of the first child is returned"""
    if query["type"] in cq_element_description["base_cq_elements"]:
        return query.get('label', '')
    if 'root' in query.keys():
        return query.get('root').get('label')
    elif 'children' in query.keys():
        return query.get('children')[0].get('label')
    else:
        raise ValueError(f"Could not find 'root' or 'children' objects in query")


def return_labels_from_queries(queries: list):
    return [get_label_from_query(query) for query in queries]


def get_selects_from_query(query: dict) -> list:
    """Returns list of all selects in query (on concept and table level)"""
    selects = []
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    if query.get("type") == "CONCEPT_QUERY":
        return get_selects_from_query(query.get("root"))

    if query.get("type") == "DATE_RESTRICTION":
        return get_selects_from_query(query.get("child"))

    if query.get("type") in ["AND", "OR"]:
        for child in query.get("children"):
            selects.extend(get_selects_from_query(child))
        return selects

    if query.get("type") == "CONCEPT":
        selects.extend(query.get("selects", []))
        for table in query.get("tables"):
            selects.extend(table.get("selects"))

        return selects

    raise ValueError(f"Unknown query type {query.get('type')}")


def remove_selects_from_query(query: dict, selects_to_keep: list = None) -> dict:
    """Check tables and concepts selects and removes them if they are not specified in selects_to_keep"""
    if selects_to_keep is None:
        selects_to_keep = []
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    if query.get("type") == "CONCEPT_QUERY":
        query["root"] = remove_selects_from_query(query.get("root"), selects_to_keep)
        return query

    if query.get("type") == "DATE_RESTRICTION":
        query["child"] = remove_selects_from_query(query.get("child"), selects_to_keep)
        return query

    if query.get("type") in ["AND", "OR"]:
        query["children"] = [remove_selects_from_query(query_child, selects_to_keep) for query_child in
                             query.get("children", [])]
        return query

    if query.get("type") == "CONCEPT":
        # check for selects in tables
        for table in query.get('tables', []):
            table["selects"] = [select for select in table.get("selects", []) if select in selects_to_keep]
        # check for concept select
        query["selects"] = [select for select in query.get("selects", []) if select in selects_to_keep]

        return query

    raise ValueError(f"Unknown query type {query.get('type')}")


def separate_query_by_selects(query: dict) -> list:
    """Searches for selects in query and creates own query for each select.
    Only works for simple OR/AND queries that have only one concept query as children
    KEEP IN MIND: If extending functionality to more than one child or other query types, keep in mind that selects
    do not need to be unique accross different concept queries """
    simple_queries = []
    query_contains_select = False
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    if query.get("type") in ["AND", "OR"]:
        query_children = query.get("children", [])
        if not query_children:
            raise ValueError(f"Could not find key 'children' or there are no childrens in query {query}")
        if len(query_children) > 1:
            raise ValueError(f"Can only extract selects of queries with only one child. Query: {query}")

        query_child = query_children[0]

        # check for selects in tables
        for table in query_child.get('tables', []):
            for table_select in table.get("selects", []):
                query_contains_select = True
                simple_queries.append(remove_selects_from_query(deepcopy(query), selects_to_keep=table_select))
            table["selects"] = []

        # check for concept selects
        for concept_select in query_child.get("selects", []):
            query_contains_select = True
            simple_queries.append(remove_selects_from_query(deepcopy(query), selects_to_keep=concept_select))

        # when there are no selects, return query since it has default select
        if not query_contains_select:
            if len(simple_queries) > 0:
                raise Exception(f"Inconsistent state: No query found but simple queries have been created.")
            return [query]

    return simple_queries


def separate_queries_by_selects(queries: list) -> list:
    simple_queries = []
    for query in queries:
        simple_queries.extend(separate_query_by_selects(query))
    return simple_queries


def wrap_saved_query(query_id: str) -> dict:
    return {
        'type': 'SAVED_QUERY',
        'query': query_id
    }


def wrap_negation(query: dict) -> dict:
    return {
        "type": "NEGATION",
        "child": query
    }


def wrap_secondary_id_query(query: dict, secondary_id: str, date_aggregation_mode: str = "MERGE"):
    if query["type"] in ["CONCEPT_QUERY"]:
        query = query.get("root")
    return {
        "type": "SECONDARY_ID_QUERY",
        "secondaryId": secondary_id,
        "root": query,
        "dateAggregationMode": date_aggregation_mode
    }


def unwrap_secondary_id_query(concept_query: dict) -> dict:
    if concept_query["type"] != "SECONDARY_ID_QUERY":
        return concept_query
    return concept_query["root"]


def add_date_restriction(query: dict, start_date: str = None, end_date: str = None) -> dict:
    date_range_obj = dict()

    if start_date is not None:
        validate_date(start_date)
        date_range_obj["min"] = start_date

    if end_date is not None:
        validate_date(end_date)
        date_range_obj["max"] = end_date

    if start_date is None and end_date is None:
        raise ValueError(f"No date specified")

    return {
        "type": "DATE_RESTRICTION",
        "dateRange": date_range_obj,
        "child": query
    }


def unwrap_concept_query(concept_query: dict) -> dict:
    if concept_query["type"] != "CONCEPT_QUERY":
        return concept_query
    return concept_query["root"]


def wrap_concept_query(query: dict, date_aggregation_mode: str = "MERGE") -> dict:
    return {
        "type": "CONCEPT_QUERY",
        "root": query,
        "dateAggregationMode": date_aggregation_mode
    }


def wrap_and(*queries: dict) -> dict:
    return {
        'type': 'AND',
        'children': [*queries]
    }


def wrap_or(*queries) -> dict:
    return {
        'type': 'OR',
        'children': [*queries]
    }


def keep_connectors_from_queries(queries: list, connectors: list):
    new_queries = []
    for query in queries:
        new_queries.append(keep_connectors_from_query(query, connectors))
    return new_queries


def keep_connectors_from_query(query: dict, connectors: list):
    concept_elements = get_concept_elements_from_query(query)
    for concept_element in concept_elements:
        if "tables" not in concept_element.keys():
            continue
        concept_element["tables"] = \
            [table for table in concept_element["tables"]
             if any([is_same_conquery_id(table.get("id"), connector) for connector in connectors])]

        if not concept_element["tables"]:
            raise ValueError("Connector can not be removed, since it is the only one of concept element: \n"
                             f"{concept_element}")

    return query


def remove_connectors_from_queries(queries: list, connectors: list):
    new_queries = []
    for query in queries:
        new_queries.append(remove_connectors_from_query(query, connectors))
    return new_queries


def remove_connectors_from_query(query: dict, connectors: list):
    concept_elements = get_concept_elements_from_query(query)
    for concept_element in concept_elements:
        if "tables" not in concept_element.keys():
            continue
        concept_element["tables"] = \
            [table for table in concept_element["tables"]
             if not any([is_same_conquery_id(table.get("id"), connector) for connector in connectors])]

        if not concept_element["tables"]:
            raise ValueError("Connector can not be removed, since it is the only one of concept element: \n"
                             f"{concept_element}")

    return query


def get_concept_elements_from_query(query: dict) -> list:
    concept_elements = []
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    query_type = query.get("type")

    if query_type in ["CONCEPT_QUERY", "SECONDARY_ID_QUERY"]:
        concept_elements.extend(get_concept_elements_from_query(query.get("root")))
        return concept_elements

    if query_type in ["DATE_RESTRICTION", "NEGATION"]:
        concept_elements.extend(get_concept_elements_from_query(query.get("child")))
        return concept_elements

    if query_type in ["AND", "OR"]:
        for query_child in query.get("children", []):
            concept_elements.extend(get_concept_elements_from_query(query_child))
        return concept_elements

    if query_type == "SAVED_QUERY":
        return []

    if query.get("type") == "CONCEPT":
        return [query]

    raise ValueError(f"Unknown Type {query_type}")


def add_matching_type_to_query(query: dict, matching_type: str, concept_id: str = None):
    concept_elements = get_concept_elements_from_query(query)

    for concept_element in concept_elements:
        if concept_id is not None and not is_in_conquery_ids(concept_id, concept_element["ids"]):
            continue

        concept_element["matchingType"] = matching_type


def add_filter_to_query(query: dict, filter_id: str, filter_obj: dict,
                        concept_id: str = None, connector_id: str = None) -> dict:
    return _add_to_table_in_query(query=query,
                                  conquery_id=filter_id,
                                  conquery_id_type="filters",
                                  concept_id=concept_id,
                                  connector_id=connector_id,
                                  filter_obj=filter_obj)


def add_connector_select_to_query(query: dict, select_id: str, concept_id: str = None,
                                  connector_id: str = None) -> dict:
    return _add_to_table_in_query(query=query,
                                  conquery_id=select_id,
                                  conquery_id_type="selects",
                                  concept_id=concept_id,
                                  connector_id=connector_id)


def add_connector_select_to_queries(queries: list, select_id: str, concept_id: str = None,
                                    connector_id: str = None) -> list:
    output_queries = list()
    for query in queries:
        output_queries.append(add_connector_select_to_query(query=deepcopy(query),
                                                            select_id=select_id,
                                                            concept_id=concept_id,
                                                            connector_id=connector_id))

    return output_queries


def add_concept_select_to_query(query: dict, select_id: str, concept_id: str = None) -> dict:
    concept_elements = get_concept_elements_from_query(query)

    for concept_element in concept_elements:

        if not contains_dataset_id(select_id):
            select_id = add_dataset_id_to_conquery_id(select_id, get_dataset(concept_element['ids'][0]))

        if concept_id is not None and not is_in_conquery_ids(concept_id, concept_element['ids']):
            continue

        if 'selects' in concept_element.keys():
            if is_in_conquery_ids(select_id, concept_element['selects']):
                continue
            concept_element['selects'] = [*concept_element['selects'], select_id]
        else:
            concept_element['selects'] = [select_id]

    return query


def add_validity_date_to_query(query: dict, validity_date_id: str, concept_id: str = None,
                               connector_id: str = None) -> dict:
    return _add_to_table_in_query(query=query,
                                  conquery_id=validity_date_id,
                                  conquery_id_type="dateColumn",
                                  concept_id=concept_id,
                                  connector_id=connector_id)


def add_validity_date_to_queries(queries: list, validity_date_id: str, concept_id: str = None,
                                 connector_id: str = None) -> list:
    return [add_validity_date_to_query(query=query, validity_date_id=validity_date_id,
                                       concept_id=concept_id, connector_id=connector_id) for query in queries]


def _add_to_table_in_query(query: dict, conquery_id: str, conquery_id_type: str,
                           concept_id: str = None, connector_id: str = None, filter_obj: dict = None) -> dict:
    concept_elements = get_concept_elements_from_query(query)
    for concept_element in concept_elements:
        # skip concept elements that do not match concept_id
        if concept_id is not None and not is_in_conquery_ids(concept_id, concept_element['ids']):
            continue

        for table in concept_element["tables"]:
            table_id = table["id"]
            # table element that do not match connector_id
            if connector_id is not None and not is_same_conquery_id(connector_id, table_id):
                continue

            if not contains_dataset_id(conquery_id):
                conquery_id = add_dataset_id_to_conquery_id(conquery_id, get_dataset(table_id))

            if conquery_id_type == "dateColumn":
                table[conquery_id_type] = {"value": conquery_id}
            elif conquery_id_type == "selects":
                # avoid duplicates
                if conquery_id in table.get(conquery_id_type, []):
                    continue
                table[conquery_id_type] = [*table.get(conquery_id_type, []), conquery_id]
            elif conquery_id_type == "filters":
                if filter_obj is None:
                    raise ValueError(f"{conquery_id_type=} but {filter_obj=}")

                if not contains_dataset_id(filter_obj["filter"]):
                    filter_obj["filter"] = add_dataset_id_to_conquery_id(filter_obj["filter"], get_dataset(table_id))

                table["filters"] = [*table.get("filters", []), filter_obj]

            else:
                raise ValueError(f"Unknown {conquery_id_type=}")

    return query


def exclude_from_secondary_id(query: dict, concept_id: str = None):
    concept_elements = get_concept_elements_from_query(query)
    for concept_element in concept_elements:
        if concept_id is not None and not is_in_conquery_ids(concept_id, concept_element["ids"]):
            continue
        concept_element["excludeFromSecondaryIdQuery"] = True

    return query


def exclude_from_time_aggregation(query: dict, concept_id: str = None):
    concept_elements = get_concept_elements_from_query(query)
    for concept_element in concept_elements:
        if concept_id is not None and not is_in_conquery_ids(concept_id, concept_element["ids"]):
            continue
        concept_element["excludeFromTimeAggregation"] = True

    return query


def concept_element_from_concept(concept_ids: list, concept_object: dict,
                                 label: str = None, connector_ids: list = None) -> dict:
    """
    Takes concept object from response of /app/datasets/concepts.
    @param concept_ids:
    @param concept_object:
    @param label:
    @param connector_ids: list of connector ids that are defined in tables
    (default None -> include all tables found in concept)
    @return:
    """

    if 'tables' not in concept_object:
        raise KeyError("'concept_object' must have key 'tables'")
    if type(concept_object.get('tables')) != list:
        raise TypeError("'concept_object.tables' must be of type 'list'")
    for table in concept_object.get('tables'):
        if 'connectorId' not in table:
            raise KeyError("Each table in 'concept_object.table' must have a 'connectorId'")
    if type(concept_ids) is not list:
        concept_ids = [concept_ids]
        if any([type(concept_id) is not str for concept_id in concept_ids]):
            raise ValueError("Parameter 'concept_ids' must be string or list of strings")
    if connector_ids is not None and type(connector_ids) is not list:
        connector_ids = [connector_ids]
        if any([type(connector_id) is not str for connector_id in connector_ids]):
            raise ValueError("Parameter 'concept_ids' must be string or list of strings")

    if label is None:
        label = concept_object.get("label")

    # get connector ids
    table_connector_id_dict_list = list()
    for table in concept_object.get('tables'):
        table_connector_id = table.get('connectorId')
        if connector_ids is not None and not is_in_conquery_ids(table_connector_id, connector_ids):
            continue
        table_connector_id_dict_list.append({'id': table_connector_id})

    if not table_connector_id_dict_list:
        raise ValueError(f"Could not find any connector for concept element")

    return {
        'type': 'CONCEPT',
        'ids': concept_ids,
        'label': label,
        'tables': table_connector_id_dict_list
    }


def concept_query_from_concept(concept_ids: list, concept_object, concept_label="",
                               start_date: str = None, end_date: str = None):
    """ Create CONCEPT_QUERY with a given CONCEPT as it root node.

    For simple query generation from a single concept.

    :param concept_ids:
    :param concept_object:
    :param concept_label:
    :return: a concept query with the given concept as it's sole root node
    """
    concept_element = concept_element_from_concept(concept_ids=concept_ids,
                                                   concept_object=concept_object,
                                                   label=concept_label)
    if start_date is not None and end_date is not None:
        concept_element = add_date_restriction(concept_element, start_date, end_date)

    concept_query = wrap_concept_query(concept_element)

    return concept_query


def edit_concept_query(concept_query_object, concept_id, connector_ids=None, date_column_id='', filter_ids=None,
                       select_ids=None, concept_select_ids=None, remove_connector=False):
    """
    Adds selects, filters, dateColumns and concept_selects to query.
    :param remove_connector: When set True, tables with given connector_ids are removed
    :param concept_query_object: Either of type 'CONCEPT-QUERY', 'OR', 'AND' or 'CONCEPT'
    :param concept_id: id of the concept-object that should be edited or added
    :param connector_ids: connector_ids ob the tables of the concept-object that should be edited.
    When none is given, all tables of the concept are edited
    :param date_column_id: date_columns to add on table level
    :param filter_ids: filters to add on table level
    :param select_ids: selects to add on table level
    :param concept_select_ids: selects to add on concept level
    :return:
    """
    connector_ids = list() if connector_ids is None else connector_ids
    filter_ids = list() if filter_ids is None else filter_ids
    select_ids = list() if select_ids is None else select_ids
    concept_select_ids = list() if concept_select_ids is None else concept_select_ids

    concept_query_object = deepcopy(concept_query_object)
    connector_ids = check_input_list(connector_ids, entry_type=str)
    filter_ids = check_input_list(filter_ids, entry_type=dict)
    select_ids = check_input_list(select_ids, entry_type=str)
    concept_select_ids = check_input_list(concept_select_ids, entry_type=str)

    if concept_query_object.get("type") == "CONCEPT_QUERY":
        concept_query_object['root'] = edit_concept_query(concept_query_object.get('root'), concept_id,
                                                          connector_ids,
                                                          date_column_id, filter_ids, select_ids,
                                                          concept_select_ids, remove_connector)
        return concept_query_object

    children = concept_query_object.get('children', [])

    if concept_query_object.get("type") == "DATE_RESTRICTION":
        concept_query_object['child'] = edit_concept_query(concept_query_object.get('child'), concept_id,
                                                           connector_ids,
                                                           date_column_id, filter_ids, select_ids,
                                                           concept_select_ids, remove_connector)
        return concept_query_object

    if type(children) is not list:
        raise TypeError(f"Value to key 'children' must be of type list, not {type(children)}")

    if concept_query_object.get("type") in ["AND", "OR"]:
        for child_ind, child in enumerate(children):
            children[child_ind] = edit_concept_query(child, concept_id, connector_ids,
                                                     date_column_id, filter_ids, select_ids, concept_select_ids,
                                                     remove_connector)
        concept_query_object['children'] = children
        return concept_query_object

    if concept_query_object.get("type") != "CONCEPT":
        raise TypeError(f"Unknown type {concept_query_object.get('type')} \n Expected type: 'CONCEPT'")

    if concept_id not in concept_query_object.get('ids', []):
        return concept_query_object

    concept_tables = concept_query_object.get('tables', [])
    concept_connector_ids = [table.get('id') for table in concept_tables]

    # when no connector_ids are defined, edit all tables of all concepts
    if not connector_ids:
        connector_ids = concept_connector_ids

    # remove connector
    if remove_connector:
        concept_tables = [concept_table for concept_table in concept_tables
                          if concept_table.get('id') not in connector_ids]
        concept_query_object['tables'] = concept_tables
        return concept_query_object

    # add tables for connector_ids that are not found
    # add first without filters and selects and add them later
    for connector_id in connector_ids:
        if connector_id not in concept_connector_ids:
            concept_tables.append({
                "id": connector_id,
                "dateColumn": None,
                "selects": [],
                "filters": []
            })

    # add selects and filters on connector level
    for concept_table in concept_tables:
        if concept_table.get('id') in connector_ids:
            if date_column_id:
                # dateColumn Value may be None
                date_column_obj = {"value": date_column_id}
                concept_table['dateColumn'] = date_column_obj
            concept_table['selects'] = [*concept_table.get('selects', []), *select_ids]
            concept_table['filters'] = [*concept_table.get('filters', []), *filter_ids]
    concept_query_object['tables'] = concept_tables

    # add selects on concept level
    if concept_select_ids:
        concept_query_object['selects'] = [*concept_query_object.get('selects', []), *concept_select_ids]

    return concept_query_object


def add_subquery_to_concept_query(query, subquery):
    query_object = deepcopy(query)
    query_object_node_type = query_object.get('type')

    if subquery.get('type') == 'CONCEPT_QUERY':
        subquery = subquery.get('root')

    if query_object_node_type == 'CONCEPT_QUERY':
        query_object['root'] = add_subquery_to_concept_query(query_object.get('root'), subquery)
        return query_object
    elif query_object_node_type == 'AND':
        query_object['children'].append(subquery)
        return query_object
    else:
        query = {
            "type": "AND",
            "children": [
                query_object,
                subquery
            ]
        }
        return query


def create_frontend_query(and_queries: list, date_restrictions: list = None):
    """ Create a more complex query from a two-dimensional list of concept queries.

    :example:
    >>> # create concept queries
    >>> concepts = await cq.get_concepts('some_dataset')
    >>> concept_object_1_1 = concepts.get('my_concept_1_1')
    >>> concept_query_1_1 = util.concept_query_from_concept('my_concept_1_1', concept_object_1_1)
    >>> concept_object_1_2 = concepts.get('my_concept_1_2')
    >>> concept_query_1_2 = util.concept_query_from_concept('my_concept_1_2', concept_object_1_2)
    >>> concept_object_2 = concepts.get('my_concept_2')
    >>> concept_query_2 = util.concept_query_from_concept('my_concept_2', concept_object_2)
    >>> # place "or" between first to queries and "and" between the result of that and the next query
    >>> cq.create_frontend_query([[concept_object_1_1, concept_object_1_2],[concept_object_2]])

    :param and_queries: one or two-dimensional list of concept_queries
    :param date_restrictions: list of lists, containing start and end date for each element in list and_queries
    :return: a concept query with AND between each element in and_queries and OR between each element of a list in
            and_queries. The date restrictions are added to the corresponding concept query
    """
    if type(and_queries) is dict:
        and_queries = [and_queries]

    for and_query_ind in range(0, len(and_queries)):
        if type(and_queries[and_query_ind]) is not list:
            and_queries[and_query_ind] = [and_queries[and_query_ind]]

    children_and = [
        {'type': 'OR',
         'children': [
             or_query['root'] if 'root' in or_query.keys() else or_query for or_query in and_query
         ]
         } for and_query in and_queries
    ]

    if date_restrictions is not None:

        if len(date_restrictions) == 2 and all(
                [type(date_restriction) is str for date_restriction in date_restrictions]):
            date_restrictions = [date_restrictions] * len(and_queries)
        elif len(date_restrictions) == 1 and type(date_restrictions[0]) is list:
            date_restrictions = date_restrictions * len(and_queries)
        elif any([type(date_restriction) is not list for date_restriction in date_restrictions]):
            raise ValueError('an element of date_restrictions is not a list')
        elif len(date_restrictions) != len(and_queries):
            raise ValueError('date_restrictions must be of length 1 or same length as and_queries')

        children_and = [
            {
                'type': 'DATE_RESTRICTION',
                'dateRange': {
                    'min': date_restrictions[and_query_ind][0],
                    'max': date_restrictions[and_query_ind][1]
                },
                'child': children
            } if date_restrictions[and_query_ind] else children
            for and_query_ind, children in enumerate(children_and)
        ]

    return {
        'type': 'CONCEPT_QUERY',
        'root': {
            'type': 'AND',
            'children': children_and
        }
    }
