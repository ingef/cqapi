from datetime import date
from datetime import datetime
from copy import deepcopy
import utility.process_description as u_pd


def object_to_dict(obj):
    """ Convert object to dict with __class__ and __module__ members.

    Can be used with json.dumps to serialize an object to json in a format
    from which it can be deserialized into the original object again.

    :param obj: Object to be serialized
    :return: (JSON-serializable) dictionary with __class__ and __module__ members
    :rtype: dict

    :example:
    >>> json.dumps(object, default=object_to_dict)
    """
    obj_dict = {
        "__class__": obj.__class__.__name__,
        "__module__": obj.__module__
    }

    obj_dict.update(obj.__dict__)

    return obj_dict


def dict_to_object(dictionary):
    """ Convert dictionary to Python object if dictionary has __class__ and __module__ members.

    Can be used with json.loads to deserialize a JSON-encoded object.
    :param dictionary: Dictionary to deserialize
    :return: Deserialized object if __class__ and __module__ are present, otherwise the input dictionary.

    :example:
    >>> json.loads('{"__class__":"someClass","__module__":"someModule", ... }', object_hook=dict_to_object)
    """
    if "__class__" in dictionary and "__module__" in dictionary:
        class_name = dictionary.pop("__class__")
        module_name = dictionary.pop("__module__")

        module = __import__(module_name)

        class_ = getattr(module, class_name)

        object = class_(**dictionary)
    else:
        object = dictionary
    return object


def selects_per_concept(concepts: dict):
    """ Aggregates a dict of concepts to a dict of available selects per concept.

    Used to declutter the concepts dict returned from a ConqueryConnection.get_concepts('dataset') call.

    Usage example:
        # lets say you're interested in all available selects (specifically their ids) for a specific concept
        concepts = await cq.get_concepts('dataset')
        selects_of_concept_x = selects_per_concept(concepts).get('concept_x')
        type(selects_of_concept)  # = list, because of the above .get() call on the dictionary

        # selects_of_concept is applicable also to concepts returned by cq.get_concept('dataset', 'specific_concept')
        concepts = await cq.get_concept('dataset', 'specific_concept')
        selects_by_concept = selects_per_concept(concepts)
        type(selects_by_concept)  # = dict

    :param concepts: dict of concepts as returned by ConqueryConnection.get_concept and .get_concepts calls.
    :return: dict of list of available selects, i.e. a mapping from concept to its available selects.

    TODO: get all selects, not only root selects
    """
    return {
        concept_id: [select_dict.get('id')
                     for select_dict in concept.get('selects', [])]
        for (concept_id, concept) in concepts.items()
    }


def filters_per_concept(concepts: dict):
    """ Aggregates a dict of concepts to a dict of available filters per concept.

    Used to declutter the concepts dict returned from a ConqueryConnection.get_concepts('dataset') call.

    Usage example:
        # lets say you're interested in all available filters (specifically their ids) for a specific concept
        concepts = await cq.get_concepts('dataset')
        filters_of_concept_x = filters_per_concept(concepts).get('concept_x')
        type(filters_of_concept)  # = list, because of the above .get() call on the dictionary

        # filters_of_concept is applicable also to concepts returned by cq.get_concept('dataset', 'specific_concept')
        concepts = await cq.get_concept('dataset', 'specific_concept')
        filters_by_concept = filters_per_concept(concepts)
        type(filters_by_concept)  # = dict

    :param concepts: dict of concepts as returned by ConqueryConnection.get_concept and .get_concepts calls.
    :return: dict of list of available filters, i.e. a mapping from concept to its available filters.
    """
    return {concept_id: [
        (table.get('id'), [table_filter.get('id') for table_filter in table.get('filters')])
        for table in concept.get('tables')]
        for (concept_id, concept) in concepts.items()}


def add_selects_to_concept_query(query, target_concept_id: str, selects: list):
    """ Add select ids to CONCEPT nodes in queries.

    :param query: query to add selects to.
    :param target_concept_id: CONCEPT's id to which the selects should be added.
    :param selects: list of select_ids to be added.
    :return: the enriched query object - will be the same as the input query iff it does not contain any CONCEPT nodes
        with the target_concept_id.
    """
    if type(selects) is not list:
        raise Exception("parameter 'selects' must be a list.")

    query_object = deepcopy(query)

    query_object_node_type = query_object.get('type')
    if query_object_node_type == 'CONCEPT_QUERY' or query_object_node_type == 'RELATIVE_FORM_QUERY':
        query_object['root'] = add_selects_to_concept_query(query_object.get('root'), target_concept_id, selects)
        return query_object
    elif query_object_node_type == 'AND':
        query_object['children'] = [
            add_selects_to_concept_query(child, target_concept_id, selects)
            for child in query_object.get('children')
        ]
        return query_object
    elif query_object_node_type == 'OR':
        query_object['children'] = [
            add_selects_to_concept_query(child, target_concept_id, selects)
            for child in query_object.get('children')
        ]
        return query_object
    elif query_object_node_type == 'NEGATION':
        query_object['child'] = add_selects_to_concept_query(query_object.get('child'), target_concept_id, selects)
        return query_object
    elif query_object_node_type == 'DATE_RESTRICTION':
        query_object['child'] = add_selects_to_concept_query(query_object.get('child'), target_concept_id, selects)
        return query_object
    elif query_object_node_type == 'CONCEPT':
        if target_concept_id in query_object.get('ids'):
            if query_object.get('selects') is not None:
                query_object.get('selects').extend(selects)
                return query_object
            else:
                query_object['selects'] = selects
                return query_object
        else:
            return query_object
    else:
        raise Exception(f"Unknown type in query_object: {query_object.get('type')}")


def add_date_restriction_to_concept_query(query, target_concept_id: str, date_start: date, date_end: date):
    """ Add (absolute) date restriction to a query.

    Adds the date restriction directly above all occurrences of the target_concept_id in the query object.

    :param query: Query to add date restriction to.
    :param target_concept_id: Id of concept node in query above which the date restriction node should be added.
    :param date_start: Start-date of the date restriction.
    :param date_end: End-date of the date restriction.
    :return:
    """
    query_object = deepcopy(query)

    if type(date_start) is not date:
        start = _parse_iso_date(date_start)
    else:
        start = date_start
    if type(date_end) is not date:
        end = _parse_iso_date(date_end)
    else:
        end = date_end
    if (end - start).days < 0:
        raise ValueError("Invalid DATE_RESTRICTION: Start-date after end-date")

    query_object_node_type = query_object.get('type')

    if query_object_node_type == 'CONCEPT_QUERY':
        query_object['root'] = add_date_restriction_to_concept_query(
            query_object.get('root'), target_concept_id, start, end)
        return query_object
    elif query_object_node_type == 'AND':
        query_object['children'] = [
            add_date_restriction_to_concept_query(child, target_concept_id, start, end)
            for child in query_object.get('children')
        ]
        return query_object
    elif query_object_node_type == 'OR':
        query_object['children'] = [
            add_date_restriction_to_concept_query(child, target_concept_id, start, end)
            for child in query_object.get('children')
        ]
        return query_object
    elif query_object_node_type == 'NEGATION':
        query_object['child'] = add_date_restriction_to_concept_query(
            query_object.get('child'), target_concept_id, start, end)
        return query_object
    elif query_object_node_type == 'DATE_RESTRICTION':
        query_object['child'] = add_date_restriction_to_concept_query(
            query_object.get('child'), target_concept_id, start, end)
        return query_object
    elif query_object_node_type == 'CONCEPT':
        if target_concept_id in query_object.get('ids'):
            return {
                "type": "DATE_RESTRICTION",
                "dateRange": {
                    "min": start.isoformat(),
                    "max": end.isoformat()
                },
                "child": query_object
            }
        else:
            return query_object
    else:
        raise Exception(f"Unknown type in query_object: {query_object.get('type')}")


def concept_query_from_concept(concept_ids: list, concept_object, concept_label=""):
    """ Create CONCEPT_QUERY with a given CONCEPT as it root node.

    For simple query generation from a single concept.

    :example:
    >>> # get concept definitions
    >>> concepts = await cq.get_concepts('some_dataset')
    >>> concept_object = concepts.get('my_concept')
    >>> concept_query = util.concept_query_from_concept('my_concept', concept_object)
    >>> # concept_query is ready for execution
    >>> cq.execute_query('some_dataset', concept_query)
    >>> # or can be combined with other utility methods to add selects etc.

    :param concept_id:
    :param concept_object:
    :return: a concept query with the given concept as it's sole root node
    """
    # todo write tests
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

    query = {
        'type': 'CONCEPT_QUERY',
        'root': {
            'type': 'CONCEPT',
            'ids': concept_ids,
            'label': concept_label,
            'tables': [{'id': table.get('connectorId')} for table in concept_object.get('tables')]
        }
    }
    return query


def edit_concept_query(concept_query_object, concept_id, connector_ids=[], date_column_id='', filter_ids=[],
                       select_ids=[], concept_select_ids=[], remove_connector=False):
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
    concept_query_object = deepcopy(concept_query_object)
    connector_ids = u_pd.check_input_list(connector_ids, entry_type=str)
    filter_ids = u_pd.check_input_list(filter_ids, entry_type=dict)
    select_ids = u_pd.check_input_list(select_ids, entry_type=str)
    concept_select_ids = u_pd.check_input_list(concept_select_ids, entry_type=str)

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


def create_form_query(form_query_type: str, query_id: str, feature_queries: list, outcome_queries: list = None,
                      resolution='COMPLETE', date_range: list = None,
                      time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                      index_selector: str = 'EARLIEST', index_placement: str = 'BEFORE'):
    """
    Create an form query
    :param form_query_type: Possible values: 'ABSOLUTE', 'RELATIVE'
    :param query_id: ID of the query that will be used to get the patient group
    :param feature_queries: list of concept queries representing features
    :param outcome_queries: list of concept queries representing outcomes
    :param resolution: Resolution for the output. Possible values: 'COMPLETE' (default), 'QUARTERS', 'YEARS
    :param date_range: time period for feature_queries - only necessary with absolut form query
    :param time_unit: Possible values: 'QUARTERS' (default) , 'DAYS'
    :param time_count_before: number of time-units in feature time period (default: 1)
    :param time_count_after: number fo time -units in outcome time period (default: 1)
    :param index_selector: which event to take as index. Possible values: 'EARLIEST', 'LAST', 'RANDOM'
    :param index_placement: which time period the index time unit is associated with. Possible values: 'BEFORE', 'NEUTRAL', 'AFTER'
    :return: Query of type RELATIVE_EXPORT_FORM
    """

    form_query_type = form_query_type.upper()
    absolute_form_flag = form_query_type == 'ABSOLUTE'
    relative_form_flag = form_query_type == 'RELATIVE'

    # validate input
    valid_time_units = ['QUARTERS', 'DAYS']
    valid_index_selectors = ['EARLIEST', 'LAST', 'RANDOM']
    valid_index_placements = ['BEFORE', 'NEUTRAL', 'AFTER']

    if time_unit not in valid_time_units:
        raise ValueError(f"Invalid time_unit. Must be one of {valid_time_units}")
    if time_count_before < 0:
        raise ValueError("Invalid time_before. Must be positive")
    if time_count_after < 0:
        raise ValueError("Invalid time_after. Must be positive")
    if index_selector not in valid_index_selectors:
        raise ValueError(f"Invalid index_selector. Must be one of {valid_index_selectors}")
    if index_placement not in valid_index_placements:
        raise ValueError(f"Invalid index_placement. Must be one of {valid_index_placements}")

    if not (absolute_form_flag or relative_form_flag):
        raise ValueError(f"form_query_type must be 'ABSOLUTE' or 'RELATIVE', not {form_query_type}")

    if absolute_form_flag:
        if date_range is None or \
                datetime.strptime(date_range[0], '%Y-%m-%d') > datetime.strptime(date_range[1], '%Y-%m-%d'):
            raise ValueError(f"Invalid dateRange. {date_range[0]} is after {date_range[1]}")

    for feature_query in feature_queries:
        if 'root' not in feature_query.keys() and 'children' not in feature_query.keys():
            raise ValueError(f"Invalid feature query. Query {feature_query} has no key root or children")

    for outcome_query in outcome_queries:
        if 'root' not in outcome_query.keys() and 'children' not in outcome_query.keys():
            raise ValueError(f"Invalid feature query. Query {outcome_query} has no key root or children")

    # edit features and outcomes queries slightly to match restrictions

    features = [{'type': 'OR', 'children': [feature_query['root']]}
                if 'root' in feature_query.keys() else feature_query
                for feature_query in feature_queries]

    if relative_form_flag:
        outcomes = [{'type': 'OR', 'children': [outcome_query['root']]}
                    if 'root' in outcome_query.keys() else outcome_query
                    for outcome_query in outcome_queries]

    if absolute_form_flag:
        return {
            'type': 'EXPORT_FORM',
            'queryGroup': query_id,
            'resolution': resolution,
            'timeMode': {
                "value": form_query_type,
                'dateRange': {
                    'min': date_range[0],
                    'max': date_range[1]
                },
                'features': features
            }
        }

    return {
        'type': 'EXPORT_FORM',
        'queryGroup': query_id,
        'resolution': resolution,
        'timeMode': {
            'value': form_query_type,
            'timeUnit': time_unit,
            'timeCountBefore': time_count_before,
            'timeCountAfter': time_count_after,
            'indexSelector': index_selector,
            'indexPlacement': index_placement,
            'features': features,
            'outcomes': outcomes
        }
    }


def _parse_iso_date(datestring: str):
    y, m, d = map(lambda x: int(x), datestring.split('-'))
    return date(y, m, d)
