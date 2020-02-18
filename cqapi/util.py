from datetime import date
from datetime import datetime
from copy import deepcopy


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


def concept_query_from_concept(concept_id, concept_object, concept_label = ""):
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

    query = {
        'type': 'CONCEPT_QUERY',
        'root': {
            'type': 'CONCEPT',
            'ids': [concept_id],
            'label': concept_label,
            'tables': [{'id': table.get('connectorId')} for table in concept_object.get('tables')]
        }
    }
    return query


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
             or_query['root'] for or_query in and_query
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


def create_absolute_form_query(query_id, feature_queries: list, date_range: list):

    """ Create an ABSOLUTE_FORM_QUERY
    :param query_id: ID of the query that will be used to get the patient group
    :param feature_queries: list of concept queries to add columns
    :param date_range: date range with list containing first and last date, dates have to be in format %Y-%m-%d
    :return:
    """

    if datetime.strptime(date_range[0], '%Y-%m-%d') > datetime.strptime(date_range[1], '%Y-%m-%d'):
        raise ValueError(f"Invalid dateRange. {date_range[0]} is after {date_range[1]}")
    for feature_query in feature_queries:
        if 'root' not in feature_query.keys() and 'children' not in feature_query.keys():
            raise ValueError(f"Invalid feature query. Query {feature_query} has no key root or children")

    features = [{'type': 'OR', 'children': [feature_query['root']]}
                if 'root' in feature_query.keys() else feature_query
                for feature_query in feature_queries]

    return {
        'type': 'EXPORT_FORM',
        'queryGroup': query_id,
        'timeMode': {
            "value": 'ABSOLUTE',
            'dateRange': {
                'min': date_range[0],
                'max': date_range[1]
            },
            'features': features
        }
    }


def create_relative_query(index_query, before_query, after_query, time_before, time_after,
                          index_selector='FIRST', index_placement='NEUTRAL', time_unit='QUARTERS'):
    """ Create a RELATIVE_FORM_QUERY for temporally relative data export.

    :param index_query: Concept query describing the event that is to be used as the index date
    :param before_query: Concept query to select which information will be requested for the time frame before the index
        date
    :param after_query: Concept query to select which information will be requested for time frame after the index date
    :param time_before: Number of time units (see `time_unit`) before the index date to consider using the
        `before_query`
    :param time_after: Number of time units (see `time_unit`) after the index date to consider using the `after_query`
    :param index_selector: One of `'FIRST'` (default), `'LAST'`, and `'RANDOM'`. Selects which index date to use in
        case `index_query` matches multiple event occurrences
    :param index_placement: One of `'BEFORE'`, `'NEUTRAL'` (default), and `'AFTER'`. Selects if the index event
        should be considered for either of the before or after time frames' results
    :param time_unit: One of `'QUARTERS'` (default) and `'DAYS'`. Time unit of `time_before` and `time_after`
        respectively
    :return:
    """
    valid_time_units = ['QUARTERS', 'DAYS']
    valid_index_selectors = ['FIRST', 'LAST', 'RANDOM']
    valid_index_placements = ['BEFORE', 'NEUTRAL', 'AFTER']

    if time_unit not in valid_time_units:
        raise ValueError(f"Invalid time_unit. Must be one of {valid_time_units}")
    if time_before < 0:
        raise ValueError("Invalid time_before. Must be positive")
    if time_after < 0:
        raise ValueError("Invalid time_after. Must be positive")
    if index_selector not in valid_index_selectors:
        raise ValueError(f"Invalid index_selector. Must be one of {valid_index_selectors}")
    if index_placement not in valid_index_placements:
        raise ValueError(f"Invalid index_placement. Must be one of {valid_index_placements}")

    return {
        'type': 'RELATIVE_FORM_QUERY',
        'query': index_query,
        'features': before_query,
        'outcomes': after_query,
        'indexSelector': index_selector,
        'indexPlacement': index_placement,
        'timeCountBefore': time_before,
        'timeCountAfter': time_after,
        'timeUnit': time_unit
    }


def _parse_iso_date(datestring: str):
    y, m, d = map(lambda x: int(x), datestring.split('-'))
    return date(y, m, d)
