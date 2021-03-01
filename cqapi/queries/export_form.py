from cqapi.queries.validate import validate_date_range, validate_resolution, validate_time_unit, validate_time_count, \
    validate_index_plament, validate_index_selector

def create_entitiy_date_form_query(query_id: str, date_range: list, feature_queries: list, resolution: str = "COMPLETE"):
    """

    @param query_id: ID of the query that will be used to get the patient group
    @param resolution: Resolution for the output. Possible values: 'COMPLETE' (default), 'QUARTERS', 'YEARS
    @param feature_queries: list of concept queries
    @param form_type: Can be one of ['ABSOLUTE', 'ENTITY_DATE']
    @param resolution:
    @return:
    """

    validate_resolution(resolution)

    # extract concept from "CONCEPT_QUERY"-Objects
    feature_queries = [feature_query.get("root")
                       if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                       for feature_query in feature_queries]

    return {
        'type': "EXPORT_FORM",
        'queryGroup': query_id,
        'resolution': resolution,
        'timeMode': {
            "value": "ENTITY_DATE",
            'dateRange': {
                'min': date_range[0],
                'max': date_range[1]
            },
            'features': feature_queries
        }
    }
def create_absolute_form_query(query_id: str, feature_queries: list, date_range: list, resolution: str = "COMPLETE"):
    """

    @param query_id: ID of the query that will be used to get the patient group
    @param resolution: Resolution for the output. Possible values: 'COMPLETE' (default), 'QUARTERS', 'YEARS
    @param feature_queries: list of concept queries
    @param date_range: list of two dates (start and end) each date must be of form YYYY-MM-DD
    @param resolution:
    @return:
    """

    validate_resolution(resolution)

    validate_date_range(date_range)

    # extract concept from "CONCEPT_QUERY"-Objects
    feature_queries = [feature_query.get("root")
                       if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                       for feature_query in feature_queries]

    return {
        'type': "EXPORT_FORM",
        'queryGroup': query_id,
        'resolution': resolution,
        'timeMode': {
            "value": "ABSOLUTE",
            'dateRange': {
                'min': date_range[0],
                'max': date_range[1]
            },
            'features': feature_queries
        }
    }


def create_relative_form_query(query_id: str, resolution: str = "COMPLETE", before_index_queries: list = None,
                               after_index_queries: list = None,
                               time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                               index_selector: str = 'EARLIEST', index_placement: str = 'BEFORE'):
    """

    @param query_id: ID of the query that will be used to get the patient group
    @param resolution: Resolution for the output. Possible values: 'COMPLETE' (default), 'QUARTERS', 'YEARS
    @param before_index_queries: list of concept queries that are collected before the index date
    @param after_index_queries: list of concept queries that are collected after the index date
    @param time_unit: Possible values: 'QUARTERS' (default) , 'DAYS'
    @param time_count_before: number of time-units in feature time period (default: 1)
    @param time_count_after: number fo time -units in outcome time period (default: 1)
    @param index_selector: which event to take as index. Possible values: 'EARLIEST', 'LATEST', 'RANDOM'
    @param index_placement: which time period the index time unit is associated with. Possible values: 'BEFORE', 'NEUTRAL', 'AFTER'
    @return:
    """
    # validate
    validate_resolution(resolution)
    validate_time_unit(time_unit)
    validate_time_count(time_count_before)
    validate_time_count(time_count_after)
    validate_index_selector(index_selector)
    validate_index_plament(index_placement)

    # extract concept from "CONCEPT_QUERY"-Objects
    before_index_queries = [feature_query.get("root")
                            if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                            for feature_query in before_index_queries]
    # extract concept from "CONCEPT_QUERY"-Objects
    after_index_queries = [outcome_query.get("root")
                           if outcome_query.get("type") == "CONCEPT_QUERY" else outcome_query
                           for outcome_query in after_index_queries]

    return {
        'type': 'EXPORT_FORM',
        'queryGroup': query_id,
        'resolution': resolution,
        'timeMode': {
            'value': 'RELATIVE',
            'timeUnit': time_unit,
            'timeCountBefore': time_count_before,
            'timeCountAfter': time_count_after,
            'indexSelector': index_selector,
            'indexPlacement': index_placement,
            'features': before_index_queries,
            'outcomes': after_index_queries
        }
    }
