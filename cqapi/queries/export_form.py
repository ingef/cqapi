from cqapi.queries.validate import validate_date_range, validate_resolutions, validate_time_unit, validate_time_count, \
    validate_index_plament, validate_index_selector
from cqapi.namespace import Keys


def create_entitiy_date_form_query(query_id: str, date_range: list, feature_queries: list,
                                   resolutions: list[str] = None, date_aggregation_mode: str = "LOGICAL"):
    """
    @param query_id: ID of the query that will be used to get the patient group
    @param resolutions: String of resolution for the output. Possible values: for example ['COMPLETE']
    @param feature_queries: list of concept queries
    @param form_type: Can be one of ['ABSOLUTE', 'ENTITY_DATE']
    @param date_aggregation_mode:
    @param date_range:
    @return:
    """

    if resolutions is None:
        resolutions = ["COMPLETE"]
    validate_resolutions(resolutions)

    # extract concept from "CONCEPT_QUERY"-Objects
    feature_queries = [feature_query.get("root")
                       if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                       for feature_query in feature_queries]

    return {
        Keys.type: "EXPORT_FORM",
        Keys.query_group: query_id,
        Keys.resolutions: resolutions,
        Keys.time_mode: {
            Keys.value: "ENTITY_DATE",
            Keys.date_aggregation_mode: date_aggregation_mode,
            Keys.date_range: {
                Keys.min: date_range[0],
                Keys.max: date_range[1]
            },
            Keys.features: feature_queries
        }
    }


def create_absolute_form_query(query_id: str, feature_queries: list, date_range: list, resolutions: list[str] = None):
    """

    @param query_id: ID of the query that will be used to get the patient group
    @param resolutions: List of resolution for the output. Possible values: e.g. 'COMPLETE' (default)
    @param feature_queries: list of concept queries
    @param date_range: list of two dates (start and end) each date must be of form YYYY-MM-DD
    @return:
    """

    if resolutions is None:
        resolutions = ["COMPLETE"]
    validate_resolutions(resolutions)

    validate_date_range(date_range)

    # extract concept from "CONCEPT_QUERY"-Objects
    feature_queries = [feature_query.get("root")
                       if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                       for feature_query in feature_queries]

    return {
        Keys.type: "EXPORT_FORM",
        Keys.query_group: query_id,
        Keys.resolutions: resolutions,
        Keys.time_mode: {
            Keys.value: "ABSOLUTE",
            Keys.date_range: {
                Keys.min: date_range[0],
                Keys.max: date_range[1]
            },
            Keys.features: feature_queries
        }
    }


def create_relative_form_query(query_id: str, resolutions: list[str] = None, before_index_queries: list = None,
                               after_index_queries: list = None,
                               time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                               index_selector: str = 'EARLIEST', index_placement: str = 'BEFORE'):
    """

    @param query_id: ID of the query that will be used to get the patient group
    @param resolutions: List of resolution for the output. Possible values: e.g. 'COMPLETE' (default)
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
    if resolutions is None:
        resolutions = ["COMPLETE"]
    validate_resolutions(resolutions)
    validate_time_unit(time_unit)
    validate_time_count(time_count_before)
    validate_time_count(time_count_after)
    validate_index_selector(index_selector)
    validate_index_plament(index_placement)

    # extract concept from "CONCEPT_QUERY"-Objects
    if before_index_queries is None:
        before_index_queries = []
    before_index_queries = [feature_query.get("root")
                            if feature_query.get("type") == "CONCEPT_QUERY" else feature_query
                            for feature_query in before_index_queries]

    # extract concept from "CONCEPT_QUERY"-Objects
    if after_index_queries is None:
        after_index_queries = []
        
    after_index_queries = [outcome_query.get("root")
                           if outcome_query.get("type") == "CONCEPT_QUERY" else outcome_query
                           for outcome_query in after_index_queries]

    return {
        Keys.type: 'EXPORT_FORM',
        Keys.query_group: query_id,
        Keys.resolutions: resolutions,
        Keys.time_mode: {
            Keys.value: 'RELATIVE',
            Keys.time_unit: time_unit,
            Keys.time_count_before: time_count_before,
            Keys.time_count_after: time_count_after,
            Keys.index_selector: index_selector,
            Keys.index_placement: index_placement,
            Keys.features: before_index_queries,
            Keys.outcomes: after_index_queries
        }
    }
