from enum import unique, Enum


class Keys:
    type = "type"
    date_aggregation_mode = "dateAggregationMode"
    id = "id"
    ids = "ids"
    connector_id = "connectorId"
    date_column = "dateColumn"
    exclude_from_secondary_id = "excludeFromSecondaryIdQuery"
    exclude_from_time_aggregation = "excludeFromTimeAggregation"
    selects = "selects"
    filter = "filter"
    filters = "filters"
    tables = "tables"
    root = "root"
    child = "child"
    children = "children"
    secondary_id = "secondaryId"
    date_range = "dateRange"
    min = "min"
    max = "max"
    label = "label"
    create_exist = "createExists"
    value = "value"
    query = "query"
    options = "options"
    row_prefix = "row_prefix"
    format = "format"
    features = "features"
    outcomes = "outcomes"
    values = "values"
    date_action = "dateAction"
    merge = "MERGE"
    intersect = "INTERSECT"
    negate = "NEGATE"
    block = "BLOCK"
    alignment_hint = "alignmentHint"
    time_mode = "timeMode"
    time_unit = "timeUnit"
    time_count_before = "timeCountBefore"
    time_count_after = "timeCountAfter"
    index_selector = "indexSelector"
    index_placement = "indexPlacement"
    resolutions = "resolution"
    create_resolution_subdivisions = "alsoCreateCoarserSubdivisions"
    query_group = "queryGroup"
    default = "default"
    description = "description"
    money_type = "MONEY"


@unique
class QueryType(Enum):
    CONCEPT = "CONCEPT"
    CONCEPT_QUERY = "CONCEPT_QUERY"
    AND = "AND"
    OR = "OR"
    SECONDARY_ID_QUERY = "SECONDARY_ID_QUERY"
    DATE_RESTRICTION = "DATE_RESTRICTION"
    NEGATION = "NEGATION"
    SAVED_QUERY = "SAVED_QUERY"
    EXPORT_FORM = "EXPORT_FORM"
    EXTERNAL = "EXTERNAL"
    FULL_EXPORT_FORM = "FULL_EXPORT_FORM"
