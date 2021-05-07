from cqapi.conquery_ids import get_dataset as get_dataset_from_id


def get_dataset_from_query(query: dict):
    if "type" not in query.keys():
        raise ValueError(f"Query object {query} has no key 'type'")

    query_type = query.get("type")

    if query_type == "SECONDARY_ID_QUERY":
        return get_dataset_from_id(query["secondaryId"])

    if query_type == "CONCEPT_QUERY":
        return get_dataset_from_query(query["root"])

    if query_type in ["DATE_RESTRICTION", "NEGATION"]:
        return get_dataset_from_query(query["child"])

    if query_type in ["AND", "OR"]:
        return get_dataset_from_query(query["children"][0])

    if query_type == "SAVED_QUERY":
        return get_dataset_from_id[query["query"]]

    if query_type == "CONCEPT":
        return get_dataset_from_id(query["ids"][0])

    if query_type == "EXPORT_FORM":
        return get_dataset_from_id(query["queryGroup"])
