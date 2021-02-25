from cqapi.util import check_arg_type
from cqapi.api_sync import ConqueryConnection

# conquery id info
conquery_id_separator = "."
dataset_loc = 0
concept_loc = 1
connector_loc = 2
select_loc = 3
filter_loc = 3


@check_arg_type(["dataset_id"])
def is_dataset_id(dataset_id: str):
    if dataset_id.startswith("adb_"):
        return True
    if dataset_id.startswith("fdb_"):
        return True
    if dataset_id.startswith("dataset"):
        return True

    return False


@check_arg_type(convert_to_list={"conquery_id_elements": str})
def id_elements_to_id(conquery_id_elements: list):
    return conquery_id_separator.join(conquery_id_elements)


@check_arg_type(["conquery_id"])
def get_conquery_id_element(conquery_id: str, index: int = None):
    conquery_id_elements = conquery_id.split(".")

    if index is None:
        return conquery_id_elements

    return conquery_id_elements[index]


def get_conquery_id_slice(conquery_id: str, first_index: int = None, second_index: int = None, until_then: bool = False,
                          from_then_on: bool = False):
    conquery_id_elements = conquery_id.split(".")
    if second_index is not None and first_index is None:
        raise ValueError("First index must be specified if second index is not None")
    if second_index is not None and first_index > second_index:
        raise ValueError("First index must be greater than second index")

    if first_index is None:
        return conquery_id_elements

    if second_index is not None:
        return conquery_id_elements[first_index:second_index]

    if until_then:
        return conquery_id_elements[:first_index]

    if from_then_on:
        return conquery_id_elements[first_index:]

    raise ValueError("Unexpected variable combination: \n"
                     f"{conquery_id=}\n"
                     f"{first_index=}\n"
                     f"{second_index=}\n"
                     f"{until_then=}\n"
                     f"{from_then_on=}\n")


@check_arg_type(["conquery_id"])
def contains_dataset_id(conquery_id: str):
    return is_dataset_id(get_conquery_id_element(conquery_id, dataset_loc))


@check_arg_type(["conquery_id"])
def add_dataset_id_to_conquery_id(conquery_id: str, dataset_id: str):
    if not is_dataset_id(dataset_id):
        raise ValueError(f"{dataset_id=} is not a valid id.")

    if contains_dataset_id(conquery_id):
        return id_elements_to_id([dataset_id, *get_conquery_id_slice(conquery_id, dataset_loc + 1,
                                                                     from_then_on=True)])

    return id_elements_to_id([dataset_id, *get_conquery_id_slice(conquery_id)])


@check_arg_type(["conquery_id"])
def remove_dataset_id_from_conquery_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       concept_loc,
                                                       from_then_on=True))
    return conquery_id


@check_arg_type(["conquery_id"])
def get_root_concept_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       concept_loc + 1,
                                                       until_then=True))
    else:
        return get_conquery_id_element(conquery_id, concept_loc - 1)


@check_arg_type(["conquery_id"])
def get_connector_id(conquery_id: str):
    if contains_dataset_id(conquery_id):
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       connector_loc + 1,
                                                       until_then=True))
    else:
        return id_elements_to_id(get_conquery_id_slice(conquery_id,
                                                       connector_loc,
                                                       until_then=True))


@check_arg_type(["conquery_id"])
def get_dataset(conquery_id: str):
    if not contains_dataset_id(conquery_id):
        raise ValueError(f"Can not extract dataset for id {conquery_id}.")

    return get_conquery_id_element(conquery_id, dataset_loc)


def child_id_in_concept(child_id: str, concept: dict):
    """Searches for child id in concepts dictionary"""
    for children_ids in [concept_element.get('children', []) for concept_element in concept]:
        if child_id in children_ids:
            return True
    return False


@check_arg_type(["concept_id"])
def find_concept_id(concept_id: str, concepts: dict, concept_obj: dict = None, conquery_url: str = None,
                    conquery_token: str = None):
    """
    Searches for concept_id in concepts or concept_obj. If concept_id is found True is returned.
    If eva access data is defined, concept_obj is loaded for concept_id level 3 or higher
    :param concept_id: concept_id that should be found
    :param concepts: dict with concept_ids
    :param concept_obj: dict with concept data (optional)
    :param conquery_url: url to conquery server to download concept obj (optional)
    :param conquery_token: token to access concept_obj optional)
    """
    concept_id_list = concept_id.split(".")
    root_concept_id = get_root_concept_id(concept_id)

    if len(concept_id_list) < 2:
        raise ValueError(f"Id has wrong shape. Id: {concept_id}")

    if len(concept_id_list) == 2:
        # root concepts can be looked up in concepts object (keys)
        return root_concept_id in concepts.keys()

    if len(concept_id_list) == 3:
        # children of root concepts can be looked up in concepts object
        if root_concept_id not in concepts.keys():
            return False
        return concept_id in concepts.get(root_concept_id).get('children', [])

    if concept_obj is None and conquery_url is None and conquery_token is None:
        raise ValueError(f"For concepts that are on level 3 and higher ({concept_id=}), "
                         f"a concept_object or conquery-request information has to be provided")

    if concept_obj is None:
        dataset_id = get_dataset(concept_id)
        print(f"Establish connection: \n"
              f"{dataset_id=} \n"
              f"{conquery_url=} \n"
              f"{conquery_token} \n"
              f"{root_concept_id}")

        with ConqueryConnection(conquery_url, conquery_token) as cq:
            concept_obj = cq.get_concept(dataset_id, root_concept_id)

    return child_id_in_concept(concept_id, concept_obj)


def is_in_conquery_ids(conquery_id: str, conquery_ids: list):
    return any(is_same_conquery_id(conquery_id, conquery_id_from_list) for conquery_id_from_list in conquery_ids)


def is_same_conquery_id(conquery_id_1: str, conquery_id_2: str, id_separator=conquery_id_separator,
                        can_diff_in_depth=True):
    """Splits ids by 'id_separator' and iterates over both reversed list.
    If 'can_diff_in_depth' is True, comparison between 'age.age_select' and 'age' will be True"""
    if not can_diff_in_depth and conquery_id_1 != conquery_id_2:
        return False

    for id_section_1, id_section_2 in zip(reversed(conquery_id_1.split(id_separator)),
                                          reversed(conquery_id_2.split(id_separator))):
        if id_section_1 != id_section_2:
            return False
    return True
