from copy import deepcopy
from cqapi import ConqueryConnection
from cqapi.queries.form_elements import AbsoluteExportForm, RelativeExportForm
import datetime
import time
from typing import List
from cqapi.queries.base_elements import QueryObject, create_query

eva_url = "http://lyo-peva02:8070"
eva_token = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"
conquery_connection = ConqueryConnection(eva_url, eva_token)

dataset = "adb_novitas"

test_psm = True
test_desc_rel = True
test_desc_abs = True
test_pred = True
test_map = True

concepts = conquery_connection.get_concepts(dataset)

start_time = datetime.datetime.now()
counter = 0
for concept_id, concept in concepts.items():
    break
    print(concept_id)
    concept_query = create_query(concept_id=concept_id,
                                 concepts=concepts,
                                 concept_query=True)

    concept_query_id = conquery_connection.execute_query(dataset=dataset, query=concept_query)

    n_results = conquery_connection.get_number_of_results(concept_query_id)

    print(n_results)
    counter += 1
    if counter >= 10:
        break

passed_time_seconds = (datetime.datetime.now() - start_time).seconds
print(f"Time: {passed_time_seconds // 60}:{passed_time_seconds % 60}")

icd_id = f"{dataset}.icd"
icd_c43_id = f"{icd_id}.c00-d48.c43-c44.c43"
icd_c44_id = f"{icd_id}.c00-d48.c43-c44.c44"

# execute c43
date_range_2020 = ["2020-01-01", "2020-12-31"]
date_range_2019 = ["2019-01-01", "2019-12-31"]
c43_concept_query = create_query(concept_id=icd_c43_id, concepts=concepts,
                                 start_date=date_range_2020[0], end_date=date_range_2020[1],
                                 concept_query=True)

print(c43_concept_query)
c43_query_id = conquery_connection.execute_query(c43_concept_query)

# execute c44
c44_concept_query = create_query(concept_id=icd_c44_id, concepts=concepts,
                                 start_date=date_range_2020[0], end_date=date_range_2020[1],
                                 concept_query=True)

c44_query_id = conquery_connection.execute_query(c44_concept_query)

# Execute Export Form
icd_concept_object = create_query(concept_id=icd_id,
                                  concepts=concepts,
                                  connector_ids=[f"{dataset}.icd.kh_diagnose_icd_code"],
                                  connector_select_ids=[f"{dataset}.icd.kh_diagnose_icd_code.anzahl_krankenhaeuser"],
                                  concept_select_ids=[f"{dataset}.icd.exists"])

atc_a_concept_object = create_query(concept_id=f"{dataset}.atc.a",
                                    concepts=concepts)

# Absolute Case

features = [icd_concept_object]

absolute_form_query = AbsoluteExportForm(query_id=c43_query_id,
                                         features=features,
                                         date_range=date_range_2020)
absolute_form_query_id = conquery_connection.execute_query(query=absolute_form_query)
if not conquery_connection.query_succeeded(absolute_form_query_id):
    print("Export Form failed")

# Relative Case
features = [icd_concept_object]

relative_form_query = RelativeExportForm(query_id=c43_query_id,
                                         before_index_queries=features,
                                         after_index_queries=features)
relative_form_query_id = conquery_connection.execute_query(query=relative_form_query)
if not conquery_connection.query_succeeded(relative_form_query_id):
    print("Export Form failed")


# PSM Form
def add_matching_type(queries: List[QueryObject], matching_type: str):
    for query in queries:
        query.row_prefix = matching_type
    return queries


features_psm = add_matching_type(deepcopy(features), "PSM")


def create_psm_form(treatment_dataset: str, treatment_query: str, control_dataset: str, control_query: str,
                    feature_queries_list: list, outcome_queries_list: list,
                    treatment_index_selector: str = "EARLIEST", control_index_selector: str = "EARLIEST",
                    time_unit: str = "QUARTERS", time_count_before: int = 1, time_count_after: int = 1,
                    index_placement: str = "BEFORE", caliper: float = 0.2,
                    matching_partners: int = 1, index_date_matching=None,
                    varq_p_value: str = "VarQ", exclude_dead: bool = True, exclude_costs: int = 100000,
                    granularity: str = "QUARTERS", maps: bool = False):
    return {
        "type": "JUPYEND_FORM@PSM_FORM",
        "title": "",
        "description": "",
        "treatmentGroupIndexSelector": treatment_index_selector,
        "treatmentGroupDatasetId": treatment_dataset,
        "treatmentGroup": treatment_query,
        "controlGroupIndexSelector": control_index_selector,
        "controlGroupDatasetId": control_dataset,
        "controlGroup": control_query,
        "timeUnit": time_unit,
        "timeCountBefore": time_count_before,
        "timeCountAfter": time_count_after,
        "indexPlacement": index_placement,
        "features": feature_queries_list,
        "outcomes": outcome_queries_list,
        "excl_concepts": {"value": "no_exclusion"},
        "caliper": caliper,
        "matchingPartners": matching_partners,
        "indexDateMatching": None,
        "varqPValue": varq_p_value,
        "excludeOutliersDeadAfter": exclude_dead,
        "excludeOutliersMaxMoneyBefore": exclude_costs,
        "excludeOutliersMaxMoneyAfter": exclude_costs,
        "granularity_outcome": granularity,
        "maps": maps
    }


def get_descriptive_group_strat(dataset, query_group: str):
    return {
        "value": "groupStrat",
        "stratGroupDataset": dataset,
        "stratGroup": query_group
    }


def create_absolute_descriptive_form(query_group: str, date_range: list, features: list,
                                     resolution: str = "QUARTERS", stratification: dict = None,
                                     ignore_zeros: bool = False):
    return {
        "type": "JUPYEND_FORM@DESCRIPTIVE_FORM",
        "title": "",
        "description": "",
        "queryGroup": query_group,
        "resolution": resolution,
        "stratification": stratification,
        "timeMode": {
            "value": "ABSOLUTE",
            "dateRange": {
                "min": date_range[0],
                "max": date_range[1]
            },
            "features": features
        },
        "ignoreZeros": ignore_zeros
    }


def create_relative_descriptive_form(query_group: str, features: list,
                                     resolution: str = "QUARTERS", stratification: dict = None,
                                     time_count_before: int = 1, time_count_after: int = 1,
                                     index_placement: str = "BEFORE", index_selector: str = "EARLIEST",
                                     ignore_zeros: bool = False):
    return {
        "type": "JUPYEND_FORM@DESCRIPTIVE_FORM",
        "title": "",
        "description": "",
        "queryGroup": query_group,
        "resolution": resolution,
        "stratification": stratification,
        "timeMode": {
            "value": "RELATIVE",
            "timeCountBefore": time_count_before,
            "timeCountAfter": time_count_after,
            "indexSelector": index_selector,
            "indexPlacement": index_placement,
            "variableStratTime": index_placement,
            "features": features
        },
        "ignoreZeros": ignore_zeros
    }


def create_map_form_query(query_group: str, date_range: list, features: list, aggregation_mode: str = "mean",
                          resolution: str = "QUARTERS", region: str = "DEUTSCHLAND", granularity: str = "states"):
    return {
        "type": "JUPYEND_FORM@MAP_FORM",
        "title": "",
        "description": "",
        "region": region,
        "granularity": granularity,
        "queryGroup": query_group,
        "features": features,
        "aggregation_mode": aggregation_mode,
        "resolution": resolution,
        "dateRange": {
            "min": date_range[0],
            "max": date_range[1]
        }
    }


def create_pred_form_classifier_var(query: dict):
    return {
        "value": "classifier_variable",
        "classifier_variable_concept": [query]
    }


def create_pred_form_stamm_features(concepts: str = "age_gender"):
    return {"value": "stamm", "stamm_select": concepts}


def create_pred_form_atc_features(level: str = "level_1", select: str = "atc.atc.exists"):
    return {
        "value": "atc",
        "atc_level": level,
        "atc_select": select
    }


def create_pred_form_icd_features():
    pass


def create_pred_form_ebm_de_features():
    pass


def create_pred_form_ops_features(level: str = "level_1", select: str = "ops.arzt+kh.exists"):
    return {
        "value": "ops",
        "ops_level": level,
        "ops_select": select
    }


def create_pred_form_analysis_group_absolute(query_group: str, date_range_analysis: list):
    return {
        "value": "absoluteAnalyseGroup",
        "analyseGroup": query_group,
        "dateRange_analysis": {
            "min": date_range_analysis[0],
            "max": date_range_analysis[1]
        }
    }


def create_pred_form_absolute(training_group: str, classifier: dict, date_range_training: list,
                              date_range_classifier: list,
                              classifier_model: str = "random_forest", stamm_features: dict = None,
                              atc_features: dict = None, icd_features: dict = None, ebm_de_features: dict = None,
                              ops_features: dict = None, additional_features: list = None,
                              analysis_group: dict = None, model_parameters: dict = None):
    if stamm_features is None:
        stamm_features = {"value": "no_stamm"}
    if atc_features is None:
        atc_features = {"value": "no_atv"}
    if icd_features is None:
        icd_features = {"value": "no_icd"}
    if ebm_de_features is None:
        ebm_de_features = {"value": "no_ebm_de"}
    if additional_features is None:
        additional_features = []
    if analysis_group is None:
        analysis_group = {"value": "noAnalyseGroup"}
    if model_parameters is None:
        model_parameters = {"value": "noModelParameters"}

    return {
        "type": "JUPYEND_FORM@PRED_FORM",
        "title": "",
        "description": "",
        "prediction_model": classifier_model,
        "trainingGroup": training_group,
        "classifier": classifier,
        "stamm_features": stamm_features,
        "atc_features": atc_features,
        "icd_features": icd_features,
        "ebm_de_features": ebm_de_features,
        "ops_features": ops_features,
        "additional_features": additional_features,
        "exclusion_features": [],
        "formType_tab": {
            "value": "absolute",
            "dateRange_classifier": {
                "min": date_range_classifier[0],
                "max": date_range_classifier[1]
            },
            "dateRange_training": {
                "min": date_range_training[0],
                "max": date_range_training[1]
            }
        },
        "analyseGroup_tab": analysis_group,
        "modelParameters": model_parameters
    }


executed_queries = dict()

features = [feature.to_dict() for feature in features]
features_psm = [feature_psm.to_dict() for feature_psm in features_psm]
# execute psm form
if test_psm:
    psm_form_query = create_psm_form(treatment_dataset=dataset, treatment_query=c43_query_id,
                                     control_dataset=dataset, control_query=c44_query_id,
                                     feature_queries_list=features_psm, outcome_queries_list=features)
    executed_queries["PSM"] = conquery_connection.execute_query(psm_form_query, dataset=dataset)


# execute descriptive forms
if test_desc_abs:
    desc_abs_form_query = create_absolute_descriptive_form(query_group=c43_query_id, date_range=date_range_2020,
                                                           features=features,
                                                           stratification=get_descriptive_group_strat(dataset,
                                                                                                      c44_query_id))
    executed_queries["DESC_ABS"] = conquery_connection.execute_query(desc_abs_form_query, dataset=dataset)

if test_desc_rel:
    desc_rel_form_query = create_relative_descriptive_form(query_group=c43_query_id,
                                                           features=features,
                                                           stratification=get_descriptive_group_strat(dataset,
                                                                                                      c44_query_id))
    executed_queries["DESC_REL"] = conquery_connection.execute_query(desc_rel_form_query, dataset=dataset)

# execute map_forms
if test_map:
    map_form_query = create_map_form_query(query_group=c43_query_id, date_range=date_range_2020, features=features)
    executed_queries["MAP"] = conquery_connection.execute_query(map_form_query, dataset=dataset)

# prediction
if test_pred:
    pred_form_query = create_pred_form_absolute(training_group=c43_query_id,
                                                classifier=create_pred_form_classifier_var(atc_a_concept_object.to_dict()),
                                                date_range_training=date_range_2019,
                                                date_range_classifier=date_range_2020,
                                                stamm_features=create_pred_form_stamm_features(),
                                                atc_features=create_pred_form_atc_features(),
                                                ops_features=create_pred_form_ops_features(),
                                                analysis_group=create_pred_form_analysis_group_absolute(c44_query_id,
                                                                                                        date_range_2020))
    executed_queries["PRED"] = conquery_connection.execute_query(pred_form_query, dataset=dataset)

# evaluate if queries succeeded or failed
while len(executed_queries.keys()) > 0:
    for executed_query_label, executed_query_id in executed_queries.copy().items():
        response = conquery_connection.get_query_info(executed_query_id)
        if response["status"] == "RUNNING":
            continue

        if response["status"] == "DONE":
            print(f"{executed_query_label} succeeded")
            executed_queries.pop(executed_query_label)
            continue
        if response["status"] == "FAILED":
            print(f"{executed_query_label} failed")
            executed_queries.pop(executed_query_label)
            continue
        if response["status"] == "NEW":
            print(f"Status 'NEW' - Query failed")
            print(f"{response=}")
            executed_queries.pop(executed_query_label)
            continue
        print(f"Unknown status {response['status']}")
        print(f"{response=}")
        executed_queries.pop(executed_query_label)

        time.sleep(2)
