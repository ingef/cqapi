from cqapi.api import ConqueryConnection
from cqapi.queries.queries import concept_element_from_concept, wrap_concept_query, add_date_restriction
import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import time
import os
from requests.exceptions import HTTPError
import csv

# defining papermill parameters
eva_url_prod = "http://lyo-peva01:8080"
eva_url_test = "http://lyo-peva02:8080"
eva_token = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"

# adb = ["adb_bosch", "adb_energie", "adb_verbundplus", "adb_novitas", "adb_vbu", "adb_viactiv"]
adb = ["adb_vbu"]
# fdb = ["fdb_demo"]
datasets = [*adb]

# ["2018-01-01", "2020-12-31"]
date_restriction = {
    'teradata': ["2018-01-01", "2020-12-31"],
    '21c': None
}

sleep_between_query_runs = 0.25

execute_prod = True
execute_test = True
only_21c = False
only_adb = False

output_path = Path(f"./eva_stats/{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/")
output_path = Path(f"./eva_stats/test_run")
output_path.mkdir(parents=True, exist_ok=True)
os.chmod(output_path, 0o777)

file_name_all = "adb_vbu.2013-2020_2"


def get_concepts(dataset, eva_url, eva_token):
    with ConqueryConnection(eva_url, eva_token) as cq:
        # get concepts for query creation
        concepts = cq.get_concepts(dataset)
    return concepts


def execute_query(concept_query, dataset, eva_url, eva_token):
    with ConqueryConnection(eva_url, eva_token) as cq:
        # execute query
        try:
            concept_query_id = cq.execute_query(dataset, concept_query)
        except HTTPError:
            print(f"Warning: could not run query \n"
                  f"{concept_query} \n"
                  f"for {dataset=} \n"
                  f"for {eva_url=}")
            return 0, f"{dataset}.failed"
        query_info = cq.get_query_info(dataset, concept_query_id)

        start_time = datetime.datetime.now()
        while query_info.get('status') == 'RUNNING':
            query_info = cq.get_query_info(dataset, concept_query_id)
            if (datetime.datetime.now() - start_time).seconds > 30:
                return 0, f"{dataset}.failed"

    n_results = query_info.get('numberOfResults', -1)

    return int(n_results), concept_query_id


def delete_all_queries(query_ids: list, eva_url, eva_token):
    with ConqueryConnection(eva_url, eva_token) as cq:
        datasets_perm = cq.get_datasets()
        for dataset_perm in datasets_perm:
            stored_queries = cq.get_stored_queries(dataset_perm)
            stored_query_ids = [_["id"] for _ in stored_queries]
            for query_id in query_ids:
                if query_id not in stored_query_ids:
                    continue
                query_id_dataset_perm = ".".join([dataset_perm, *query_id.split(".")[1:]])
                cq.delete_stored_query(dataset_perm, query_id_dataset_perm)


df_columns = ['dataset', 'concept_id', 'Rows Test', 'Rows Prod', 'Diff Rows']
compare_df = pd.DataFrame(columns=df_columns)

csv_dict = {
    "dataset": "",
    "concept_id": "",
    "connector_id": "",
    "Rows Prod": "",
    "Rows Test": "",
    "Diff Rows": ""
}
with (output_path / f'{"test"}.csv').open('w') as f:  # You will need 'wb' mode in Python 2.x
    w = csv.DictWriter(f, csv_dict.keys(), delimiter=";")
    w.writeheader()

compare_df_total = None
for dataset in datasets:

    file_name_individual_db = f"{dataset}_concepts_test"

    # always run this, even if only queries on test eva are executed, since we rely on them later
    concept_url = eva_url_prod if execute_prod else eva_url_test
    concepts = get_concepts(dataset, concept_url, eva_token)
    concepts = {concept_id: concept for (concept_id, concept) in concepts.items() if concept.get('active')}

    if execute_test:
        concepts_test = get_concepts(dataset, eva_url_test, eva_token)
        concepts_test = {concept_id: concept for (concept_id, concept) in concepts_test.items() if
                         concept.get('active')}

    concept_queries = []
    concept_ids = []
    connector_ids = []

    for concept_ind, concept in enumerate(concepts.keys()):
        if concept == "adb_vbu.atc":
            continue
        is_21c_concept = "21c" in concept.lower()
        if only_21c and not is_21c_concept:
            continue
        if only_adb and is_21c_concept:
            continue

        date_restriction_concept = date_restriction.get('21c') if is_21c_concept else date_restriction.get('teradata')

        for connector_id in [table["connectorId"] for table in concepts[concept]["tables"]]:
            query = concept_element_from_concept(concept_ids=[concept],
                                                 concept_object=concepts[concept],
                                                 connector_ids=[connector_id])

            if date_restriction_concept is not None:
                query = add_date_restriction(query,
                                             start_date=date_restriction_concept[0],
                                             end_date=date_restriction_concept[1])

            concept_queries.append(wrap_concept_query(query))
            concept_ids.append(concept)
            connector_ids.append(connector_id.split(".")[2])

    # execute queries
    query_ids_prod = []
    query_ids_test = []
    for concept_query_ind, concept_query in enumerate(concept_queries):
        csv_dict_line = csv_dict.copy()
        concept = concept_ids[concept_query_ind]
        print(f"{concept} - {datetime.datetime.now()}")
        if execute_prod:
            result_prod, query_id_prod = execute_query(concept_query, dataset, eva_url_prod, eva_token)
            query_ids_prod.append(query_id_prod)

            compare_df.loc[concept_query_ind, 'dataset'] = concept.split(".")[0]
            compare_df.loc[concept_query_ind, 'concept_id'] = concept.split(".")[1]
            compare_df.loc[concept_query_ind, 'connector_id'] = connector_ids[concept_query_ind]
            compare_df.loc[concept_query_ind, 'Rows Prod'] = result_prod

            csv_dict_line["dataset"] = concept.split(".")[0]
            csv_dict_line["concept_id"] = concept.split(".")[1]
            csv_dict_line["connector_id"] = connector_ids[concept_query_ind]
            csv_dict_line["Rows Prod"] = result_prod

        if execute_test:
            if concept not in concepts_test.keys():
                compare_df.loc[concept_query_ind, 'Rows Test'] = np.nan
                csv_dict_line['Rows Test'] = np.nan
                continue

            result_test, query_id_test = execute_query(concept_query, dataset, eva_url_test, eva_token)
            query_ids_test.append(query_id_test)

            compare_df.loc[concept_query_ind, 'Rows Test'] = int(result_test)
            csv_dict_line["Rows Test"] = int(result_test)

            if not execute_prod:  # write dataset if only test is active!
                compare_df.loc[concept_query_ind, 'dataset'] = concept.split(".")[0]
                compare_df.loc[concept_query_ind, 'concept_id'] = concept.split(".")[1]
                compare_df.loc[concept_query_ind, 'connector_id'] = connector_ids[concept_query_ind]

                csv_dict_line["dataset"] = concept.split(".")[0]
                csv_dict_line["concept_id"] = concept.split(".")[1]
                csv_dict_line["connector_id"] = connector_ids[concept_query_ind]

        if execute_prod and execute_test:
            compare_df['Diff Rows'] = compare_df['Rows Test'] - compare_df['Rows Prod']
            csv_dict_line['Diff Rows'] = compare_df['Rows Test'] - compare_df['Rows Prod']
        else:
            csv_dict_line["Diff Rows"] = np.nan

        # write to csv
        with (output_path / f'{"test"}.csv').open("a") as file:
            writer = csv.DictWriter(file, csv_dict.keys(), delimiter=';')
            # Write header
            writer.writerow(csv_dict_line)

        compare_df.to_csv(str(output_path / f'{file_name_individual_db}.csv'), index=False)
        time.sleep(sleep_between_query_runs)

    delete_all_queries(query_ids_prod, eva_url_prod, eva_token)
    delete_all_queries(query_ids_test, eva_url_test, eva_token)
    # write info for each dataset
    compare_df.to_csv(str(output_path / f'{file_name_individual_db}.csv'), index=False)

    if compare_df_total is None:
        compare_df_total = compare_df
    else:
        compare_df_total = compare_df_total.append(compare_df)

compare_df_total.to_csv(str(output_path / f'{file_name_all}.csv'), index=False)
