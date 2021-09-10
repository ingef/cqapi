from cqapi.queries.editor import *
from cqapi.queries import check_concept_ids_in_concepts_for_new_dataset


def test_check_concept_ids_in_concepts_for_new_dataset():
    if False:
        eva_token = "conqueryToken"

        concept_ids = ["dataset1.icd", "dataset1.icd.a", "dataset1.icd.a00-b99.a00-a09.a01.a01_2", "dataset1.icd.a.drop", "dataset1.atc.b"]
        conquery_conn = ConqueryConnection("http://lyo-peva02.spectrumk.ads:8070",eva_token)

        found_children = check_concept_ids_in_concepts_for_new_dataset(concept_ids=concept_ids,
                                                      new_dataset="adb_bosch",
                                                      conquery_conn=conquery_conn)
        found_children_val = ["adb_bosch.icd", "adb_bosch.icd.a00-b99.a00-a09.a01.a01_2", "adb_bosch.atc.b"]
        assert len(found_children) == len(found_children_val)
        assert set(found_children) == set(found_children_val)
