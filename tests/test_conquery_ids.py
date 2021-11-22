from cqapi.conquery_ids import ConqueryIdCollection, ConqueryId, DatasetId, ConceptId, ConnectorId, SelectId, DateId, \
    ChildId
import pytest
from cqapi.datasets import set_test_datasets

set_test_datasets()


def test_compare_conquery_ids():
    assert DatasetId("dataset1").is_same_conquery_id(DatasetId("dataset1"))
    assert not DatasetId("dataset1").is_same_conquery_id(DatasetId("dataset2"))
    assert ConceptId("icd", DatasetId("dataset1")).is_same_conquery_id(ConceptId("icd", DatasetId("dataset1")))

def test_get_concept_id():
    assert ConnectorId("conn", ConceptId("concept", DatasetId("dataset2"))).get_concept_id() == "dataset2.concept"


def test_get_connector_id():
    assert SelectId("sel", ConnectorId("conn", ConceptId("concept", DatasetId("dataset2")))).get_connector_id() ==\
           "dataset2.concept.conn"

def test_get_dataset():
    assert ConceptId("concept", DatasetId("dataset2")).get_dataset() == "dataset2"

@pytest.mark.skip("Needs conquery connetion")
def test_label():
    concepts = dict()
    ids = ConqueryIdCollection()

    ids.add(ConceptId("concept", DatasetId("dataset2")))
    ids.add(ConceptId("concept", DatasetId("dataset3")))
    ids.add(SelectId("sel", ConnectorId("conn", ConceptId("concept", DatasetId("dataset2")))))

    ids.create_label_dicts(concepts=concepts)

    ids.print_id_labels_as_table(concepts=concepts)
