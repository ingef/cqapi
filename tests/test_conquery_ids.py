from cqapi.conquery_ids import ConqueryIdCollection, DatasetId, ConceptId, ConnectorId, SelectId,\
    get_dataset_from_id_string, get_id_with_changed_dataset
import pytest
from cqapi.datasets import set_test_datasets
set_test_datasets()


def test_compare_conquery_ids():
    assert DatasetId("dataset1").is_same_id(DatasetId("dataset1"))
    assert not DatasetId("dataset1").is_same_id(DatasetId("dataset2"))
    assert ConceptId("icd", DatasetId("dataset1")).is_same_id(ConceptId("icd", DatasetId("dataset1")))


def test_get_concept_id():
    assert ConnectorId("conn", ConceptId("concept", DatasetId("dataset2"))).get_concept_id().id == "dataset2.concept"
    assert ConnectorId("conn", ConceptId("concept", DatasetId("dataset2"))).get_concept_id().id != "dataset1.concept"


def test_get_connector_id():
    assert SelectId("sel", ConnectorId("conn", ConceptId("concept", DatasetId("dataset2")))).get_connector_id().id ==\
           "dataset2.concept.conn"
    assert SelectId("sel", ConnectorId("conn", ConceptId("concept", DatasetId("dataset2")))).get_connector_id().id !=\
           "dataset3.concept.conn"


def test_get_dataset():
    assert ConceptId("concept", DatasetId("dataset2")).get_dataset() == "dataset2"
    assert ConceptId("concept", DatasetId("dataset2")).get_dataset() != "dataset1"


def test_is_in_id_list():
    assert ConceptId("concept", DatasetId("dataset1")).is_in_id_list([ConceptId("concept", DatasetId("dataset1"))])


def test_change_dataset():
    concept_id = ConceptId("concept", DatasetId("dataset1"))
    concept_id.change_dataset(new_dataset="dataset2")
    assert concept_id.get_dataset() == "dataset2"


def test_from_str():
    assert ConceptId.from_str("dataset1.concept") == ConceptId("concept", DatasetId("dataset1"))


def test_get_dataset_from_id_string():
    assert get_dataset_from_id_string("dataset1.concept") == "dataset1"


def test_get_id_with_changed_dataset():
    new_id = get_id_with_changed_dataset(
        new_dataset="dataset2", conquery_id=ConceptId("concept", DatasetId("dataset1")))
    assert new_id.get_dataset() == "dataset2"


@pytest.mark.skip("Needs conquery connection")
def test_label():
    concepts = dict()
    ids = ConqueryIdCollection()

    ids.add(ConceptId("concept", DatasetId("dataset2")))
    ids.add(ConceptId("concept", DatasetId("dataset3")))
    ids.add(SelectId("sel", ConnectorId("conn", ConceptId("concept", DatasetId("dataset2")))))

    ids.create_label_dicts(concepts=concepts)

    ids.print_id_labels_as_table(concepts=concepts)

