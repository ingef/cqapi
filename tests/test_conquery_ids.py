from cqapi.conquery_ids import ConqueryIdCollection, ConqueryId
import cqapi.conquery_ids as con
import pytest
import cqapi.datasets

cqapi.datasets.set_dataset_list(["dataset1", "dataset2"])


def test_compare_conquery_ids():
    assert con.is_same_conquery_id("age", "age")
    assert not con.is_same_conquery_id("age", "not_age")

    assert con.is_same_conquery_id("age.age_select", "age.age_select")
    assert not con.is_same_conquery_id("age", "age.age_select")
    assert not con.is_same_conquery_id("age.not_select", "age")

    assert con.is_same_conquery_id("dataset1.age.age.age_select",
                                   "dataset2.age.age.age_select")


def test_contains_dataset_id():
    assert con.contains_dataset_id("dataset2.age.age.age_select")
    assert not con.contains_dataset_id("age.age")


def test_id_elements_to_id():
    assert con.id_elements_to_id(["foo", "bar"]) == f"foo{con.conquery_id_separator}bar"


def test_get_conquery_id_element():
    assert con.get_conquery_id_element("foo.bar.foo2.bar2", 0) == "foo"


def test_get_conquery_id_slice():
    assert con.get_conquery_id_slice("foo.bar.foo2.bar2") == ["foo", "bar", "foo2", "bar2"]
    assert con.get_conquery_id_slice("foo.bar.foo2.bar2", 2, until_then=True) == ["foo", "bar"]
    assert con.get_conquery_id_slice("foo.bar.foo2.bar2", 2, from_then_on=True) == ["foo2", "bar2"]
    assert con.get_conquery_id_slice("foo.bar.foo2.bar2", 1, 3) == ["bar", "foo2"]
    with pytest.raises(ValueError):
        con.get_conquery_id_slice("foo.bar.foo2.bar2", 2, 1)


def test_add_dataset_id_to_conquery_id():
    with pytest.raises(ValueError):
        con.add_dataset_id_to_conquery_id("foo", "unknown_dataset")
    assert con.add_dataset_id_to_conquery_id("foo", "dataset2") == "dataset2.foo"
    assert con.add_dataset_id_to_conquery_id("dataset1.foo", "dataset2") == "dataset2.foo"


def test_remove_dataset_id_from_conquery_id():
    assert con.remove_dataset_id_from_conquery_id("foo") == "foo"
    assert con.remove_dataset_id_from_conquery_id("dataset2.foo") == "foo"


def test_get_root_concept_id():
    assert con.get_root_concept_id("dataset2.concept.connector.select") == "dataset2.concept"
    assert con.get_root_concept_id("concept.connector.select") == "concept"


def test_get_connector_id():
    assert con.get_connector_id("dataset2.concept.connector.select") == "dataset2.concept.connector"
    assert con.get_connector_id("concept.connector.select") == "concept.connector"


def test_get_dataset():
    with pytest.raises(ValueError):
        con.get_dataset("concept.connector.select")
    assert con.get_dataset("dataset2.concept.connector.select") == "dataset2"


def test_label():
    if False:
        concepts = dict()
        ids = ConqueryIdCollection()

        ids.add(ConqueryId("dataset1.icd", "concept"))
        ids.add(ConqueryId("dataset1.icd.au_fall_21c.sum_au", "connector_select"))
        ids.add(ConqueryId("dataset1.icd.au_fall_21c.krankheitsursache", "filter"))
        ids.add(ConqueryId("dataset1.atc.atc", "connector"))

        print(ids.create_label_dicts(concepts=concepts))

        table = ids.print_id_labels_as_table(concepts=concepts)
