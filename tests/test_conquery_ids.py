import json
from pathlib import Path
import pandas as pd

from cqapi.conquery_ids import ConqueryIdCollection, DatasetId, ConceptId, ConnectorId, SelectId, DateId, \
    get_dataset_from_id_string, get_copy_of_id_with_changed_dataset, FilterId, ChildId

from cqapi.datasets import set_test_datasets


set_test_datasets()

concepts = dict({
    "dataset1.khfalle": {
        "parent": "dataset1.infonet_leistungsmengen_struc",
        "label": "Krankenhausfälle",
        "description": None,
        "active": True,
        "children": [],
        "additionalInfos": [
            {
                "key": "Leistungsfälle TP4a",
                "value": "Alle Krankenhausfälle (unabhängig, ob vollstationärer, teilstationärer, vorstationärer Fall oder ambulante Behandlung)"
            }
        ],
        "matchingEntries": 100,
        "dateRange": None,
        "tables": [
            {
                "id": "dataset1.kh_fall",
                "connectorId": "dataset1.khfalle.krankenhausf$c3$a4lle",
                "label": "Krankenhausfälle",
                "dateColumn": {
                    "defaultValue": "dataset1.khfalle.krankenhausf$c3$a4lle.entlassungsdatum",
                    "options": [
                        {
                            "label": "Entlassungsdatum",
                            "value": "dataset1.khfalle.krankenhausf$c3$a4lle.entlassungsdatum",
                            "templateValues": None,
                            "optionValue": None
                        },
                        {
                            "label": "Aufnahmedatum",
                            "value": "dataset1.khfalle.krankenhausf$c3$a4lle.aufnahmedatum",
                            "templateValues": None,
                            "optionValue": None
                        },
                        {
                            "label": "Aufenthaltsdauer",
                            "value": "dataset1.khfalle.krankenhausf$c3$a4lle.aufenthaltsdauer",
                            "templateValues": None,
                            "optionValue": None
                        }
                    ]
                },
                "filters": [
                    {
                        "id": "dataset1.khfalle.krankenhausf$c3$a4lle.anzahl_krankenhausf$c3$a4lle",
                        "label": "Anzahl Krankenhausfälle",
                        "type": "INTEGER_RANGE",
                        "unit": "Fälle",
                        "description": "Anzahl Krankenhausfälle",
                        "options": None,
                        "min": 1,
                        "max": None,
                        "template": None,
                        "pattern": None,
                        "allowDropFile": None
                    }
                ],
                "selects": [
                    {
                        "id": "dataset1.khfalle.krankenhausf$c3$a4lle.anzahl_krankenhausfaelle_select",
                        "label": "Anzahl Krankenhausfälle",
                        "description": "Automatisch erzeugter Zusatzwert."
                    }
                ]
            }
        ],
        "detailsAvailable": True,
        "codeListResolvable": False,
        "selects": []
    }})


def test_compare_conquery_ids():
    assert DatasetId("dataset1") == DatasetId("dataset1")
    assert not DatasetId("dataset1") == DatasetId("dataset2")
    assert ConceptId("icd", DatasetId("dataset1")) == ConceptId("icd", DatasetId("dataset1"))
    assert ConceptId("icd", DatasetId("dataset1")) == ConceptId("icd", DatasetId("dataset2"))
    assert ConceptId("icd1", DatasetId("dataset1")) != ConceptId("icd2", DatasetId("dataset1"))


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
    new_id = get_copy_of_id_with_changed_dataset(
        new_dataset="dataset2", conquery_id=ConceptId("concept", DatasetId("dataset1")))
    assert new_id.get_dataset() == "dataset2"


def test_get_id_label():
    assert DateId("entlassungsdatum", ConnectorId("krankenhausf$c3$a4lle",
                                                  ConceptId("khfalle", DatasetId("dataset1")))).get_id_label(concepts) == \
        "Krankenhausfälle - Krankenhausfälle - Entlassungsdatum"

    assert FilterId("anzahl_krankenhausf$c3$a4lle", ConnectorId(
        "krankenhausf$c3$a4lle", ConceptId("khfalle", DatasetId("dataset1")))).get_id_label(concepts) == \
        "Krankenhausfälle - Krankenhausfälle - Anzahl Krankenhausfälle"

    assert ChildId("e14", ChildId("e14-e18", ConceptId("khfalle", DatasetId("dataset1")))).get_id_label(concepts) == \
           "Krankenhausfälle - E14-E18 - E14"


def test_print_id_labels_as_table():
    validation_table = pd.DataFrame(
        {
            "Konzept": ["ATC (WIDO)", "ATC (WIDO)", "ATC (WIDO)", "ICD"],
            "Zusatzwert (Konzept)": ["", "", "ATC (Wido) liegt vor", ""],
            "Quelle": ["", "ATC", "", ""],
            "Zusatzwert (Quelle)": ["", "Anzahl Rezepte", "", ""]
        }
    )

    def get_tests_path() -> Path:
        for path in Path(__file__).parents:
            if path.name == "tests":
                return path
        else:
            return Path("tests")

    test_path = get_tests_path()

    ids = ConqueryIdCollection()
    ids.add(ConceptId("atc_wido", DatasetId("dataset1")))
    ids.add(ConceptId("icd", DatasetId("dataset1")))
    ids.add(SelectId("exists", ConceptId("atc_wido", DatasetId("dataset1"))))
    ids.add(SelectId("anzahl_rezepte", ConnectorId("atc", ConceptId("atc_wido", DatasetId("dataset1")))))

    with open(test_path / "concepts" / "dataset1_concepts.json", "r", encoding="utf-8") as f:
        dataset1_concepts = json.load(f)
        table = ids.print_id_labels_as_table(concepts=dataset1_concepts)
        pd.testing.assert_frame_equal(table, validation_table)
