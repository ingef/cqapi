from cqapi.conquery_ids import ConqueryIdCollection, ConqueryId
from cqapi import ConqueryConnection

eva_token = "dKRILd5JEKwBSuxJ0DhK/ONzy60wPkY7FzpaQQ+WGXOSsR0JB5gl/IPohUmbcC3R/3MLhg6zxDZiD0KjqdnJfF4o0zW7gBBvsd7ZZ/vR22aHyzOrY4813xtfdxMZFnVJUTllPiM4CeU2JFJdK6pznfehc4refCQO1onpR7QJW5d/8LqJrtThPTizZFCCSoFEK94zpWbTRtLgdKhPBQvSgaz6WzPM7+9lpqHRCTESwdUrjSJLj0zDtXeh9V482gSMdT6DiCIxonI0+YlCYLNref1dhMdG2ok2xw/FUK7Cp7hMKDyHwVm72CB+CN9a3vba"
eva_url = "http://lyo-peva02:8080"
conn = ConqueryConnection(eva_url, eva_token)
concepts = conn.get_concepts(dataset="adb_bosch")


def test_label():
    ids = ConqueryIdCollection()

    ids.add(ConqueryId("adb_bosch.icd", "concept"))
    ids.add(ConqueryId("adb_bosch.icd.au_fall_21c.sum_au", "connector_select"))
    ids.add(ConqueryId("adb_bosch.icd.au_fall_21c.krankheitsursache", "filter"))
    ids.add(ConqueryId("adb_bosch.atc.atc", "connector"))

    print(ids.create_label_dicts(concepts=concepts))

    table = ids.print_id_labels_as_table(concepts=concepts)
    print("hi")


test_label()
