from cqapi.queries.validate import validate_date
import pytest


def test_validate_date():
    validate_date("2007-02-02")

    with pytest.raises(ValueError):
        validate_date("200-02-02")
    with pytest.raises(ValueError):
        validate_date("2000-00-31")

    with pytest.raises(ValueError):
        validate_date("2000-13-31")

    with pytest.raises(ValueError):
        validate_date("2000-12-32")

test_validate_date()