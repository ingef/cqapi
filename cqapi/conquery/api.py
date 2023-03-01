from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


class Url:
    """There is an url parsing buildin package in Python urllib.parse.urljoin,
    but it seems more complex than we need for this use case"""

    def __init__(self, base: str, *components: str):
        self.base = base
        self.components = components

    def __truediv__(self, component) -> Url:
        return Url(self.base, *[*self.components, component])

    def __str__(self):
        return self.parse()

    def parse(self):
        return f"{self.base}/{'/'.join([component.strip('/') for component in self.components])}"


@dataclass
class ConqueryApiUrls:
    conquery_url: str

    @property
    def api(self) -> Url:
        return Url(self.conquery_url, "api")

    def me(self) -> Url:
        return self.api / "me"

    def datasets(self) -> Url:
        return self.api / "datasets"

    def dataset(self, dataset: str) -> Url:
        return self.datasets() / dataset

    def concepts(self, dataset: str = None) -> Url:
        if dataset is None:
            return self.api / "concepts"
        else:
            return self.dataset(dataset=dataset) / "concepts"

    def concept_id(self, concept_id: str) -> Url:
        return self.concepts() / concept_id

    def queries(self, dataset: str = None) -> Url:
        if dataset is None:
            return self.api / "queries"
        else:
            return self.datasets() / dataset / "queries"

    def query_id(self, query_id: str) -> Url:
        return self.queries() / query_id

    def query_result(self, query_id: str, file_format: str = "csv", file_suffix: Optional[str] = None) -> Url:
        if file_suffix is None:
            if file_format == "arrow":
                file_suffix = ".arrf"
            elif file_format in {"csv", "xlsx"}:
                file_suffix = f".{file_format}"
            else:
                raise ValueError(f"Unknown file format {file_format}")

        return self.api / "result" / file_format / f"{query_id}{file_suffix}"

    def query_reexecute(self, query_id: str) -> Url:
        return self.query_id(query_id=query_id) / "reexecute"

    def form_configs(self, dataset: str = None):
        if dataset is None:
            return self.api / "form-configs"
        else:
            return self.dataset(dataset=dataset) / "form-configs"

    def form_config_id(self, form_config_id: str):
        return self.form_configs() / form_config_id
