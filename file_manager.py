import re
from logging import Logger
from typing import Optional

import yaml
from neo4j import Record
from requests import Session

IVT_DIRECTORY_SCHEMAS_URL = "https://api.github.com/repos/hubmapconsortium/ingest-validation-tools/contents/src/ingest_validation_tools/directory-schemas"

TIMEOUT = 30  # seconds


class FileManager:
    def __init__(
        self,
        ingest_api_url: str,
        ubkg_url: str,
        ubkg_application_context: str,
        token: str,
        session: Session,
        logger: Logger,
    ):
        self._ingest_api_url = ingest_api_url
        self._token = token
        self._session = session
        self._logger = logger
        self._ivt_directory_schemas = None
        self._primary_schemas = dict()
        self._processed_files = (
            "",  # current dataset UUID
            dict(),  # mapping of file paths to descriptions
        )
        self._dataset_type_hierarchy_map = None # self._get_dataset_type_hierarchy(ubkg_url, ubkg_application_context, session)

    def get_additional_info(self, dataset: Record, path: str) -> Optional[dict]:
        if dataset["creation_action"] == "Create Dataset Activity":
            # primary dataset
            info = {"data_class": "Primary Dataset"}

            # get the description and is_qa_qc from the latest IVT file schema
            try:
                last_file_schema = self._get_latest_file_schema(dataset["uuid"])
                for item in last_file_schema:
                    if re.match(item["pattern"], path):
                        info["description"] = item.get("description")
                        if "is_qa_qc" in item:
                            info["is_qa_qc"] = str(item["is_qa_qc"]).title()
                        break
            except Exception as e:
                self._logger.error(f"Error fetching description for dataset {dataset['uuid']}: {e}")

            # get the analyte class and assay_input_entity from the dataset type
            try:
                metadata = dataset.get("metadata") or {}
                analyte_class = metadata.get("analyte_class")
                if analyte_class:
                    info["analyte_class"] = analyte_class

                assay_input_entity = metadata.get("assay_input_entity")
                if assay_input_entity:
                    info["assay_input_entity"] = assay_input_entity
            except Exception as e:
                self._logger.error(
                    f"Error fetching analyte_class for dataset {dataset['uuid']}: {e}"
                )

            # get the dataset type hierarchy if supported i.e. for SN but not HM
            if self._dataset_type_hierarchy_map:
                try:
                    if dataset["dataset_type"] in self._dataset_type_hierarchy_map:
                        info["dataset_type_hierarchy"] = {
                            "first_level": self._dataset_type_hierarchy_map[dataset["dataset_type"]],
                            "second_level": dataset["dataset_type"],
                        }
                    else:
                        info["dataset_type_hierarchy"] = {
                            "first_level": dataset["dataset_type"],
                            "second_level": dataset["dataset_type"],
                        }
                except Exception as e:
                    self._logger.error(
                        f"Error fetching dataset_type_hierarchy for dataset {dataset['uuid']}: {e}"
                    )

            return info

        elif dataset["creation_action"] == "Central Process":
            # processed dataset
            if self._processed_files[0] != dataset["uuid"]:
                # cache the processed files for this dataset
                # in the format {rel_path: dict(description, is_data_product, is_qa_qc, data_class)}
                self._processed_files = (
                    dataset["uuid"],
                    {
                        f["rel_path"]: {
                            "description": f["description"],
                            "is_data_product": str(f.get("is_data_product", False)).title(),
                            "is_qa_qc": str(f.get("is_qa_qc", False)).title(),
                        }
                        for f in (dataset.get("files") or [])
                    },
                )

            # get the info for the given path (rel_path = path)
            info = self._processed_files[1].get(path, {})
            info["data_class"] = "Processed Dataset"

            return info

        else:
            raise Exception(f"Unknown creation_action: {dataset['creation_action']}")

    def _get_latest_file_schema(self, dataset_uuid: str) -> list[dict]:
        res = self._session.get(
            f"{self._ingest_api_url}/assaytype/{dataset_uuid}",
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=TIMEOUT,
        )
        if res.status_code != 200:
            raise Exception(f"Failed to fetch dataset info for UUID: {dataset_uuid}")
        dir_schema = res.json().get("dir-schema")
        if not dir_schema:
            raise Exception(f"No dir-schema found for Dataset UUID: {dataset_uuid}")

        if self._ivt_directory_schemas is None:
            # get the list of schema files from the IVT and cache them
            res = self._session.get(IVT_DIRECTORY_SCHEMAS_URL, timeout=TIMEOUT)
            if res.status_code != 200:
                raise Exception("Failed to fetch IVT directory-schemas")
            self._ivt_directory_schemas = {
                file["name"]: file["download_url"]
                for file in res.json()
                if file["name"].endswith(".yaml")
            }

        # find all schema files that start with dir_schema
        matching_keys = [k for k in self._ivt_directory_schemas.keys() if k.startswith(dir_schema)]
        if not matching_keys:
            raise Exception(f"No schemas found for prefix: {dir_schema}")

        # find the latest version of the schema, download it if not already cached
        latest_schema = max(matching_keys, key=self._extract_version)
        if latest_schema not in self._primary_schemas:
            schema = self._fetch_schema(latest_schema)
            schema = list(reversed(schema))
            self._primary_schemas[latest_schema] = schema

        return self._primary_schemas[latest_schema]

    def _extract_version(self, filename: str) -> int:
        parts = filename.split(".")
        return int(parts[1])

    def _fetch_schema(self, latest_schema: str) -> list[dict]:
        if self._ivt_directory_schemas is None:
            raise Exception("IVT directory schemas not initialized")

        download_url = self._ivt_directory_schemas.get(latest_schema)
        if not download_url:
            raise Exception(f"No download URL found for schema: {latest_schema}")

        res = self._session.get(download_url, timeout=TIMEOUT)
        if res.status_code != 200:
            raise Exception(f"Failed to download schema: {latest_schema}")

        content = yaml.safe_load(res.text)
        if isinstance(content, str):
            # this is a symlink where just another schema name is provided
            return self._fetch_schema(content)
        return content.get("files", [])

    def _get_dataset_type_hierarchy(self, ubkg_url: str, ubkg_application_context: str, session: Session) -> dict[str, list[str]]:
        res = session.get(f"{ubkg_url}/dataset-types?application_context={ubkg_application_context}", timeout=TIMEOUT)
        if res.status_code != 200:
            msg = f"Error fetching UBKG dataset types: {res.status_code}"
            raise Exception(msg)

        return {
            item["dataset_type"]: item["dataset_modalities"]
            for item in res.json()
            if item["dataset_modalities"]
        }
