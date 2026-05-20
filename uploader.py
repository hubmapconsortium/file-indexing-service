import os
from datetime import datetime
from typing import Literal

import boto3


class AWSS3Uploader:

    def __init__(self, bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str, aws_region_name:str):
        self._bucket_name = bucket_name
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
        )

    def upload_file(
        self,
        key: str,
        filepath: str,
        last_modified_at: int,
        storage_class: Literal["DEEP_ARCHIVE", "STANDARD"] = "DEEP_ARCHIVE",
    ) -> str:
        file_size = os.path.getsize(filepath)
        if file_size >= 5 * 1024 * 1024 * 1024:  # 5GB limit in AWS. Must use multipart upload.
            return self._upload_multipart_file(
                key=key,
                filepath=filepath,
                last_modified_at=last_modified_at,
                storage_class=storage_class,
            )
        else:
            return self._upload_file(
                key=key,
                filepath=filepath,
                last_modified_at=last_modified_at,
                storage_class=storage_class,
            )

    def _upload_file(
        self,
        key: str,
        filepath: str,
        last_modified_at: int,
        storage_class: Literal["DEEP_ARCHIVE", "STANDARD"] = "DEEP_ARCHIVE",
    ) -> str:
        with open(filepath, "rb") as f:
            file_data = f.read()
            res = self._s3_client.put_object(
                Bucket=self._bucket_name,
                Body=file_data,
                Key=key,
                StorageClass=storage_class,
                Metadata={"mtime": datetime.fromtimestamp(last_modified_at).isoformat()},
                ChecksumAlgorithm="CRC64NVME",
            )
        return res["VersionId"]

    def _upload_multipart_file(
        self,
        key: str,
        filepath: str,
        last_modified_at: int,
        storage_class: Literal["DEEP_ARCHIVE", "STANDARD"] = "DEEP_ARCHIVE",
    ) -> str:
        upload_id = None
        try:
            part_size = 4 * 1024 * 1024 * 1024  # 4GB
            multipart = self._s3_client.create_multipart_upload(
                Bucket=self._bucket_name,
                Key=key,
                StorageClass=storage_class,
                Metadata={"mtime": datetime.fromtimestamp(last_modified_at).isoformat()},
                ChecksumAlgorithm="CRC64NVME",
                ChecksumType="FULL_OBJECT",
            )
            upload_id = multipart["UploadId"]

            parts = []
            with open(filepath, "rb") as f:
                part_number = 1
                while True:
                    part_data = f.read(part_size)
                    if not part_data:
                        break

                    resp = self._s3_client.upload_part(
                        Bucket=self._bucket_name,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=part_data,
                        ChecksumAlgorithm="CRC64NVME",
                    )
                    parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                    part_number += 1

            complete_resp = self._s3_client.complete_multipart_upload(
                Bucket=self._bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
                ChecksumType="FULL_OBJECT",
            )
            upload_id = None
            return complete_resp["VersionId"]
        finally:
            if upload_id:
                self._s3_client.abort_multipart_upload(
                    Bucket=self._bucket_name,
                    Key=key,
                    UploadId=upload_id,
                )