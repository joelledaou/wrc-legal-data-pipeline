"""Thin clients for the two stores: MongoDB (metadata) and MinIO (documents)."""

from __future__ import annotations

from minio import Minio
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from wrc_pipeline.config import Settings


def get_mongo_collection(settings: Settings, name: str) -> Collection:
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10_000)
    return client[settings.mongo_db][name]


def ensure_landing_indexes(collection: Collection) -> None:
    """Records are keyed by document URL path (natural dedup key); these
    indexes only serve the common query patterns."""
    collection.create_index([("identifier", ASCENDING)])
    collection.create_index([("partition_date", ASCENDING)])
    collection.create_index([("published_date", ASCENDING)])
    collection.create_index([("body", ASCENDING)])


def get_minio_client(settings: Settings) -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def get_object(client: Minio, bucket: str, key: str) -> bytes:
    response = client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def object_exists(client: Minio, bucket: str, key: str) -> bool:
    try:
        client.stat_object(bucket, key)
        return True
    except Exception:
        return False
