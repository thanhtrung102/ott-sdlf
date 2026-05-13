import os
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def lambda_handler(event, context):
    """Stage A pass-through: validates event format, marks records as processed.
    Actual ETL is performed by Stage B (Glue job).
    """
    try:
        for record in event:
            key = (
                record.get("object", {}).get("key")
                or record.get("detail", {}).get("object", {}).get("key")
                or "unknown"
            )
            logger.info(f"Stage A pass-through: {key}")
            record["processed"] = True
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

    return event
