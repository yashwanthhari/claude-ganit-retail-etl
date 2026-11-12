import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark + Delta session. Tests using it are skipped if pyspark/delta are missing."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.config import load_config
    from lakehouse.spark_session import get_spark

    session = get_spark(load_config(), app_name="tests", shuffle_partitions=2)
    yield session
    session.stop()
