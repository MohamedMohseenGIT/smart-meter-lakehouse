import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841 - Spark tests are skipped when PySpark is absent
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]")
               .appName("smartmeter-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
