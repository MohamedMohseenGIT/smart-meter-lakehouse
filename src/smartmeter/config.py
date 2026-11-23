"""Single place that knows how catalog, schemas, volumes and tables are named.

Every notebook, pipeline and job resolves names through `LakehouseNames`, so moving between
environments (dev schemas are prefixed per developer by Databricks Asset Bundles) only changes
parameters, never code.
"""

from __future__ import annotations

from dataclasses import dataclass

PARAMS = ("catalog", "landing_schema", "bronze_schema", "silver_schema", "gold_schema", "ml_schema", "ops_schema")


@dataclass(frozen=True)
class LakehouseNames:
    catalog: str
    landing_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    ml_schema: str
    ops_schema: str
    volume: str = "raw"

    # ---- paths -------------------------------------------------------------------------------
    @property
    def raw_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.volume}"

    def raw_path(self, source: str) -> str:
        return f"{self.raw_root}/{source}"

    # ---- tables ------------------------------------------------------------------------------
    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    def ml(self, name: str) -> str:
        return f"{self.catalog}.{self.ml_schema}.{name}"

    def ops(self, table: str) -> str:
        return f"{self.catalog}.{self.ops_schema}.{table}"

    # ---- constructors ------------------------------------------------------------------------
    @classmethod
    def from_mapping(cls, values: dict) -> LakehouseNames:
        missing = [p for p in PARAMS if not values.get(p)]
        if missing:
            raise ValueError(f"Missing lakehouse parameters: {missing}")
        return cls(**{p: values[p] for p in PARAMS})

    @classmethod
    def from_widgets(cls, dbutils) -> LakehouseNames:
        """Notebook tasks receive job parameters as widgets."""
        for p in PARAMS:
            dbutils.widgets.text(p, "")
        return cls.from_mapping({p: dbutils.widgets.get(p) for p in PARAMS})

    @classmethod
    def from_spark_conf(cls, spark, prefix: str = "smartmeter.") -> LakehouseNames:
        """Lakeflow Declarative Pipelines receive parameters through the pipeline configuration."""
        return cls.from_mapping({p: spark.conf.get(prefix + p, None) for p in PARAMS})
