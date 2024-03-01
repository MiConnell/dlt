import os
from copy import deepcopy
from typing import Iterator, Dict, Any, List
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    PARTITION_HINT,
    CLUSTER_HINT,
    TABLE_DESCRIPTION_HINT,
    ROUND_HALF_EVEN_HINT,
    ROUND_HALF_AWAY_FROM_ZERO_HINT,
    TABLE_EXPIRATION_HINT,
)

import google
import pytest
import sqlfluff
from google.cloud.bigquery import Table

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TColumnHint
from dlt.common.utils import custom_environ
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient
from dlt.destinations.impl.bigquery.bigquery_adapter import bigquery_adapter
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.extract import DltResource
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    drop_active_pipeline_data,
)
from tests.load.utils import TABLE_UPDATE, sequence_generator, empty_schema


def test_configuration() -> None:
    os.environ["MYBG__CREDENTIALS__CLIENT_EMAIL"] = "1234"
    os.environ["MYBG__CREDENTIALS__PRIVATE_KEY"] = "1234"
    os.environ["MYBG__CREDENTIALS__PROJECT_ID"] = "1234"

    # check names normalised
    with custom_environ({"MYBG__CREDENTIALS__PRIVATE_KEY": "---NO NEWLINE---\n"}):
        c = resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults(), sections=("mybg",))
        assert c.private_key == "---NO NEWLINE---\n"

    with custom_environ({"MYBG__CREDENTIALS__PRIVATE_KEY": "---WITH NEWLINE---\n"}):
        c = resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults(), sections=("mybg",))
        assert c.private_key == "---WITH NEWLINE---\n"


@pytest.fixture
def gcp_client(empty_schema: Schema) -> BigQueryClient:
    # return a client without opening connection
    creds = GcpServiceAccountCredentialsWithoutDefaults()
    creds.project_id = "test_project_id"
    # noinspection PydanticTypeChecker
    return BigQueryClient(
        empty_schema,
        BigQueryClientConfiguration(
            dataset_name=f"test_{uniq_id()}", credentials=creds  # type: ignore[arg-type]
        ),
    )


def test_create_table(gcp_client: BigQueryClient) -> None:
    # non existing table
    # Add BIGNUMERIC column
    table_update = TABLE_UPDATE + [
        {
            "name": "col_high_p_decimal",
            "data_type": "decimal",
            "precision": 76,
            "scale": 0,
            "nullable": False,
        },
        {
            "name": "col_high_s_decimal",
            "data_type": "decimal",
            "precision": 38,
            "scale": 24,
            "nullable": False,
        },
    ]
    sql = gcp_client._get_table_update_sql("event_test_table", table_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` INTEGER NOT NULL" in sql
    assert "`col2` FLOAT64 NOT NULL" in sql
    assert "`col3` BOOLEAN NOT NULL" in sql
    assert "`col4` TIMESTAMP NOT NULL" in sql
    assert "`col5` STRING " in sql
    assert "`col6` NUMERIC(38,9) NOT NULL" in sql
    assert "`col7` BYTES" in sql
    assert "`col8` BIGNUMERIC" in sql
    assert "`col9` JSON NOT NULL" in sql
    assert "`col10` DATE" in sql
    assert "`col11` TIME" in sql
    assert "`col1_precision` INTEGER NOT NULL" in sql
    assert "`col4_precision` TIMESTAMP NOT NULL" in sql
    assert "`col5_precision` STRING(25) " in sql
    assert "`col6_precision` NUMERIC(6,2) NOT NULL" in sql
    assert "`col7_precision` BYTES(19)" in sql
    assert "`col11_precision` TIME NOT NULL" in sql
    assert "`col_high_p_decimal` BIGNUMERIC(76,0) NOT NULL" in sql
    assert "`col_high_s_decimal` BIGNUMERIC(38,24) NOT NULL" in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITION BY" not in sql


def test_alter_table(gcp_client: BigQueryClient) -> None:
    # existing table has no columns
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == 1
    assert "event_test_table" in sql
    assert "ADD COLUMN `col1` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql
    assert "ADD COLUMN `col3` BOOLEAN NOT NULL" in sql
    assert "ADD COLUMN `col4` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5` STRING" in sql
    assert "ADD COLUMN `col6` NUMERIC(38,9) NOT NULL" in sql
    assert "ADD COLUMN `col7` BYTES" in sql
    assert "ADD COLUMN `col8` BIGNUMERIC" in sql
    assert "ADD COLUMN `col9` JSON NOT NULL" in sql
    assert "ADD COLUMN `col10` DATE" in sql
    assert "ADD COLUMN `col11` TIME" in sql
    assert "ADD COLUMN `col1_precision` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col4_precision` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5_precision` STRING(25)" in sql
    assert "ADD COLUMN `col6_precision` NUMERIC(6,2) NOT NULL" in sql
    assert "ADD COLUMN `col7_precision` BYTES(19)" in sql
    assert "ADD COLUMN `col11_precision` TIME NOT NULL" in sql
    # table has col1 already in storage
    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = gcp_client._get_table_update_sql("event_test_table", mod_table, True)[0]
    assert "ADD COLUMN `col1` INTEGER NOT NULL" not in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql


def test_create_table_with_partition_and_cluster(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[9]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    # clustering must be the last
    assert sql.endswith("CLUSTER BY `col2`, `col5`")
    assert "PARTITION BY `col10`" in sql


def test_double_partition_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["partition"] = True
    # double partition
    with pytest.raises(DestinationSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", mod_update, False)
    assert excc.value.columns == ["`col4`", "`col5`"]


def test_create_table_with_time_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[3]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY DATE(`col4`)" in sql


def test_create_table_with_date_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[9]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY `col10`" in sql


def test_create_table_with_integer_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY RANGE_BUCKET(`col1`, GENERATE_ARRAY(-172800000, 691200000, 86400))" in sql


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_date(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={"my_date_column": {"data_type": "date", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.Date]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000).date(),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_date(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={"my_date_column": {"data_type": "date", "partition": False, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.Date]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000).date(),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_timestamp(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {"data_type": "timestamp", "partition": True, "nullable": False}
        },
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.DateTime]]:
        for i in range(10):
            yield {
                "my_timestamp_column": pendulum.from_timestamp(1700784000 + i * 50_000),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_timestamp(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {"data_type": "timestamp", "partition": False, "nullable": False}
        },
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.DateTime]]:
        for i in range(10):
            yield {
                "my_timestamp_column": pendulum.from_timestamp(1700784000 + i * 50_000),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_integer(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_integer(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "partition": False, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions


@pytest.fixture(autouse=True)
def drop_bigquery_schema() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_adapter_no_hints_parsing() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    assert some_data.columns == {
        "int_col": {"name": "int_col", "data_type": "bigint"},
    }


def test_adapter_hints_parsing_partitioning_more_than_one_column() -> None:
    @dlt.resource(
        columns=[{"name": "col1", "data_type": "bigint"}, {"name": "col2", "data_type": "bigint"}]
    )
    def some_data() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(3)]

    assert some_data.columns == {
        "col1": {"data_type": "bigint", "name": "col1"},
        "col2": {"data_type": "bigint", "name": "col2"},
    }

    with pytest.raises(ValueError, match="^`partition` must be a single column name as a string.$"):
        bigquery_adapter(some_data, partition=["col1", "col2"])


def test_adapter_hints_parsing_partitioning() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, partition="int_col")
    assert some_data.columns == {
        "int_col": {"name": "int_col", "data_type": "bigint", "x-bigquery-partition": True},
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_partitioning(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "bigint"}])
    def no_hints() -> Iterator[Dict[str, int]]:
        yield from [{"col1": i} for i in range(10)]

    hints = bigquery_adapter(no_hints._clone(new_name="hints"), partition="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        assert not no_hints_table.range_partitioning, "`no_hints` table IS clustered on a column."

        if not hints_table.range_partitioning:
            raise ValueError("`hints` table IS NOT clustered on a column.")
        else:
            assert (
                hints_table.range_partitioning.field == "col1"
            ), "`hints` table IS NOT clustered on column `col1`."


def test_adapter_hints_parsing_round_half_away_from_zero() -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    bigquery_adapter(hints, round_half_away_from_zero="col1")

    assert hints.columns == {
        "col1": {
            "name": "col1",
            "data_type": "wei",
            "x-bigquery-round-half-away-from-zero": True,
        },
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_round_half_away_from_zero(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def no_hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints._clone(new_name="hints"), round_half_away_from_zero="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        with c.execute_query("""
                SELECT table_name, rounding_mode
                FROM `INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name IN ('no_hints', 'hints')
                  AND column_name = 'col1';""") as cur:
            results = cur.fetchall()

            hints_rounding_mode = None
            no_hints_rounding_mode = None

            for row in results:
                if row["table_name"] == "no_hints":  # type: ignore
                    no_hints_rounding_mode = row["rounding_mode"]  # type: ignore
                elif row["table_name"] == "hints":  # type: ignore
                    hints_rounding_mode = row["rounding_mode"]  # type: ignore

            assert (no_hints_rounding_mode is None) and (
                hints_rounding_mode == "ROUND_HALF_AWAY_FROM_ZERO"
            )


def test_adapter_hints_parsing_round_half_even() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, float]]:
        yield from [{"double_col": float(i)} for i in range(3)]

    bigquery_adapter(some_data, round_half_even="double_col")
    assert some_data.columns == {
        "double_col": {
            "name": "double_col",
            "data_type": "double",
            "x-bigquery-round-half-even": True,
        },
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_round_half_even(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def no_hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints._clone(new_name="hints"), round_half_even="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        with c.execute_query("""
                SELECT table_name, rounding_mode
                FROM `INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name IN ('no_hints', 'hints')
                  AND column_name = 'col1';""") as cur:
            results = cur.fetchall()

            hints_rounding_mode = None
            no_hints_rounding_mode = None

            for row in results:
                if row["table_name"] == "no_hints":  # type: ignore
                    no_hints_rounding_mode = row["rounding_mode"]  # type: ignore
                elif row["table_name"] == "hints":  # type: ignore
                    hints_rounding_mode = row["rounding_mode"]  # type: ignore

            assert (no_hints_rounding_mode is None) and (hints_rounding_mode == "ROUND_HALF_EVEN")


def test_adapter_hints_parsing_clustering() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, cluster="int_col")
    assert some_data.columns == {
        "int_col": {"name": "int_col", "data_type": "bigint", "x-bigquery-cluster": True},
    }


def test_adapter_hints_parsing_multiple_clustering() -> None:
    @dlt.resource(
        columns=[{"name": "col1", "data_type": "bigint"}, {"name": "col2", "data_type": "text"}]
    )
    def some_data() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": i, "col2": str(i)} for i in range(10)]

    bigquery_adapter(some_data, cluster=["col1", "col2"])
    assert some_data.columns == {
        "col1": {"name": "col1", "data_type": "bigint", "x-bigquery-cluster": True},
        "col2": {"name": "col2", "data_type": "text", "x-bigquery-cluster": True},
    }


def test_adapter_hints_merge() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(10)]

    bigquery_adapter(hints, cluster=["col1"])
    bigquery_adapter(hints, partition="col2")

    assert hints.columns == {
        "col1": {"name": "col1", "data_type": "text", CLUSTER_HINT: True},
        "col2": {"name": "col2", "data_type": "bigint", PARTITION_HINT: True},
    }


def test_adapter_hints_unset() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(10)]

    bigquery_adapter(hints, partition="col1")
    bigquery_adapter(hints, partition="col2")

    assert hints.columns == {
        "col1": {"name": "col1", "data_type": "text"},
        "col2": {"name": "col2", "data_type": "bigint", PARTITION_HINT: True},
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_multiple_clustering(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "bigint"},
            {"name": "col2", "data_type": "text"},
            {"name": "col3", "data_type": "text"},
            {"name": "col4", "data_type": "text"},
        ]
    )
    def no_hints() -> Iterator[Dict[str, Any]]:
        yield from [
            {
                "col1": i,
                "col2": str(i),
                "col3": str(i),
                "col4": str(i),
            }
            for i in range(10)
        ]

    hints = bigquery_adapter(
        no_hints._clone(new_name="hints"), cluster=["col1", "col2", "col3", "col4"]
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        no_hints_cluster_fields = (
            [] if no_hints_table.clustering_fields is None else no_hints_table.clustering_fields
        )
        hints_cluster_fields = (
            [] if hints_table.clustering_fields is None else hints_table.clustering_fields
        )

        assert not no_hints_cluster_fields, "`no_hints` table IS clustered some column."
        assert [
            "col1",
            "col2",
            "col3",
            "col4",
        ] == hints_cluster_fields


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_clustering(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints._clone(new_name="hints"), cluster="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        no_hints_cluster_fields = (
            [] if no_hints_table.clustering_fields is None else no_hints_table.clustering_fields
        )
        hints_cluster_fields = (
            [] if hints_table.clustering_fields is None else hints_table.clustering_fields
        )

        assert not no_hints_cluster_fields, "`no_hints` table IS clustered by `col1`."
        assert ["col1"] == hints_cluster_fields, "`hints` table IS NOT clustered by `col1`."


def test_adapter_hints_empty() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    with pytest.raises(
        ValueError,
        match="^AT LEAST one of `partition`, `cluster`, `round_half_away_from_zero`",
    ):
        bigquery_adapter(some_data)


def test_adapter_hints_round_mutual_exclusivity_requirement() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    with pytest.raises(
        ValueError,
        match=(
            "are present in both `round_half_away_from_zero` and `round_half_even` "
            "which is not allowed. They must be mutually exclusive.$"
        ),
    ):
        bigquery_adapter(
            some_data, round_half_away_from_zero="double_col", round_half_even="double_col"
        )


def test_adapter_additional_table_hints_parsing_table_description() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    table_description = "Once upon a time a small table got hinted."
    bigquery_adapter(some_data, table_description=table_description)

    assert some_data._hints["x-bigquery-table-description"] == table_description  # type: ignore


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_additional_table_hints_table_description(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(
        no_hints._clone(new_name="hints"),
        table_description="Once upon a time a small table got hinted.",
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        assert not no_hints_table.description
        assert hints_table.description == "Once upon a time a small table got hinted."


def test_adapter_additional_table_hints_parsing_table_expiration() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, table_expiration_datetime="2030-01-01")

    assert some_data._hints["x-bigquery-table-expiration"] == pendulum.datetime(2030, 1, 1)  # type: ignore


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_additional_table_hints_table_expiration(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(
        no_hints._clone(new_name="hints"), table_expiration_datetime="2030-01-01"
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        assert not no_hints_table.expires
        assert hints_table.expires == pendulum.datetime(2030, 1, 1, 0)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_merge_behaviour(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
            {"name": "col3", "data_type": "double"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i, "col3": float(i)} for i in range(10)]

    bigquery_adapter(hints, table_expiration_datetime="2030-01-01", cluster=["col1"])
    bigquery_adapter(
        hints, table_description="A small table somewhere in the cosmos...", partition="col2"
    )

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(hints)

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        table_fqtn = c.make_qualified_table_name("hints", escape=False)

        table: Table = nc.get_table(table_fqtn)

        table_cluster_fields = [] if table.clustering_fields is None else table.clustering_fields

        # Test merging behaviour.
        assert table.expires == pendulum.datetime(2030, 1, 1, 0)
        assert ["col1"] == table_cluster_fields, "`hints` table IS NOT clustered by `col1`."
        assert table.description == "A small table somewhere in the cosmos..."

        if not table.range_partitioning:
            raise ValueError("`hints` table IS NOT clustered on a column.")
        else:
            assert (
                table.range_partitioning.field == "col2"
            ), "`hints` table IS NOT clustered on column `col2`."
