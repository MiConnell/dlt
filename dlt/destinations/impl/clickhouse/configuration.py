import logging
from typing import ClassVar, List, Any, Final, TYPE_CHECKING, Literal, cast

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import (
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.libs.sql_alchemy import URL
from dlt.common.utils import digest128


TSecureConnection = Literal[0, 1]


@configspec
class ClickhouseCredentials(ConnectionStringCredentials):
    drivername: str = "clickhouse"
    host: str
    """Host with running ClickHouse server."""
    port: int = 9000
    """Port ClickHouse server is bound to. Defaults to 9000."""
    username: str = "default"
    """Database user. Defaults to 'default'."""
    database: str = "default"
    """database connect to. Defaults to 'default'."""
    secure: TSecureConnection = 1
    """Enables TLS encryption when connecting to ClickHouse Server. 0 means no encryption, 1 means encrypted."""
    connect_timeout: int = 10
    """Timeout for establishing connection. Defaults to 10 seconds."""
    send_receive_timeout: int = 300
    """Timeout for sending and receiving data. Defaults to 300 seconds."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "host",
        "port",
        "username",
        "database",
        "secure",
        "connect_timeout",
        "send_receive_timeout",
    ]


    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))
        self.send_receive_timeout = int(
            self.query.get("send_receive_timeout", self.send_receive_timeout)
        )
        self.secure = cast(TSecureConnection, int(self.query.get("secure", self.secure)))
        if not self.is_partial():
            self.resolve()

    def to_url(self) -> URL:
        url = super().to_url()
        url.update_query_pairs(
            [
                ("connect_timeout", str(self.connect_timeout)),
                ("send_receive_timeout", str(self.send_receive_timeout)),
                ("secure", str(1) if self.secure else str(0)),
            ]
        )
        logging.info(url)
        return url


@configspec
class ClickhouseClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "clickhouse"  # type: ignore[misc]
    credentials: ClickhouseCredentials

    # Primary key columns are used to build a sparse primary index which allows for efficient data retrieval,
    # but they do not enforce uniqueness constraints. It permits duplicate values even for the primary key
    # columns within the same granule.
    # See: https://clickhouse.com/docs/en/optimize/sparse-primary-indexes

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: ClickhouseCredentials = None,
            dataset_name: str = None,
            destination_name: str = None,
            environment: str = None
        ) -> None:
            super().__init__(
                credentials=credentials,
                dataset_name=dataset_name,
                destination_name=destination_name,
                environment=environment,
            )
            ...
