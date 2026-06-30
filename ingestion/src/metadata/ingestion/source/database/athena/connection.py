#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Source connection handler
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote_plus

from botocore.exceptions import ClientError
from sqlalchemy.engine import Engine

from metadata.clients.aws_client import AWSClient
from metadata.core.connections.test_connection import (
    ErrorPack,
    Matchers,
    check,
    when,
)
from metadata.core.connections.test_connection.checks.database import (
    DatabaseStep,
    list_schemas,
    list_tables,
    list_views,
    run_sql,
)
from metadata.core.connections.test_connection.classifier import exception_chain
from metadata.core.connections.test_connection.network import NETWORK_ERRORS
from metadata.generated.schema.entity.services.connections.database.athenaConnection import (
    AthenaConnection as AthenaConnectionConfig,
)
from metadata.ingestion.connections.builders import (
    create_generic_db_connection,
    get_connection_args_common,
)
from metadata.ingestion.connections.connection import BaseConnection

if TYPE_CHECKING:
    from metadata.core.connections.test_connection import ChecksProvider
    from metadata.core.connections.test_connection.classifier import Matcher
    from metadata.core.connections.test_connection.records import Evidence


def _message(error: BaseException) -> str:
    """The lower-cased text of the error and its cause chain."""
    return " ".join(str(current) for current in exception_chain(error)).lower()


def _aws_error_code(error: BaseException) -> str | None:
    """The botocore ``ClientError`` code anywhere in the cause chain.

    pyathena wraps a botocore ``ClientError`` raised by the underlying AWS call;
    SQLAlchemy then wraps that, so the actionable code (``AccessDeniedException``
    and friends) only survives by walking the chain."""
    code = None
    for current in exception_chain(error):
        if isinstance(current, ClientError):
            code = current.response.get("Error", {}).get("Code")
            break
    return code


def _aws_code(*codes: str) -> Matcher:
    """Match a botocore ``ClientError`` code - the stable signal for an AWS-side
    rejection, where the rendered message text varies."""
    wanted = frozenset(codes)
    return lambda error: _aws_error_code(error) in wanted


def _all_of(*tokens: str) -> Matcher:
    """Match when every token is present in the error's cause-chain text."""
    return lambda error: all(token in _message(error) for token in tokens)


# Athena's transport is HTTPS to the regional AWS endpoint over botocore, so auth
# and permission failures surface as botocore ``ClientError``s matched by
# code/message, not driver errnos. NETWORK_ERRORS is still folded in so a genuine
# DNS/socket failure to the endpoint is typed rather than left raw.
ATHENA_ERRORS = ErrorPack(
    when(
        _aws_code(
            "AccessDeniedException",
            "UnrecognizedClientException",
            "InvalidSignatureException",
            "AuthFailure",
        )
    ).diagnose(
        "Authentication failed",
        fix="Check the AWS credentials (access key, secret, session token, or assume-role ARN) "
        "and that the IAM principal is allowed to call Athena.",
    ),
    when(_all_of("workgroup", "is not found")).diagnose(
        "Workgroup not found",
        fix="Verify the configured workgroup exists in this account and region.",
    ),
    when(Matchers.contains("output location")).diagnose(
        "Query result location not configured",
        fix="Set s3StagingDir to an S3 path the principal can write to, or configure a query "
        "result location on the workgroup.",
    ),
    when(Matchers.contains("could not connect to the endpoint")).diagnose(
        "Cannot reach the AWS Athena endpoint",
        fix="Check that awsRegion is correct and that the Athena endpoint is reachable from where ingestion runs.",
    ),
    when(Matchers.contains("not authorized")).diagnose(
        "Not authorized",
        fix="Grant the IAM principal the required Athena and Glue permissions "
        "(e.g. athena:StartQueryExecution, glue:GetDatabases, glue:GetTables).",
    ),
).including(NETWORK_ERRORS)


class AthenaChecks:
    """Test-connection checks for Athena."""

    errors = ATHENA_ERRORS

    def __init__(self, client: Engine) -> None:
        self.client = client

    @check(DatabaseStep.CheckAccess)
    def check_access(self) -> Evidence:
        # run_sql, not ping: the URL carries the AWS endpoint host:port but the
        # transport is HTTPS over botocore, so a raw TCP preflight to it would be
        # meaningless. A real reachability failure still surfaces via NETWORK_ERRORS.
        return run_sql(self.client, "SELECT 1", lambda _: "connection established")

    @check(DatabaseStep.GetSchemas)
    def get_schemas(self) -> Evidence:
        return list_schemas(self.client)

    @check(DatabaseStep.GetTables)
    def get_tables(self) -> Evidence:
        return list_tables(self.client, None)

    @check(DatabaseStep.GetViews)
    def get_views(self) -> Evidence:
        return list_views(self.client, None)


class AthenaConnection(BaseConnection[AthenaConnectionConfig, Engine]):
    @staticmethod
    def get_connection_url(connection: AthenaConnectionConfig) -> str:
        """
        Method to get connection url
        """
        aws_access_key_id = connection.awsConfig.awsAccessKeyId
        aws_secret_access_key = connection.awsConfig.awsSecretAccessKey
        aws_session_token = connection.awsConfig.awsSessionToken
        if connection.awsConfig.assumeRoleArn:
            assume_configs = AWSClient.get_assume_role_config(connection.awsConfig)
            if assume_configs:
                aws_access_key_id = assume_configs.accessKeyId
                aws_secret_access_key = assume_configs.secretAccessKey
                aws_session_token = assume_configs.sessionToken

        url = f"{connection.scheme.value}://"  # pyright: ignore[reportOptionalMemberAccess]
        if aws_access_key_id:
            url += aws_access_key_id
            if aws_secret_access_key:
                url += f":{aws_secret_access_key.get_secret_value()}"
        else:
            url += ":"
        url += f"@athena.{connection.awsConfig.awsRegion}.amazonaws.com:443"

        url += f"?s3_staging_dir={quote_plus(str(connection.s3StagingDir))}"
        if connection.workgroup:
            url += f"&work_group={connection.workgroup}"
        if aws_session_token:
            url += f"&aws_session_token={quote_plus(aws_session_token)}"
        if connection.catalogId:
            url += f"&catalog_name={quote_plus(connection.catalogId)}"

        return url

    def _get_client(self) -> Engine:
        engine = create_generic_db_connection(
            connection=self.service_connection,
            get_connection_url_fn=self.get_connection_url,
            get_connection_args_fn=get_connection_args_common,
        )
        self._on_close(engine.dispose)
        return engine

    def checks(self) -> ChecksProvider:
        return AthenaChecks(client=self.client)


def get_lake_formation_client(connection: AthenaConnectionConfig):
    """
    Get the lake formation client
    """
    return AWSClient(connection.awsConfig).get_lake_formation_client()
