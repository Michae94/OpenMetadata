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
"""Unit tests for Athena connection handling."""

import socket
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from metadata.core.connections.test_connection.check import collect_checks
from metadata.core.connections.test_connection.checks.database import DatabaseStep
from metadata.generated.schema.entity.services.connections.database.athenaConnection import (
    AthenaConnection as AthenaConnectionConfig,
)
from metadata.generated.schema.entity.services.connections.database.athenaConnection import (
    AthenaScheme,
)
from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials
from metadata.ingestion.connections.connection import BaseConnection
from metadata.ingestion.source.database.athena.connection import (
    ATHENA_ERRORS,
    AthenaChecks,
    AthenaConnection,
)

CONNECTION_MODULE = "metadata.ingestion.source.database.athena.connection"


def _config(**kwargs) -> AthenaConnectionConfig:
    base = {
        "awsConfig": AWSCredentials(awsAccessKeyId="key", awsRegion="us-east-2", awsSecretAccessKey="secret_key"),
        "s3StagingDir": "s3://postgres/input/",
        "workgroup": "primary",
        "scheme": AthenaScheme.awsathena_rest,
    }
    base.update(kwargs)
    return AthenaConnectionConfig(**base)


def _client_error(code: str, message: str = "denied") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "StartQueryExecution")


def test_athena_connection_is_base_connection():
    assert issubclass(AthenaConnection, BaseConnection)


def test_get_client_uses_the_class_url_builder():
    with patch(f"{CONNECTION_MODULE}.create_generic_db_connection") as mock_connection:
        _ = AthenaConnection(_config()).client
    assert mock_connection.call_args.kwargs["get_connection_url_fn"].__name__ == "get_connection_url"


def test_get_client_registers_engine_disposal():
    conn = AthenaConnection(_config())
    with patch(f"{CONNECTION_MODULE}.create_generic_db_connection") as mock_connection:
        engine = mock_connection.return_value
        _ = conn.client
        conn.close()
    engine.dispose.assert_called_once_with()


def test_checks_returns_provider_over_the_client():
    conn = AthenaConnection(_config())
    conn._client = MagicMock()
    provider = conn.checks()
    assert isinstance(provider, AthenaChecks)
    assert provider.client is conn._client


def test_every_athena_step_resolves_to_a_check():
    provider = AthenaChecks(client=MagicMock())
    resolved = collect_checks(provider)
    assert set(resolved) == {
        DatabaseStep.CheckAccess,
        DatabaseStep.GetSchemas,
        DatabaseStep.GetTables,
        DatabaseStep.GetViews,
    }


def test_error_pack_classifies_access_denied_as_auth_failure():
    diagnosis = ATHENA_ERRORS.classify(_client_error("AccessDeniedException"))
    assert diagnosis is not None
    assert diagnosis.title == "Authentication failed"


def test_error_pack_classifies_unrecognized_client_as_auth_failure():
    diagnosis = ATHENA_ERRORS.classify(_client_error("UnrecognizedClientException"))
    assert diagnosis is not None
    assert diagnosis.title == "Authentication failed"


def test_error_pack_classifies_auth_failure_through_a_wrapping_cause():
    wrapped = RuntimeError("query failed")
    wrapped.__cause__ = _client_error("InvalidSignatureException")
    diagnosis = ATHENA_ERRORS.classify(wrapped)
    assert diagnosis is not None
    assert diagnosis.title == "Authentication failed"


def test_error_pack_classifies_missing_workgroup():
    error = RuntimeError("WorkGroup primary is not found")
    diagnosis = ATHENA_ERRORS.classify(error)
    assert diagnosis is not None
    assert diagnosis.title == "Workgroup not found"


def test_error_pack_classifies_missing_result_location():
    error = RuntimeError("No output location provided for query")
    diagnosis = ATHENA_ERRORS.classify(error)
    assert diagnosis is not None
    assert diagnosis.title == "Query result location not configured"


def test_error_pack_classifies_unreachable_endpoint():
    error = RuntimeError('Could not connect to the endpoint URL: "https://athena.bad.amazonaws.com/"')
    diagnosis = ATHENA_ERRORS.classify(error)
    assert diagnosis is not None
    assert diagnosis.title == "Cannot reach the AWS Athena endpoint"


def test_error_pack_classifies_not_authorized():
    diagnosis = ATHENA_ERRORS.classify(
        _client_error("SomeOtherException", "User is not authorized to perform: glue:GetTables")
    )
    assert diagnosis is not None
    assert diagnosis.title == "Not authorized"


def test_error_pack_folds_in_the_network_pack():
    diagnosis = ATHENA_ERRORS.classify(socket.gaierror("name resolution failed"))
    assert diagnosis is not None
    assert diagnosis.title == "Host could not be resolved"


def test_error_pack_returns_none_for_unknown_error():
    assert ATHENA_ERRORS.classify(RuntimeError("something unrelated")) is None
