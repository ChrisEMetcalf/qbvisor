from unittest.mock import Mock

from qbvisor._resources.base import BaseResource
from qbvisor.metadata import QuickBaseMetaCache
from qbvisor.transport import QuickBaseTransport, RetryPolicy


class ProbeResource(BaseResource):
    def resolve_table(self) -> tuple[str, str]:
        return self._ids("Operations", "Projects")

    def fetch_collection(self) -> list[dict[str, object]]:
        return self._request_array(
            method="GET",
            path="resources",
            params={"scope": "test"},
            retry_policy=RetryPolicy.SAFE,
        )


def test_resource_contract_delegates_shared_client_dependencies():
    context = Mock()
    context.meta = Mock(spec=QuickBaseMetaCache)
    context.transport = Mock(spec=QuickBaseTransport)
    context._ids.return_value = ("app_operations", "tbl_projects")
    context._request.return_value = [{"id": 1}]
    resource = ProbeResource(context)

    assert resource.meta is context.meta
    assert resource.transport is context.transport
    assert resource.resolve_table() == ("app_operations", "tbl_projects")
    assert resource.fetch_collection() == [{"id": 1}]
    context._ids.assert_called_once_with("Operations", "Projects")
    context._request.assert_called_once_with(
        method="GET",
        path="resources",
        params={"scope": "test"},
        retry_policy=RetryPolicy.SAFE,
        response_type=list,
    )
