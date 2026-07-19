"""Type-check the installed wheel as a downstream qbvisor consumer."""

from qbvisor import QuickBaseClient, __version__

client_class: type[QuickBaseClient] = QuickBaseClient
package_version: str = __version__
