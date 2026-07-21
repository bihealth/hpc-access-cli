"""Pydantic models for representing records.

Re-exports from :mod:`api_models` and :mod:`local_models` for backward
compatibility.  New code should import from the specific module directly.
"""

from hpc_access_cli.api_models import (
    GroupFolders,
    HpcGroup,
    HpcProject,
    HpcUser,
    ResourceData,
    ResourceDataUser,
    Status,
)
from hpc_access_cli.local_models import (
    LOGIN_SHELL_DISABLED,
    FsDirectory,
    FsDirectoryOp,
    HpcaccessState,
    LdapGroup,
    LdapGroupOp,
    LdapUser,
    LdapUserOp,
    OperationsContainer,
    StateOperation,
    SystemState,
)

__all__ = [
    "LOGIN_SHELL_DISABLED",
    "FsDirectory",
    "FsDirectoryOp",
    "GroupFolders",
    "HpcaccessState",
    "HpcGroup",
    "HpcProject",
    "HpcUser",
    "LdapGroup",
    "LdapGroupOp",
    "LdapUser",
    "LdapUserOp",
    "OperationsContainer",
    "ResourceData",
    "ResourceDataUser",
    "StateOperation",
    "Status",
    "SystemState",
]
