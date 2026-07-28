"""Local models for LDAP, filesystem, and state operations."""

from __future__ import annotations

import enum
import grp
import os
import pwd
import stat
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from hpc_access_cli.api_models import HpcGroup, HpcProject, HpcUser

#: Login shell to use for disabled users.
LOGIN_SHELL_DISABLED = "/usr/sbin/nologin"


class FsDirectory(BaseModel):
    """Information about a file system directory.

    This consists of the classic POSIX file system attributes and
    additional Ceph extended attributes.
    """

    #: Absolute path to the directory.
    path: str
    #: The username of the owner of the directory.
    owner_name: str
    #: The user UID of the owner of the directory.
    owner_uid: int
    #: The group of the directory.
    group_name: str
    #: The group GID of the directory.
    group_gid: int
    #: The directory permissions.
    perms: str

    #: The size of the directory in bytes.
    rbytes: Optional[int]
    #: The number of files in the directory.
    rfiles: Optional[int]
    #: The bytes quota.
    quota_bytes: Optional[int]
    #: The files quota.
    quota_files: Optional[int]

    @staticmethod
    def from_path(path: str) -> "FsDirectory":
        """Create a new instance from a path."""
        from hpc_access_cli.fs import get_extended_attribute

        # Get owner user name, owner uid, group name, group gid
        uid = os.stat(path).st_uid
        gid = os.stat(path).st_gid
        try:
            owner_name = pwd.getpwuid(uid).pw_name
        except KeyError:
            if os.environ.get("DEBUG", "0") == "1":
                owner_name = "unknown"
            else:
                raise
        try:
            group_name = grp.getgrgid(gid).gr_name
        except KeyError:
            if os.environ.get("DEBUG", "0") == "1":
                group_name = "unknown"
            else:
                raise
        # Get permissions mask
        mode = os.stat(path).st_mode
        permissions = stat.filemode(mode)
        # Get Ceph extended attributes.
        rbytes = int(get_extended_attribute(path, "ceph.dir.rbytes"))
        rfiles = int(get_extended_attribute(path, "ceph.dir.rfiles"))
        try:
            quota_bytes = int(get_extended_attribute(path, "ceph.quota.max_bytes"))
        except ValueError:
            # attribute missing => no quota set
            quota_bytes = None
        try:
            quota_files = int(get_extended_attribute(path, "ceph.quota.max_files"))
        except ValueError:
            # attribute missing => no quota set
            quota_files = None

        return FsDirectory(
            path=path,
            owner_name=owner_name,
            owner_uid=uid,
            group_name=group_name,
            group_gid=gid,
            perms=permissions,
            rbytes=rbytes,
            rfiles=rfiles,
            quota_bytes=quota_bytes,
            quota_files=quota_files,
        )


class LdapUser(BaseModel):
    """A user from the LDAP directory."""

    #: The common name of the user.
    cn: str
    #: The distinguished name of the user.
    dn: str
    #: The username.
    uid: str
    #: The email address of the user.
    mail: Optional[str]
    #: The user's surname.
    sn: Optional[str]
    #: The user's given name.
    given_name: Optional[str]
    #: The user's display name.
    display_name: Optional[str]
    #: The numeric user ID.
    uid_number: int
    #: The primary group of the user.
    gid_number: Optional[int]
    #: The home directory of the user.
    home_directory: str
    #: The login shell of the user.
    login_shell: str
    #: Telephone number.
    telephone_number: Optional[str]


class LdapGroup(BaseModel):
    """A group from the LDAP directory.

    Note that we use this both for work groups and for projects.  Work groups
    will have ``member_uids==[]`` as the members are added via their primary
    numeric group uid.
    """

    #: The common name of the group.
    cn: str
    #: The distinguished name of the group.
    dn: str
    #: The GID number.
    gid_number: int
    #: Description of the group.
    description: Optional[str]
    #: The distinguished name of the group's owner.
    owner_dn: Optional[str]
    #: The distinguished name of the group's delegates.
    delegate_dns: List[str]
    #: The member uids (== user names) of the group.
    member_uids: List[str]


class SystemState(BaseModel):
    """System state retrieved from LDAP and file system."""

    #: Mapping from LDAP username to ``LdapUser``.
    ldap_users: Dict[str, LdapUser]
    #: Mapping from LDAP groupname to ``LdapGroup``.
    ldap_groups: Dict[str, LdapGroup]
    #: Mapping from file system path to ``FsDirectory``.
    fs_directories: Dict[str, FsDirectory]


class HpcaccessState(BaseModel):
    """State as loaded from hpc-access."""

    hpc_users: Dict[UUID, HpcUser]
    hpc_groups: Dict[UUID, HpcGroup]
    hpc_projects: Dict[UUID, HpcProject]


@enum.unique
class StateOperation(enum.Enum):
    """Operation to perform on the state."""

    #: Create a new object.
    CREATE = "CREATE"
    #: Update an object's attributes.
    UPDATE = "UPDATE"
    #: Disable access to an update; note that we will never delete
    #: in scripts by design.
    DISABLE = "DISABLE"


class FsDirectoryOp(BaseModel):
    """Operation to perform on a file system directory."""

    #: The operation to perform.
    operation: StateOperation
    #: The directory to operate on.
    directory: FsDirectory
    #: The diff to update.
    diff: Dict[str, None | int | str]


class LdapUserOp(BaseModel):
    """Operation to perform on a LDAP user."""

    #: The operation to perform.
    operation: StateOperation
    #: The user to operate on.
    user: LdapUser
    #: The diff to update (``None`` => clear).
    diff: Dict[str, None | int | str | List[str] | Dict[str, Any]]


class LdapGroupOp(BaseModel):
    """Operation to perform on a LDAP group."""

    #: The operation to perform.
    operation: StateOperation
    #: The group to operate on.
    group: LdapGroup
    #: The diff to update (``None`` => clear).
    diff: Dict[str, None | int | str | List[str] | Dict[str, Any]]


class OperationsContainer(BaseModel):
    """Container for all operations to perform."""

    #: Operations to perform on LDAP users.
    ldap_user_ops: List[LdapUserOp]
    #: Operations to perform on LDAP groups.
    ldap_group_ops: List[LdapGroupOp]
    #: Operations to perform on file system directories.
    fs_ops: List[FsDirectoryOp]
