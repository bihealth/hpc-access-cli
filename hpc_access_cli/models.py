"""Pydantic models for representing records."""

import datetime
import enum
import grp
import os
import pwd
import stat
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

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


class Gecos(BaseModel):
    """GECOS information about a user."""

    #: The full name of the user.
    full_name: Optional[str] = None
    #: The office location of the user.
    office_location: Optional[str] = None
    #: The office phone number of the user.
    office_phone: Optional[str] = None
    #: The home phone number of the user.
    home_phone: Optional[str] = None
    #: The other information about the user.
    other: Optional[str] = None

    def to_string(self):
        """Convert the GECOS information to a GECOS string."""
        return ",".join(
            [
                self.full_name if self.full_name else "",
                self.office_location if self.office_location else "",
                self.office_phone if self.office_phone else "",
                self.home_phone if self.home_phone else "",
                self.other if self.other else "",
            ]
        )

    @staticmethod
    def from_string(gecos: str) -> "Gecos":
        """Create a new instance from a GECOS string."""
        parts = gecos.split(",", 4)
        if len(parts) < 5:
            parts.extend([""] * (5 - len(parts)))
        return Gecos(
            full_name=parts[0] if parts[0] != "None" else None,
            office_location=parts[1] if parts[1] != "None" else None,
            office_phone=parts[2] if parts[2] != "None" else None,
            home_phone=parts[3] if parts[3] != "None" else None,
            other=parts[4] if parts[4] != "None" else None,
        )


class LdapUser(BaseModel):
    """A user form the LDAP directory."""

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
    #: The numeric user ID.
    uid_number: int
    #: The primary group of the user.
    gid_number: Optional[int]
    #: The home directory of the user.
    home_directory: str
    #: The login shell of the user.
    login_shell: str
    #: The GECOS information of the user.
    gecos: Optional[Gecos]
    #: Public SSH keys.
    ssh_public_key: List[str]


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


class ResourceData(BaseModel):
    """A resource request/usage for a user."""

    #: Storage on tier 1 in TB (work).
    tier1_work: float = 0.0
    #: Storage on tier 1 in TB (scratch).
    tier1_scratch: float = 0.0
    #: Storage on tier 2 (mirrored) in TB.
    tier2_mirrored: float = 0.0
    #: Storage on tier 2 (unmirrored) in TB.
    tier2_unmirrored: float = 0.0


class ResourceDataUser(BaseModel):
    """A resource request/usage for a user."""

    #: Storage on tier 1 in GB (home).
    tier1_home: float = 0.0


class GroupFolders(BaseModel):
    """Folders for a group or project."""

    #: The work directory.
    tier1_work: str
    #: The scratch directory.
    tier1_scratch: str
    #: The mirrored directory.
    tier2_mirrored: str
    #: The unmirrored directory.
    tier2_unmirrored: str


@enum.unique
class Status(enum.Enum):
    """Status of a hpc user, group, or project."""

    INITIAL = "INITIAL"
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    EXPIRED = "EXPIRED"


class HpcUser(BaseModel):
    """A user as read from the hpc-access API."""

    #: The UUID of the record.
    uuid: UUID
    #: The UUID of the primary ``HpcGroup``.
    primary_group: Optional[UUID]
    #: Description of the record.
    description: Optional[str]
    #: The user's email address.
    email: Optional[str]
    #: The full name of the user.
    full_name: str
    #: The first name fo the user.
    first_name: Optional[str]
    #: The last name of the user.
    last_name: Optional[str]
    #: The office phone number of the user.
    phone_number: Optional[str]
    #: The requested resources.
    resources_requested: Optional[ResourceDataUser]
    #: The used resources.
    resources_used: Optional[ResourceDataUser]
    #: The status of the record.
    status: Status
    #: The POSIX UID of the user.
    uid: int
    #: The username of the record.
    username: str
    #: Point in time of user expiration.
    expiration: datetime.datetime
    #: The home directory.
    home_directory: str
    #: The login shell
    login_shell: str
    #: The version of the user record.
    current_version: int


class HpcGroup(BaseModel):
    """A group as read from the hpc-access API."""

    #: The UUID of the record.
    uuid: UUID
    #: The owning ``HpcUser``.
    owner: UUID
    #: Description of the record.
    description: Optional[str]
    #: The delegate.
    delegate: Optional[UUID]
    #: The requested resources.
    resources_requested: Optional[ResourceData]
    #: The used resources.
    resources_used: Optional[ResourceData]
    #: The status of the record.
    status: Status
    #: The POSIX GID of the corresponding Unix group.
    gid: Optional[int]
    #: The name of the record.
    name: str
    #: The folders of the group.
    folders: GroupFolders
    #: Point in time of group expiration.
    expiration: datetime.datetime
    #: The version of the group record.
    current_version: int


class HpcProject(BaseModel):
    """A project as read from the hpc-access API."""

    #: The UUID of the record.
    uuid: UUID
    #: The owning ``HpcGroup``, owner of group is owner of project.
    group: Optional[UUID]
    #: Description of the record.
    description: Optional[str]
    #: The delegate for the project.
    delegate: Optional[UUID]
    #: The requested resources.
    resources_requested: Optional[ResourceData]
    #: The used resources.
    resources_used: Optional[ResourceData]
    #: The status of the record.
    status: Status
    #: The POSIX GID of the corresponding Unix group.
    gid: Optional[int]
    #: The name of the record.
    name: str
    #: The folders of the group.
    folders: GroupFolders
    #: Point in time of group expiration.
    expiration: datetime.datetime
    #: The version of the project record.
    current_version: int
    #: The project's member user UUIDs.
    members: List[UUID]


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
