"""Pydantic models for hpc-access REST API records.

These correspond to the schemas served by the hpc-access API.
Regenerate from the OpenAPI schema with::

    python scripts/generate_models.py

Currently these are hand-maintained to match the API responses.
"""

from __future__ import annotations

import datetime
import enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ResourceData(BaseModel):
    """A resource request/usage for a group or project."""

    #: Storage on tier 1 in TiB (work).
    tier1_work: float = 0.0
    #: Storage on tier 1 in TiB (scratch).
    tier1_scratch: float = 0.0
    #: Storage on tier 2 (mirrored) in TiB.
    tier2_mirrored: float = 0.0
    #: Storage on tier 2 (unmirrored) in TiB.
    tier2_unmirrored: float = 0.0


class ResourceDataUser(BaseModel):
    """A resource request/usage for a user."""

    #: Storage on tier 1 in GiB (home).
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
    #: The first name of the user.
    first_name: Optional[str]
    #: The last name of the user.
    last_name: Optional[str]
    #: The display name of the user.
    display_name: Optional[str]
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
    members: list[UUID]
