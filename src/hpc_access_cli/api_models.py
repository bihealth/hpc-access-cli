"""Auto-generated Pydantic models from the hpc-access OpenAPI schema.

Regenerate with:  python scripts/generate_models.py
"""

from __future__ import annotations

import enum
from typing import Any
from uuid import UUID

from pydantic import AnyUrl, AwareDatetime, BaseModel, Field


@enum.unique
class Status(enum.Enum):
    """Status of a hpc user, group, or project."""

    INITIAL = "INITIAL"
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    EXPIRED = "EXPIRED"


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


class HpcGroup(BaseModel):
    uuid: str
    date_created: AwareDatetime
    owner: UUID = Field(..., description="Record UUID")
    delegate: UUID = Field(..., description="Record UUID")
    resources_requested: Any
    resources_used: Any
    status: str
    description: str
    gid: int
    name: str
    folders: Any
    expiration: AwareDatetime
    current_version: int


class HpcGroupCreateRequest(BaseModel):
    uuid: str
    date_created: AwareDatetime
    resources_requested: Any
    description: str
    expiration: AwareDatetime
    name: str
    folders: Any
    current_version: int


class HpcProject(BaseModel):
    uuid: str
    date_created: AwareDatetime
    group: UUID = Field(..., description="Record UUID")
    delegate: UUID = Field(..., description="Record UUID")
    resources_requested: Any
    resources_used: Any
    status: str
    description: str
    gid: int
    name: str
    folders: Any
    expiration: AwareDatetime
    members: list[UUID]
    current_version: int


class HpcProjectCreateRequest(BaseModel):
    uuid: str
    date_created: AwareDatetime
    resources_requested: Any
    description: str
    expiration: AwareDatetime
    group: UUID = Field(..., description="Record UUID")
    members: list[UUID]
    name: str
    name_requested: str
    folders: Any
    current_version: int


class HpcUser(BaseModel):
    uuid: str
    date_created: AwareDatetime
    email: str
    full_name: str
    first_name: str
    last_name: str
    display_name: str
    phone_number: str
    primary_group: UUID = Field(..., description="Record UUID")
    resources_requested: Any
    resources_used: Any
    status: str
    description: str
    uid: int
    username: str
    expiration: AwareDatetime
    home_directory: str
    login_shell: str
    removed: bool
    current_version: int


class HpcUserLookup(BaseModel):
    id: int
    username: str
    primary_group: str = Field(..., description="Name of the group on the cluster")
    full_name: str


class PaginatedHpcGroupList(BaseModel):
    next: AnyUrl | None = Field(
        None, examples=['http://api.example.org/accounts/?cursor=cD00ODY%3D"']
    )
    previous: AnyUrl | None = Field(
        None, examples=["http://api.example.org/accounts/?cursor=cj0xJnA9NDg3"]
    )
    results: list[HpcGroup]


class PaginatedHpcProjectList(BaseModel):
    next: AnyUrl | None = Field(
        None, examples=['http://api.example.org/accounts/?cursor=cD00ODY%3D"']
    )
    previous: AnyUrl | None = Field(
        None, examples=["http://api.example.org/accounts/?cursor=cj0xJnA9NDg3"]
    )
    results: list[HpcProject]


class PaginatedHpcUserList(BaseModel):
    next: AnyUrl | None = Field(
        None, examples=['http://api.example.org/accounts/?cursor=cD00ODY%3D"']
    )
    previous: AnyUrl | None = Field(
        None, examples=["http://api.example.org/accounts/?cursor=cj0xJnA9NDg3"]
    )
    results: list[HpcUser]


class PaginatedHpcUserLookupList(BaseModel):
    next: AnyUrl | None = Field(
        None, examples=['http://api.example.org/accounts/?cursor=cD00ODY%3D"']
    )
    previous: AnyUrl | None = Field(
        None, examples=["http://api.example.org/accounts/?cursor=cj0xJnA9NDg3"]
    )
    results: list[HpcUserLookup]


class PatchedHpcGroup(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    owner: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: Any | None = None
    resources_used: Any | None = None
    status: str | None = None
    description: str | None = None
    gid: int | None = None
    name: str | None = None
    folders: Any | None = None
    expiration: AwareDatetime | None = None
    current_version: int | None = None


class PatchedHpcGroupCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: Any | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    name: str | None = None
    folders: Any | None = None
    current_version: int | None = None


class PatchedHpcProject(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: Any | None = None
    resources_used: Any | None = None
    status: str | None = None
    description: str | None = None
    gid: int | None = None
    name: str | None = None
    folders: Any | None = None
    expiration: AwareDatetime | None = None
    members: list[UUID] | None = None
    current_version: int | None = None


class PatchedHpcProjectCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: Any | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    members: list[UUID] | None = None
    name: str | None = None
    name_requested: str | None = None
    folders: Any | None = None
    current_version: int | None = None


class PatchedHpcUser(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    email: str | None = None
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    display_name: str | None = None
    phone_number: str | None = None
    primary_group: UUID | None = Field(None, description="Record UUID")
    resources_requested: Any | None = None
    resources_used: Any | None = None
    status: str | None = None
    description: str | None = None
    uid: int | None = None
    username: str | None = None
    expiration: AwareDatetime | None = None
    home_directory: str | None = None
    login_shell: str | None = None
    removed: bool | None = None
    current_version: int | None = None
