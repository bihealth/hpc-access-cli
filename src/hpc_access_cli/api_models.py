"""Auto-generated Pydantic models from the hpc-access OpenAPI schema.

Regenerate with:  python scripts/generate_models.py
"""

from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from pydantic import AnyUrl, AwareDatetime, BaseModel, Field


class GroupFolders(BaseModel):
    tier1_work: str
    tier1_scratch: str
    tier2_mirrored: str
    tier2_unmirrored: str


class HpcUserLookup(BaseModel):
    id: int | None = None
    username: str | None = None
    primary_group: str | None = Field(None, description="Name of the group on the cluster")
    full_name: str | None = None


class PaginatedHpcUserLookupList(BaseModel):
    next: AnyUrl | None = Field(
        None, examples=['http://api.example.org/accounts/?cursor=cD00ODY%3D"']
    )
    previous: AnyUrl | None = Field(
        None, examples=["http://api.example.org/accounts/?cursor=cj0xJnA9NDg3"]
    )
    results: list[HpcUserLookup]


class ResourceData(BaseModel):
    tier1_work: float | None = 0.0
    tier1_scratch: float | None = 0.0
    tier2_mirrored: float | None = 0.0
    tier2_unmirrored: float | None = 0.0


class ResourceDataUser(BaseModel):
    tier1_home: float | None = 0.0


class Status(StrEnum):
    initial = "INITIAL"
    active = "ACTIVE"
    deleted = "DELETED"
    expired = "EXPIRED"


class HpcGroup(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    owner: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: ResourceData | None = None
    resources_used: ResourceData
    status: Status | None = None
    description: str | None = None
    gid: int
    name: str | None = None
    folders: GroupFolders
    expiration: AwareDatetime | None = None
    current_version: int | None = None


class HpcGroupCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: ResourceData | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    name: str
    folders: GroupFolders
    current_version: int | None = None


class HpcProject(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: ResourceData | None = None
    resources_used: ResourceData
    status: Status | None = None
    description: str | None = None
    gid: int
    name: str | None = None
    folders: GroupFolders
    expiration: AwareDatetime | None = None
    members: list[UUID] | None = None
    current_version: int | None = None


class HpcProjectCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: ResourceData | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    members: list[UUID] | None = None
    name: str
    name_requested: str | None = None
    folders: GroupFolders
    current_version: int | None = None


class HpcUser(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    email: str | None = None
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    display_name: str | None = None
    phone_number: str | None = None
    primary_group: UUID | None = Field(None, description="Record UUID")
    resources_requested: ResourceDataUser | None = None
    resources_used: ResourceDataUser
    status: Status | None = None
    description: str | None = None
    uid: int | None = None
    username: str | None = None
    expiration: AwareDatetime | None = None
    home_directory: str
    login_shell: str
    removed: bool | None = None
    current_version: int | None = None


class HpcaccessState(BaseModel):
    hpc_users: dict[str, HpcUser]
    hpc_groups: dict[str, HpcGroup]
    hpc_projects: dict[str, HpcProject]


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


class PatchedHpcGroup(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    owner: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: ResourceData | None = None
    resources_used: ResourceData | None = None
    status: Status | None = None
    description: str | None = None
    gid: int | None = None
    name: str | None = None
    folders: GroupFolders | None = None
    expiration: AwareDatetime | None = None
    current_version: int | None = None


class PatchedHpcGroupCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: ResourceData | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    name: str | None = None
    folders: GroupFolders | None = None
    current_version: int | None = None


class PatchedHpcProject(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    delegate: UUID | None = Field(None, description="Record UUID")
    resources_requested: ResourceData | None = None
    resources_used: ResourceData | None = None
    status: Status | None = None
    description: str | None = None
    gid: int | None = None
    name: str | None = None
    folders: GroupFolders | None = None
    expiration: AwareDatetime | None = None
    members: list[UUID] | None = None
    current_version: int | None = None


class PatchedHpcProjectCreateRequest(BaseModel):
    uuid: str | None = None
    date_created: AwareDatetime | None = None
    resources_requested: ResourceData | None = None
    description: str | None = None
    expiration: AwareDatetime | None = None
    group: UUID | None = Field(None, description="Record UUID")
    members: list[UUID] | None = None
    name: str | None = None
    name_requested: str | None = None
    folders: GroupFolders | None = None
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
    resources_requested: ResourceDataUser | None = None
    resources_used: ResourceDataUser | None = None
    status: Status | None = None
    description: str | None = None
    uid: int | None = None
    username: str | None = None
    expiration: AwareDatetime | None = None
    home_directory: str | None = None
    login_shell: str | None = None
    removed: bool | None = None
    current_version: int | None = None
