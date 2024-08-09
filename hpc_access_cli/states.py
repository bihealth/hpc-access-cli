"""State gathering, comparison and update."""

import datetime
import os
import re
import sys
from typing import Dict, List, Optional
from uuid import uuid4

from rich.console import Console

from hpc_access_cli.config import HpcaccessSettings, Settings
from hpc_access_cli.constants import (
    BASE_DN_CHARITE,
    BASE_DN_GROUPS,
    BASE_DN_MDC,
    BASE_DN_PROJECTS,
    BASE_PATH_TIER1,
    BASE_PATH_TIER2,
    CEPHFS_TIER_MAPPING,
    ENTITIES,
    ENTITY_GROUPS,
    ENTITY_PROJECTS,
    ENTITY_USERS,
    HPC_ALUMNIS_GID,
    HPC_ALUMNIS_GROUP,
    HPC_USERS_GID,
    POSIX_AG_PREFIX,
    POSIX_PROJECT_PREFIX,
    PREFIX_MAPPING,
    QUOTA_HOME_BYTES,
    RE_PATH,
)
from hpc_access_cli.fs import FsResourceManager
from hpc_access_cli.ldap import LdapConnection
from hpc_access_cli.models import (
    LOGIN_SHELL_DISABLED,
    FsDirectory,
    FsDirectoryOp,
    Gecos,
    GroupFolders,
    HpcaccessState,
    HpcGroup,
    HpcProject,
    HpcUser,
    LdapGroup,
    LdapGroupOp,
    LdapUser,
    LdapUserOp,
    OperationsContainer,
    ResourceData,
    ResourceDataUser,
    StateOperation,
    Status,
    SystemState,
)
from hpc_access_cli.rest import HpcaccessClient

#: The rich console to use for output.
console_err = Console(file=sys.stderr)


def strip_prefix(name: str, prefix: str | None = None) -> str:
    if prefix:
        if name.startswith(prefix):
            return name[len(prefix) :]
    else:
        if name.startswith(POSIX_AG_PREFIX):
            return name[len(POSIX_AG_PREFIX) :]
        elif name.startswith(POSIX_PROJECT_PREFIX):
            return name[len(POSIX_PROJECT_PREFIX) :]
    return name


def user_dn(user: HpcUser) -> str:
    """Get the DN for the user."""
    if user.username.endswith("_m"):
        return f"cn={user.full_name},{BASE_DN_MDC}"
    else:
        return f"cn={user.full_name},{BASE_DN_CHARITE}"


def gather_hpcaccess_state(settings: HpcaccessSettings) -> HpcaccessState:
    """Gather the state."""
    console_err.log("Loading hpc-access users, groups, and projects...")
    rest_client = HpcaccessClient(settings)
    result = HpcaccessState(
        hpc_users={u.uuid: u for u in rest_client.load_users()},
        hpc_groups={g.uuid: g for g in rest_client.load_groups()},
        hpc_projects={p.uuid: p for p in rest_client.load_projects()},
    )
    console_err.log("  # of users:", len(result.hpc_users))
    console_err.log("  # of groups:", len(result.hpc_groups))
    console_err.log("  # of projects:", len(result.hpc_projects))
    console_err.log("... have hpc-access data now.")
    rest_client.close()
    return result


def deploy_hpcaccess_state(settings: HpcaccessSettings, state: HpcaccessState) -> None:
    """Deploy the state."""
    console_err.log("Deploying hpc-access users, groups, and projects...")
    rest_client = HpcaccessClient(settings)
    for u in state.hpc_users.values():
        rest_client.update_user_resources_used(u)
    for g in state.hpc_groups.values():
        rest_client.update_group_resources_used(g)
    for p in state.hpc_projects.values():
        rest_client.update_project_resources_used(p)
    rest_client.close()
    console_err.log("... deployed hpc-access data now.")


class TargetStateBuilder:
    """ "Helper class that is capable of building the target state giving data
    from hpc-access.
    """

    def __init__(self, settings: HpcaccessSettings, system_state: SystemState):
        #: The settings to use.
        self.settings = settings
        #: The current system state, used for determining next group id.
        self.system_state = system_state
        #: The next gid.
        self.next_gid = self._get_next_gid(system_state)
        console_err.log(f"Next available GID is {self.next_gid}.")

    def _get_next_gid(self, system_state: SystemState) -> int:
        """Get the next available GID."""
        gids = [g.gid_number for g in system_state.ldap_groups.values()]
        gids.extend([u.gid_number for u in system_state.ldap_users.values() if u.gid_number])
        return max(gids) + 1 if gids else 1000

    def run(self) -> SystemState:
        """Run the builder."""
        hpcaccess_state = gather_hpcaccess_state(self.settings)
        return self._build(hpcaccess_state)

    def _build(self, hpcaccess_state: HpcaccessState) -> SystemState:
        """Build the target state."""
        # IMPORANT: Note that order matters here! First, we must create
        # LDAP groups so we have the Unix GIDs when users are considered.
        ldap_groups = self._build_ldap_groups(hpcaccess_state)
        ldap_users = self._build_ldap_users(hpcaccess_state)
        # build hpc-users group
        ldap_groups["hpc-users"] = LdapGroup(
            dn="cn=hpc-users,ou=Groups,dc=hpc,dc=bihealth,dc=org",
            cn="hpc-users",
            gid_number=HPC_USERS_GID,
            description="users allowed to login (active+have group)",
            owner_dn=None,
            delegate_dns=[],
            member_uids=[
                u.uid
                for u in ldap_users.values()
                if u.gid_number != HPC_ALUMNIS_GID and "nologin" not in u.login_shell
            ],
        )
        return SystemState(
            ldap_users=ldap_users,
            ldap_groups=ldap_groups,
            fs_directories=self._build_fs_directories(hpcaccess_state),
        )

    def _build_fs_directories(self, hpcaccess_state: HpcaccessState) -> Dict[str, FsDirectory]:
        """Build the file system directories from the hpc-access state."""
        result = {}
        for user in hpcaccess_state.hpc_users.values():
            if user.primary_group:
                hpc_group = hpcaccess_state.hpc_groups[user.primary_group]
                group_name = hpc_group.name
                group_gid = hpc_group.gid or HPC_ALUMNIS_GID
            else:
                group_name = HPC_ALUMNIS_GROUP
                group_gid = HPC_ALUMNIS_GID
            result[f"{BASE_PATH_TIER1}/home/users/{user.username}"] = FsDirectory(
                path=f"{BASE_PATH_TIER1}/home/users/{user.username}",
                owner_name=user.username,
                owner_uid=user.uid,
                group_name=group_name,
                group_gid=group_gid,
                perms="drwx--S---",
                rbytes=None,
                rfiles=None,
                # Currently, hard-coded user quotas only.
                # Note: maybe remove from HpcUser model!
                quota_bytes=QUOTA_HOME_BYTES,
                quota_files=None,
            )
        for group in hpcaccess_state.hpc_groups.values():
            if not group.gid:
                console_err.log(
                    f"Group {group.name} has no gid, skipping.",
                )
                continue
            owner = hpcaccess_state.hpc_users[group.owner]
            group_name = strip_prefix(group.name, prefix=POSIX_AG_PREFIX)
            # Tier 1
            quota_work = (group.resources_requested or ResourceData).tier1_work
            if not quota_work:
                continue
            quota_scratch = (group.resources_requested or ResourceData).tier1_scratch
            if not quota_scratch:
                continue
            for volume, quota in (
                ("home", QUOTA_HOME_BYTES),
                ("scratch", quota_scratch * 1024 * 1024 * 1024 * 1024),
                ("work", quota_work * 1024 * 1024 * 1024 * 1024),
            ):
                result[f"{BASE_PATH_TIER1}/{volume}/groups/ag-{group_name}"] = FsDirectory(
                    path=f"{BASE_PATH_TIER1}/{volume}/groups/ag-{group_name}",
                    owner_name=owner.username,
                    owner_uid=owner.uid,
                    group_name=group_name,
                    group_gid=group.gid,
                    perms="drwxrwS---",
                    rbytes=None,
                    rfiles=None,
                    quota_bytes=None if quota is None else int(quota),
                    quota_files=None,
                )
            # Tier 2
            for variant in ("unmirrored", "mirrored"):
                if variant == "mirrored":
                    quota = (group.resources_requested or ResourceData).tier2_mirrored
                elif variant == "unmirrored":
                    quota = (group.resources_requested or ResourceData).tier2_unmirrored
                else:
                    raise ValueError("Invalid variant")
                if not quota:
                    continue
                result[f"{BASE_PATH_TIER2}/{variant}/groups/ag-{group_name}"] = FsDirectory(
                    path=f"{BASE_PATH_TIER2}/{variant}/groups/ag-{group_name}",
                    owner_name=owner.username,
                    owner_uid=owner.uid,
                    group_name=group_name,
                    group_gid=group.gid,
                    perms="drwxrwS---",
                    rbytes=None,
                    rfiles=None,
                    quota_bytes=None if quota is None else int(quota),
                    quota_files=None,
                )
        for project in hpcaccess_state.hpc_projects.values():
            if not project.gid:
                console_err.log(
                    f"Project {project.name} has no gid, skipping.",
                )
                continue
            if not project.group:
                console_err.log(
                    f"Project {project.name} has no owning group, skipping.",
                )
                continue
            owning_group = hpcaccess_state.hpc_groups[project.group]
            owner = hpcaccess_state.hpc_users[owning_group.owner]
            project_name = strip_prefix(group.name, prefix=POSIX_PROJECT_PREFIX)
            # Tier 1
            quota_work = (project.resources_requested or ResourceData).tier1_work
            if not quota_work:
                continue
            quota_scratch = (project.resources_requested or ResourceData).tier1_scratch
            if not quota_scratch:
                continue
            for volume, quota in (
                ("home", QUOTA_HOME_BYTES),
                ("scratch", quota_scratch * 1024 * 1024 * 1024 * 1024),
                ("work", quota_work * 1024 * 1024 * 1024 * 1024),
            ):
                result[f"{BASE_PATH_TIER1}/{volume}/projects/{project_name}"] = FsDirectory(
                    path=f"{BASE_PATH_TIER1}/{volume}/projects/{project_name}",
                    owner_name=owner.username,
                    owner_uid=owner.uid,
                    group_name=project_name,
                    group_gid=project.gid,
                    perms="drwxrwS---",
                    rbytes=None,
                    rfiles=None,
                    quota_bytes=None if quota is None else int(quota),
                    quota_files=None,
                )
            # Tier 2
            for variant in ("unmirrored", "mirrored"):
                if variant == "mirrored":
                    quota = (project.resources_requested or ResourceData).tier2_mirrored
                elif variant == "unmirrored":
                    quota = (project.resources_requested or ResourceData).tier2_unmirrored
                else:
                    raise ValueError("Invalid variant")
                if not quota:
                    continue
                result[f"{BASE_PATH_TIER2}/{variant}/projects/{project_name}"] = FsDirectory(
                    path=f"{BASE_PATH_TIER2}/{variant}/projects/{project_name}",
                    owner_name=owner.username,
                    owner_uid=owner.uid,
                    group_name=project_name,
                    group_gid=project.gid,
                    perms="drwxrwS---",
                    rbytes=None,
                    rfiles=None,
                    quota_bytes=None if quota is None else int(quota),
                    quota_files=None,
                )

        return result

    def _build_ldap_users(self, hpcaccess_state: HpcaccessState) -> Dict[str, LdapUser]:
        """Build the LDAP users from the hpc-access state."""
        result = {}
        for user in hpcaccess_state.hpc_users.values():
            gecos = Gecos(
                full_name=user.full_name,
                office_location=None,
                office_phone=user.phone_number,
                other=None,
            )
            if user.primary_group:
                hpc_group = hpcaccess_state.hpc_groups[user.primary_group]
                group_gid = hpc_group.gid or HPC_ALUMNIS_GID
            else:
                group_gid = HPC_ALUMNIS_GID
            result[user.username] = LdapUser(
                dn=user_dn(user),
                cn=user.full_name,
                sn=user.last_name,
                given_name=user.first_name,
                uid=user.username,
                mail=user.email,
                gecos=gecos,
                uid_number=user.uid,
                gid_number=group_gid,
                # user.home_directory
                home_directory=f"{BASE_PATH_TIER1}/home/users/{user.username}",
                # user.login_shell
                login_shell="/usr/bin/bash",
                # SSH keys are managed via upstream LDAP.
                ssh_public_key=[],
            )
        return result

    def _build_ldap_groups(self, state: HpcaccessState) -> Dict[str, LdapGroup]:
        """Build the LDAP groups from the hpc-access state."""
        result = {}
        # build for work groups
        for group in state.hpc_groups.values():
            if not group.gid:
                # assign new group Unix GID if necessary
                group.gid = self.next_gid
                self.next_gid += 1
            group_dn = f"cn={POSIX_AG_PREFIX}{group.name},{BASE_DN_GROUPS}"
            owner = state.hpc_users[group.owner]
            delegate = state.hpc_users[group.delegate] if group.delegate else None
            group_name = f"{POSIX_AG_PREFIX}{group.name}"
            result[group_name] = LdapGroup(
                dn=group_dn,
                cn=group_name,
                gid_number=group.gid,
                description=group.description,
                owner_dn=user_dn(owner),
                delegate_dns=[user_dn(delegate)] if delegate else [],
                member_uids=[],
            )
        # build for projects
        for project in state.hpc_projects.values():
            if not project.gid:
                # assign new project Unix GID if necessary
                project.gid = self.next_gid
                self.next_gid += 1
            group_dn = f"cn={POSIX_PROJECT_PREFIX}{project.name},{BASE_DN_PROJECTS}"
            if project.group:
                owning_group = state.hpc_groups[project.group]
                owner = state.hpc_users[owning_group.owner]
                owner_dn = user_dn(owner)
            else:
                owner_dn = None
            delegate = state.hpc_users[project.delegate] if project.delegate else None
            project_name = f"{POSIX_PROJECT_PREFIX}{project.name}"
            result[project_name] = LdapGroup(
                dn=group_dn,
                cn=project_name,
                gid_number=project.gid,
                description=project.description,
                owner_dn=owner_dn,
                delegate_dns=[user_dn(delegate)] if delegate else [],
                member_uids=[],
            )
        return result


def gather_system_state(settings: Settings) -> SystemState:
    """Gather the system state from LDAP and file system."""
    connection = LdapConnection(settings.ldap_hpc)
    console_err.log("Loading LDAP users and groups...")
    ldap_users = connection.load_users()
    ldap_groups = connection.load_groups()
    console_err.log("Loading file system directories...")
    fs_mgr = FsResourceManager(prefix="/data/sshfs" if os.environ.get("DEBUG", "0") == "1" else "")
    fs_directories = fs_mgr.load_directories()
    result = SystemState(
        ldap_users={u.uid: u for u in ldap_users},
        ldap_groups={g.cn: g for g in ldap_groups},
        fs_directories={d.path: d for d in fs_directories},
    )
    console_err.log("  # of users:", len(result.ldap_users))
    console_err.log("  # of groups:", len(result.ldap_groups))
    console_err.log("  # of directories:", len(result.fs_directories))
    console_err.log("... have system state now")
    return result


def fs_validation(fs: FsDirectory) -> tuple[str, str, str]:
    """Validate the path."""
    matches = re.search(RE_PATH, fs.path)
    if not matches:
        raise ValueError(f"no match for path {fs.path}")

    tier, subdir, entity, folder_name = matches.groups()

    if entity not in ENTITIES:
        raise ValueError(f"entity unknown ({'/'.join(ENTITIES)}): {entity}")

    entity_name = (
        fs.owner_name
        if entity == "users"
        else strip_prefix(fs.group_name, prefix=PREFIX_MAPPING[entity])
    )

    if not entity_name == folder_name:
        raise ValueError(f"name mismatch: {entity_name} {fs.path}")

    resource = CEPHFS_TIER_MAPPING.get((tier, subdir, entity))

    if not resource:
        raise ValueError(
            f"path {fs.path} not in {['/'.join(k) for k in CEPHFS_TIER_MAPPING.keys()]}"
        )

    return entity, folder_name, resource


def convert_to_hpcaccess_state(system_state: SystemState) -> HpcaccessState:
    """Convert hpc-access to system state.

    Note that this will make up the UUIDs.
    """
    # create UUID mapping from user/groupnames
    user_uuids = {u.uid: uuid4() for u in system_state.ldap_users.values()}
    user_by_uid = {u.uid: u for u in system_state.ldap_users.values()}
    user_by_dn = {u.dn: u for u in system_state.ldap_users.values()}
    group_uuids = {
        g.cn: uuid4()
        for g in system_state.ldap_groups.values()
        if g.cn.startswith(POSIX_AG_PREFIX) or g.cn.startswith(POSIX_PROJECT_PREFIX)
    }
    group_by_name = {strip_prefix(g.cn): g for g in system_state.ldap_groups.values()}
    group_by_gid_number = {g.gid_number: g for g in system_state.ldap_groups.values()}
    group_by_owner_dn: Dict[str, LdapGroup] = {}
    for g in system_state.ldap_groups.values():
        if g.owner_dn:
            group_by_owner_dn[user_by_dn[g.owner_dn].dn] = g
    user_quotas: Dict[str, ResourceDataUser] = {}
    group_quotas: Dict[str, ResourceData] = {}
    for fs_data in system_state.fs_directories.values():
        try:
            entity, name, resource = fs_validation(fs_data)
        except ValueError as e:
            console_err.log(f"WARNING: {e}")
            continue

        quota_bytes = fs_data.quota_bytes if fs_data.quota_bytes is not None else 0

        if entity == ENTITY_USERS:
            if name not in user_by_uid:
                console_err.log(f"WARNING: user {name} not found")
                continue
            if name not in user_quotas:
                user_quotas[name] = {}
            user_quotas[name][resource] = quota_bytes / 1024**3
        elif entity in (ENTITY_GROUPS, ENTITY_PROJECTS):
            if name not in group_by_name:
                console_err.log(f"WARNING: group {name} not found")
                continue
            if name not in group_quotas:
                group_quotas[name] = {}
            group_quotas[name][resource] = quota_bytes / 1024**4

    def build_hpcuser(u: LdapUser, quotas: Dict[str, str]) -> HpcUser:
        if u.login_shell != LOGIN_SHELL_DISABLED:
            status = Status.ACTIVE
            expiration = datetime.datetime.now() + datetime.timedelta(days=365)
        else:
            status = Status.EXPIRED
            expiration = datetime.datetime.now()
        if u.gid_number and u.gid_number in group_by_gid_number:
            primary_group = group_uuids.get(group_by_gid_number[u.gid_number].cn)
        else:
            primary_group = None
        return HpcUser(
            uuid=user_uuids[u.uid],
            primary_group=primary_group,
            description=None,
            full_name=u.cn,
            first_name=u.given_name,
            last_name=u.sn,
            email=u.mail,
            phone_number=u.gecos.office_phone if u.gecos else None,
            resources_requested=ResourceDataUser(**quotas),
            resources_used=ResourceDataUser(
                tier1_home=0,
            ),
            status=status,
            uid=u.uid_number,
            username=u.uid,
            expiration=expiration,
            home_directory=u.home_directory,
            login_shell=u.login_shell,
            current_version=1,
        )

    def build_hpcgroup(g: LdapGroup, quotas: Dict[str, str]) -> Optional[HpcGroup]:
        expiration = datetime.datetime.now() + datetime.timedelta(days=365)
        name = strip_prefix(g.cn, POSIX_AG_PREFIX)
        if not g.owner_dn:
            console_err.log(f"no owner DN for {g.cn}, skipping")
            return
        return HpcGroup(
            uuid=group_uuids[g.cn],
            name=name,
            description=g.description,
            owner=user_uuids[user_by_dn[g.owner_dn].uid],
            delegate=user_uuids[user_by_dn[g.delegate_dns[0]].uid] if g.delegate_dns else None,
            resources_requested=ResourceData(**quotas),
            resources_used=ResourceData(
                tier1_work=0,
                tier1_scratch=0,
                tier2_mirrored=0,
                tier2_unmirrored=0,
            ),
            status=Status.ACTIVE,
            gid=g.gid_number,
            folders=GroupFolders(
                tier1_work=f"{BASE_PATH_TIER1}/work/groups/{name}",
                tier1_scratch=f"{BASE_PATH_TIER1}/scratch/groups/{name}",
                tier2_mirrored=f"{BASE_PATH_TIER2}/mirrored/groups/{name}",
                tier2_unmirrored=f"{BASE_PATH_TIER2}/unmirrored/groups/{name}",
            ),
            expiration=expiration,
            current_version=1,
        )

    def build_hpcproject(p: LdapGroup, quotas: Dict[str, str]) -> Optional[HpcProject]:
        expiration = datetime.datetime.now() + datetime.timedelta(days=365)
        name = strip_prefix(p.cn, POSIX_PROJECT_PREFIX)
        if not p.owner_dn:
            console_err.log(f"no owner DN for {p.cn}, skipping")
            return
        members = []
        for uid in p.member_uids:
            uid = uid.strip()
            user = user_by_uid[uid]
            members.append(user_uuids[user.uid])
        gid_number = user_by_dn[p.owner_dn].gid_number
        if not gid_number:
            group = None
        else:
            group = group_uuids[group_by_gid_number[gid_number].cn]
        return HpcProject(
            uuid=group_uuids[p.cn],
            name=name,
            description=g.description,
            group=group,
            delegate=user_uuids[user_by_dn[p.delegate_dns[0]].uid] if p.delegate_dns else None,
            resources_requested=ResourceData(**quotas),
            resources_used=ResourceData(
                tier1_work=0,
                tier1_scratch=0,
                tier2_mirrored=0,
                tier2_unmirrored=0,
            ),
            status=Status.ACTIVE,
            gid=p.gid_number,
            folders=GroupFolders(
                tier1_work=f"{BASE_PATH_TIER1}/work/projects/{name}",
                tier1_scratch=f"{BASE_PATH_TIER1}/scratch/projects/{name}",
                tier2_mirrored=f"{BASE_PATH_TIER2}/mirrored/projects/{name}",
                tier2_unmirrored=f"{BASE_PATH_TIER2}/unmirrored/projects/{name}",
            ),
            expiration=expiration,
            current_version=1,
            members=members,
        )

    # construct the resulting state
    hpc_users = {}
    for u in system_state.ldap_users.values():
        hpc_user = build_hpcuser(u, user_quotas.get(u.uid, {}))
        hpc_users[hpc_user.uuid] = hpc_user
    hpc_groups = {}
    hpc_projects = {}
    for g in system_state.ldap_groups.values():
        if g.cn.startswith(POSIX_AG_PREFIX):
            hpc_group = build_hpcgroup(
                g, group_quotas.get(strip_prefix(g.cn, prefix=POSIX_AG_PREFIX), {})
            )
            if hpc_group:
                hpc_groups[hpc_group.uuid] = hpc_group
        elif g.cn.startswith(POSIX_PROJECT_PREFIX):
            hpc_project = build_hpcproject(
                g, group_quotas.get(strip_prefix(g.cn, prefix=POSIX_PROJECT_PREFIX), {})
            )
            if hpc_project:
                hpc_projects[hpc_project.uuid] = hpc_project
    return HpcaccessState(
        hpc_users=hpc_users,
        hpc_groups=hpc_groups,
        hpc_projects=hpc_projects,
    )


class TargetStateComparison:
    """Helper class that compares two system states.

    Differences are handled as follows.

    - LDAP
        - Missing LDAP objects are created.
        - Existing but differing LDAP objects are updated.
        - Extra LDAP users are disabled by setting `loginShell` to `/sbin/nologin`.
    - file system
        - Missing directories are created.
        - Existing but differing directories are updated.
        - Extra directories have the owner set to ``root:root`` and the access
          to them is disabled.
    """

    def __init__(self, settings: HpcaccessSettings, src: SystemState, dst: SystemState):
        #: Configuration of ``hpc-access`` system to use.
        self.settings = settings
        #: Source state
        self.src = src
        #: Target state
        self.dst = dst

    def run(self) -> OperationsContainer:
        """Run the comparison."""
        console_err.log("Comparing source and target state...")
        result = OperationsContainer(
            ldap_user_ops=self._compare_ldap_users(),
            ldap_group_ops=self._compare_ldap_groups(),
            fs_ops=self._compare_fs_directories(),
        )
        console_err.log("... have operations now.")
        return result

    def _compare_ldap_users(self) -> List[LdapUserOp]:
        """Compare ``LdapUser`` records between system states."""
        result = []
        extra_usernames = set(self.src.ldap_users.keys()) - set(self.dst.ldap_users.keys())
        missing_usernames = set(self.dst.ldap_users.keys()) - set(self.src.ldap_users.keys())
        common_usernames = set(self.src.ldap_users.keys()) & set(self.dst.ldap_users.keys())
        for username in extra_usernames:
            user = self.src.ldap_users[username]
            result.append(LdapUserOp(operation=StateOperation.DISABLE, user=user, diff={}))
        for username in missing_usernames:
            user = self.src.ldap_users[username]
            result.append(LdapUserOp(operation=StateOperation.CREATE, user=user, diff={}))
        for username in common_usernames:
            src_user = self.src.ldap_users[username]
            dst_user = self.dst.ldap_users[username]
            src_user_dict = src_user.model_dump()
            dst_user_dict = dst_user.model_dump()
            all_keys = set(src_user_dict.keys()) | set(dst_user_dict.keys())
            if src_user_dict != dst_user_dict:
                diff = {}
                for key in all_keys:
                    if src_user_dict.get(key) != dst_user_dict.get(key):
                        diff[key] = dst_user_dict.get(key)
                result.append(LdapUserOp(operation=StateOperation.UPDATE, user=src_user, diff=diff))
        return result

    def _compare_ldap_groups(self) -> List[LdapGroupOp]:
        result = []
        extra_group_names = set(self.src.ldap_groups.keys()) - set(self.dst.ldap_groups.keys())
        missing_group_names = set(self.dst.ldap_groups.keys()) - set(self.src.ldap_groups.keys())
        common_group_names = set(self.src.ldap_groups.keys()) & set(self.dst.ldap_groups.keys())
        for name in extra_group_names:
            group = self.src.ldap_groups[name]
            result.append(LdapGroupOp(operation=StateOperation.DISABLE, group=group, diff={}))
        for name in missing_group_names:
            group = self.dst.ldap_groups[name]
            result.append(LdapGroupOp(operation=StateOperation.CREATE, group=group, diff={}))
        for name in common_group_names:
            src_group = self.src.ldap_groups[name]
            dst_group = self.dst.ldap_groups[name]
            src_group_dict = src_group.model_dump()
            dst_group_dict = dst_group.model_dump()
            all_keys = set(src_group_dict.keys()) | set(dst_group_dict.keys())
            if src_group_dict != dst_group_dict:
                diff = {}
                for key in all_keys:
                    if src_group_dict.get(key) != dst_group_dict.get(key):
                        diff[key] = dst_group_dict.get(key)
                result.append(
                    LdapGroupOp(operation=StateOperation.UPDATE, group=src_group, diff=diff)
                )
        return result

    def _compare_fs_directories(self) -> List[FsDirectoryOp]:
        result = []
        extra_paths = set(self.src.fs_directories.keys()) - set(self.dst.fs_directories.keys())
        missing_paths = set(self.dst.fs_directories.keys()) - set(self.src.fs_directories.keys())
        common_paths = set(self.src.fs_directories.keys()) & set(self.dst.fs_directories.keys())
        for path in extra_paths:
            directory = self.src.fs_directories[path]
            result.append(
                FsDirectoryOp(operation=StateOperation.DISABLE, directory=directory, diff={})
            )
        for path in missing_paths:
            directory = self.dst.fs_directories[path]
            result.append(
                FsDirectoryOp(operation=StateOperation.CREATE, directory=directory, diff={})
            )
        for path in common_paths:
            src_directory = self.src.fs_directories[path]
            dst_directory = self.dst.fs_directories[path]
            src_directory_dict = src_directory.model_dump()
            dst_directory_dict = dst_directory.model_dump()
            if src_directory_dict != dst_directory_dict:
                diff = {}
                for key in ("owner_uid", "owner_gid", "perms", "quota_bytes", "quota_files"):
                    if src_directory_dict.get(key) != dst_directory_dict.get(key):
                        diff[key] = dst_directory_dict.get(key)
                result.append(
                    FsDirectoryOp(
                        operation=StateOperation.UPDATE,
                        directory=src_directory,
                        diff=diff,
                    )
                )
        return result
