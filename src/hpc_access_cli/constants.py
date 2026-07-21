#: Prefix for POSIX groups
POSIX_AG_PREFIX = "hpc-ag-"
#: Prefix for POSIX projects
POSIX_PROJECT_PREFIX = "hpc-prj-"

#: Base path for tier1
BASE_PATH_TIER1 = "/data/cephfs-1"
#: Base path for tier2
BASE_PATH_TIER2 = "/data/cephfs-2"

#: Base DN for work groups.
BASE_DN_GROUPS = "ou=Teams,ou=Groups,dc=hpc,dc=bihealth,dc=org"
#: Base DN for projects
BASE_DN_PROJECTS = "ou=Projects,ou=Groups,dc=hpc,dc=bihealth,dc=org"
#: Base DN for Charite users
BASE_DN_CHARITE = "ou=Charite,ou=Users,dc=hpc,dc=bihealth,dc=org"
#: Base DN for MDC users
BASE_DN_MDC = "ou=MDC,ou=Users,dc=hpc,dc=bihealth,dc=org"

#: Quota on user home (1G)
QUOTA_HOME_BYTES = 1024 * 1024 * 1024
#: Quota on scratch (100T)
QUOTA_SCRATCH_BYTES = 100 * 1024 * 1024 * 1024 * 1024

#: Group name for users without a group.
HPC_ALUMNIS_GROUP = "hpc-alumnis"
#: GID for users without a group.
HPC_ALUMNIS_GID = 1030001
#: Group name for hpc-users (active+has home)
HPC_USERS_GROUP = "hpc-users"
#: GID for hpc-users
HPC_USERS_GID = 1005269

ENTITY_USERS = "users"
ENTITY_GROUPS = "groups"
ENTITY_PROJECTS = "projects"
ENTITIES = (
    ENTITY_USERS,
    ENTITY_GROUPS,
    ENTITY_PROJECTS,
)

TIER_USER_HOME = "tier1_home"
TIER_WORK = "tier1_work"
TIER_SCRATCH = "tier1_scratch"
TIER_UNMIRRORED = "tier2_unmirrored"
TIER_MIRRORED = "tier2_mirrored"

FOLDER_HOME = "home"
FOLDER_WORK = "work"
FOLDER_SCRATCH = "scratch"
FOLDER_UNMIRRORED = "unmirrored"
FOLDER_MIRRORED = "mirrored"

FOLDER_CEPHFS1 = "cephfs-1"
FOLDER_CEPHFS2 = "cephfs-2"

CEPHFS_TIER_MAPPING = {
    (FOLDER_CEPHFS1, FOLDER_HOME, ENTITY_USERS): TIER_USER_HOME,
    (FOLDER_CEPHFS1, FOLDER_WORK, ENTITY_PROJECTS): TIER_WORK,
    (FOLDER_CEPHFS1, FOLDER_WORK, ENTITY_GROUPS): TIER_WORK,
    (FOLDER_CEPHFS1, FOLDER_SCRATCH, ENTITY_PROJECTS): TIER_SCRATCH,
    (FOLDER_CEPHFS1, FOLDER_SCRATCH, ENTITY_GROUPS): TIER_SCRATCH,
    (FOLDER_CEPHFS2, FOLDER_UNMIRRORED, ENTITY_PROJECTS): TIER_UNMIRRORED,
    (FOLDER_CEPHFS2, FOLDER_UNMIRRORED, ENTITY_GROUPS): TIER_UNMIRRORED,
    (FOLDER_CEPHFS2, FOLDER_MIRRORED, ENTITY_PROJECTS): TIER_MIRRORED,
    (FOLDER_CEPHFS2, FOLDER_MIRRORED, ENTITY_GROUPS): TIER_MIRRORED,
}
PREFIX_MAPPING = {
    "projects": POSIX_PROJECT_PREFIX,
    "groups": POSIX_AG_PREFIX,
}
RE_PATH = r"/(?P<tier>cephfs-[12])/(?P<subdir>[^/]+)/(?P<entity>[^/]+)/(?P<name>[^/]+)"
