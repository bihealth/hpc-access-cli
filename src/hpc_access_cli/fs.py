"""Code for load file system resource management."""

import errno
import os
import sys
from pathlib import Path
from subprocess import check_call
from typing import Dict, List

import xattr
from rich.console import Console

from hpc_access_cli.constants import BASE_PATH_TIER1, BASE_PATH_TIER2
from hpc_access_cli.models import FsDirectory, FsDirectoryOp, StateOperation

#: The rich console to use for logging.
console_err = Console(file=sys.stderr)


FS_USER_OPS = r"""
#!/bin/bash

USERNAME=%(username)s
HOME=%(folder_home)s
WORK=%(folder_work)s
SCRATCH=%(folder_scratch)s
GROUP_WORK=%(folder_group_work)s
GROUP_SCRATCH=%(folder_group_scratch)s

# Check that user and group exist
if ! getent passwd $USERNAME >/dev/null; then
    >&2 echo "User $USERNAME does not exist."
    exit 1
fi

# Check that the group folders exist
for dir in $GROUP_WORK $GROUP_SCRATCH; do
    if ! [[ -d "$dir" ]]; then
        >&2 echo "$dir directory does not exist."
        exit 1
    fi
done

# Check that the directories do not exist yet
for dir in $HOME $WORK $SCRATCH; do
    if [[ -d $dir ]]; then
        >&2 echo "Directory $dir already exists."
        exit 1
    fi
done

mkdir $HOME
mkdir $WORK
mkdir $SCRATCH

# Create utility dirs
mkdir $WORK/{R,ondemand,.apptainer,.local}
mkdir $SCRATCH/.cache

# Set quota
setfattr -n ceph.quota.max_bytes -v $((1*(1024**3))) $HOME

# Transfer skel directory
rsync -av /etc/skel.bih/. $HOME/.
mkdir -p $HOME/.ssh
touch $HOME/.ssh/authorized_keys
chmod -R u=rwX,go= $HOME
ssh-keygen -t ed25519 -C "$USERNAME  on BIH HPC" -f $HOME/.ssh/id_ed25519 -N "" 2>/dev/null
cat $HOME/.ssh/id_ed25519.pub >> $HOME/.ssh/authorized_keys

# Set remaining owners and permissions
chown -R $USERNAME:$GROUP $HOME
chown -R $USERNAME:$GROUP $WORK
chown -R $USERNAME:$GROUP $SCRATCH
chmod 700 $WORK
chmod 700 $SCRATCH

echo "Creating symlinks..."
ln -s $WORK $HOME/work
ln -s $SCRATCH $HOME/scratch
ln -s work/R $HOME/R
ln -s work/ondemand $HOME/ondemand
ln -s work/.local $HOME/.local
ln -s work/.apptainer $HOME/.apptainer
ln -s scratch/.cache $HOME/.cache
"""


FS_GROUP_OPS = r"""
#!/bin/bash

OWNER=%(owner)s
GROUP=%(group)s
QUOTA1=%(quota1)s
QUOTA2=%(quota2)s
WORK=%(folder_work)s
SCRATCH=%(folder_scratch)s
TIER2=%(folder_unmirrored)s

# Check if group exists
if ! getent group $GROUP >/dev/null; then
    >&2 echo "Group $GROUP does not exist."
    exit 1
fi

# Check if quotas are valid
if ! [[ "$QUOTA1" =~ ^[1-9]+$ || "$QUOTA2" =~ ^[1-9]+$ ]]; then
    >&2 echo "Quotas $QUOTA1 and $QUOTA2 are not positive integers."
    exit 1
fi

# Check if owner exists
if [[ $OWNER == "" ]] || ! getent passwd $OWNER >/dev/null; then
    >&2 echo "User $OWNER does not exist."
    exit 1
fi

# Check that the directories do not exist yet
for dir in $WORK $SCRATCH $TIER2; do
    if [[ -d $dir ]]; then
        >&2 echo "Directory $dir already exists."
        exit 1
    fi
done

mkdir -p $WORK/users
mkdir -p $SCRATCH/users
mkdir $TIER2
chown -R $OWNER:$GROUP $WORK
chown -R $OWNER:$GROUP $SCRATCH
chown -R $OWNER:$GROUP $TIER2
chmod 770 $WORK
chmod 770 $SCRATCH
chmod 770 $TIER2
chmod 750 $WORK/users
chmod 750 $SCRATCH/users

# Set quotas
setfattr -n ceph.quota.max_bytes -v $(($QUOTA1*(1024**4))) $WORK
setfattr -n ceph.quota.max_bytes -v $(($QUOTA1*10*(1024**4))) $SCRATCH
setfattr -n ceph.quota.max_bytes -v $(($QUOTA2*(1024**4))) $TIER2
"""


FS_PROJECT_OPS = r"""
#!/bin/bash

GROUP=%(group)s
QUOTA1=%(quota1)s
QUOTA2=%(quota2)s
OWNER=%(owner)s
WORK=%(folder_work)s
SCRATCH=%(folder_scratch)s
TIER2=%(folder_unmirrored)s

# Check if group exists
if ! getent group $GROUP >/dev/null; then
    >&2 echo "Group $GROUP does not exist."
    exit 1
fi

# Check if quotas are valid
if ! [[ "$QUOTA1" =~ ^[0-9]+$ || "$QUOTA2" =~ ^[0-9]+$ ]]; then
    >&2 echo "Quotas $QUOTA1 and $QUOTA2 are not integers."
    exit 1
fi

# Check if owner exists
if [[ $OWNER == "" ]] || ! getent passwd $OWNER >/dev/null; then
    >&2 echo "User $OWNER does not exist."
    exit 1
fi

# Tier 1
if [[ "$QUOTA1" -ne "0" ]]; then
    # Check that the directories do not exist yet
    for dir in $WORK $SCRATCH; do
        if [[ -d $dir ]]; then
            >&2 echo "Directory $dir already exists."
            exit 1
        fi
    done

    echo "Creating Tier 1 location with $QUOTA1 TiB quota."
    mkdir $WORK
    mkdir $SCRATCH
    chown -R $OWNER:$GROUP $WORK
    chown -R $OWNER:$GROUP $SCRATCH
    chmod -R 2770 $WORK
    chmod -R 2770 $SCRATCH

    # Set quotas
    setfattr -n ceph.quota.max_bytes -v $(($QUOTA1*(1024**4))) $WORK
    setfattr -n ceph.quota.max_bytes -v $((10*(1024**4))) $SCRATCH

    ln -s $SCRATCH $WORK/scratch
fi

# Tier 2
if [[ "$QUOTA2" -ne "0" ]]; then
    # Check that the directories do not exist yet
    for dir in $TIER2; do
        if [[ -d $dir ]]; then
            >&2 echo "Directory $dir already exists."
            exit 1
        fi
    done

    echo "Creating Tier 2 location with $QUOTA2 TiB quota."
    mkdir $TIER2
    chown -R $OWNER:$GROUP $TIER2
    chmod -R 2770 $TIER2

    # Set quota
    setfattr -n ceph.quota.max_bytes -v $(($QUOTA2*(1024**4))) $TIER2
fi
"""


def get_extended_attribute(path: str, attr_name: str) -> str:
    """Get the value of an extended attribute."""
    try:
        # Get the value of the specified extended attribute
        value = xattr.getxattr(path, attr_name).decode("utf-8")
        return value
    except OSError as e:
        if os.environ.get("DEBUG", "0") == "1":
            return "0"
        # Handle the case when the attribute is not found
        if e.errno == errno.ENODATA:
            raise ValueError(f"extended attribute {attr_name} not found") from e
        else:
            # Re-raise the exception for other errors
            raise


def _transform_perms(perms: str) -> str:
    """Transform the permissions string."""
    perms_user = perms[1:4].replace("-", "")
    if "S" in perms_user:
        perms_user = f"u={perms_user.replace('S', '')},u+s"
    elif "S" in perms_user:
        perms_user = f"u={perms_user.replace('s', 'x')},u+s"
    else:
        perms_user = f"u={perms_user},u-s"
    perms_group = perms[4:7].replace("-", "")
    if "S" in perms_group:
        perms_group = f"g={perms_group.replace('S', '')},g+s"
    elif "s" in perms_group:
        perms_group = f"g={perms_group.replace('s', 'x')},g+s"
    perms_other = perms[7:].replace("-", "").replace("S", "").replace("s", "x")
    perms_other = f"o={perms_other},o-s"
    return f"{perms_user},{perms_group},{perms_other}"


class FsResourceManager:
    """Helper class to manage resources on file system.

    Effectively, it reads/writes the well-known folders and attributes.
    """

    def __init__(self, *, prefix: str = ""):
        self.path_tier1_home = f"{prefix}{BASE_PATH_TIER1}/home"
        self.path_tier1_work = f"{prefix}{BASE_PATH_TIER1}/work"
        self.path_tier1_scratch = f"{prefix}{BASE_PATH_TIER1}/scratch"
        self.path_tier2_mirrored = f"{prefix}{BASE_PATH_TIER2}/mirrored"
        self.path_tier2_unmirrored = f"{prefix}{BASE_PATH_TIER2}/unmirrored"

    def load_directories(self) -> List[FsDirectory]:
        """Load the directories and their sizes."""
        result = []
        for path in (self.path_tier1_home, self.path_tier1_work, self.path_tier1_scratch):
            for path_obj in Path(path).glob("*/*"):
                if path_obj.is_dir():
                    result.append(FsDirectory.from_path(str(path_obj)))
        for path in (self.path_tier2_mirrored, self.path_tier2_unmirrored):
            for path_obj in Path(path).glob("*/*"):
                if path_obj.is_dir():
                    result.append(FsDirectory.from_path(str(path_obj)))
        result.sort(key=lambda x: x.path)
        return result

    def apply_fs_op(self, fs_op: FsDirectoryOp, dry_run: bool = False):
        """Apply the file system operations."""
        if fs_op.operation == StateOperation.CREATE:
            self._fs_op_create(fs_op.directory, dry_run)
        elif fs_op.operation == StateOperation.DISABLE:
            self._fs_op_disable(fs_op.directory, dry_run)
        elif fs_op.operation == StateOperation.UPDATE:
            self._fs_op_update(fs_op.directory, fs_op.diff, dry_run)

    def _fs_op_create(self, directory: FsDirectory, dry_run: bool):
        perms = _transform_perms(directory.perms)
        console_err.log(f"+ mkdir -v -m {perms} -p {directory.path}")
        console_err.log(
            f"+ chown -c {directory.owner_name}:{directory.group_name} {directory.path}"
        )
        if not dry_run:
            check_call(["mkdir", "-v", "-m", perms, "-p", directory.path])
            check_call(
                ["chown", "-c", f"{directory.owner_name}:{directory.group_name}", directory.path]
            )

    def _fs_op_disable(self, directory: FsDirectory, dry_run: bool):
        console_err.log(f"+ setfattr -n ceph-quota.max_files -v 0 {directory.path}")
        if not dry_run:
            check_call(["setfattr", "-n", "ceph-quota.max_files", "-v", "0", directory.path])

    def _fs_op_update(
        self, directory: FsDirectory, diff: Dict[str, None | int | str], dry_run: bool
    ):
        for key, value in diff.items():
            if key == "quota_bytes":
                if value is None:
                    console_err.log(f"+ setfattr -x ceph-quota.max_bytes {directory.path}")
                    if not dry_run:
                        check_call(["setfattr", "-x", "ceph-quota.max_bytes", directory.path])
                else:
                    console_err.log(
                        f"+ setfattr -n ceph-quota.max_bytes -v {value} {directory.path}"
                    )
                    if not dry_run:
                        check_call(
                            [
                                "setfattr",
                                "-n",
                                "ceph-quota.max_bytes",
                                "-v",
                                f"{value}",
                                directory.path,
                            ]
                        )
            elif key == "quota_files":
                if value is None:
                    console_err.log(f"+ setfattr -x ceph-quota.max_files {directory.path}")
                    if not dry_run:
                        check_call(["setfattr", "-x", "ceph-quota.max_files", directory.path])
                else:
                    console_err.log(
                        f"+ setfattr -n ceph-quota.max_files -v {value} {directory.path}"
                    )
                    if not dry_run:
                        check_call(
                            [
                                "setfattr",
                                "-n",
                                "ceph-quota.max_files",
                                "-v",
                                f"{value}",
                                directory.path,
                            ]
                        )
            elif key in ["owner_name", "owner_uid"]:
                console_err.log(f"+ chown -c {value} {directory.path}")
                if not dry_run:
                    check_call(["chown", "-c", f"{value}", directory.path])
            elif key in ["group_name", "group_gid"]:
                console_err.log(f"+ chgrp -c {value} {directory.path}")
                if not dry_run:
                    check_call(["chgrp", "-c", f"{value}", directory.path])
            elif key == "perms":
                perms = _transform_perms(directory.perms)
                console_err.log(f"+ chmod -c {perms} {directory.path}")
                if not dry_run:
                    check_call(["chmod", "-c", perms, directory.path])
            else:
                raise ValueError(f"I don't know how to handle fs directory diff key '{key}'")
