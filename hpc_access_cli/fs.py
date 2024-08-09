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
