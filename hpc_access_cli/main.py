import os
import sys
from typing import List

import mechanize
import typer
from rich.console import Console
from typing_extensions import Annotated

from hpc_access_cli.config import load_settings
from hpc_access_cli.constants import ENTITIES, ENTITY_USERS
from hpc_access_cli.fs import FsResourceManager
from hpc_access_cli.ldap import LdapConnection
from hpc_access_cli.models import StateOperation
from hpc_access_cli.states import (
    TargetStateBuilder,
    TargetStateComparison,
    convert_to_hpcaccess_state,
    deploy_hpcaccess_state,
    fs_validation,
    gather_hpcaccess_state,
    gather_system_state,
)

#: The typer application object to use.
app = typer.Typer()
#: The rich console to use for output.
console_err = Console(file=sys.stderr)
console_out = Console(file=sys.stdout)


@app.command("mailman-sync")
def mailman_sync(
    config_path: Annotated[
        str, typer.Option(..., help="path to configuration file")
    ] = "/etc/hpc-access-cli/config.json",
    dry_run: Annotated[bool, typer.Option(..., help="perform a dry run (no changes)")] = True,
):
    """obtain email addresses of active users and sync to mailman"""
    settings = load_settings(config_path)
    dst_state = gather_hpcaccess_state(settings.hpc_access)
    emails = list(sorted(user.email for user in dst_state.hpc_users.values() if user.email))
    console_err.log(f"will update to {len(emails)} email addresses")
    console_err.log("\n".join(emails))

    console_err.log(f"Opening URL to mailman '{settings.mailman.server_url}' ...")
    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.open(str(settings.mailman.server_url))
    console_err.log("  ... filling login form")
    br.select_form(nr=0)
    br["adminpw"] = settings.mailman.admin_password.get_secret_value()
    console_err.log("  ... submitting login form")
    _ = br.submit()
    console_err.log("  ... filling sync membership list form")
    br.select_form(nr=0)
    br["memberlist"] = "\n".join(emails)
    if br.forms()[0].action != str(settings.mailman.server_url):  # type: ignore
        raise Exception(f"unexpected form action {br.forms()[0].action}")  # type: ignore
    console_err.log("  ... submitting sync membership list form")
    if dry_run:
        console_err.log("  ... **dry run, not submitting**")
    else:
        _ = br.submit()
    console_err.log("... done")


@app.command("state-dump")
def dump_data(
    config_path: Annotated[
        str, typer.Option(..., help="path to configuration file")
    ] = "/etc/hpc-access-cli/config.json",
):
    """dump system state as hpc-access state"""
    settings = load_settings(config_path)
    console_err.print_json(data=settings.model_dump(mode="json"))
    system_state = gather_system_state(settings)
    hpcaccess_state = convert_to_hpcaccess_state(system_state)
    console_out.print_json(data=hpcaccess_state.model_dump(mode="json"))


@app.command("state-sync")
def sync_data(
    config_path: Annotated[
        str, typer.Option(..., help="path to configuration file")
    ] = "/etc/hpc-access-cli/config.json",
    ldap_user_ops: Annotated[
        List[StateOperation],
        typer.Option(..., help="user operations to perform (default: all)"),
    ] = list,
    ldap_group_ops: Annotated[
        List[StateOperation],
        typer.Option(..., help="group operations to perform (default: all)"),
    ] = list,
    fs_ops: Annotated[
        List[StateOperation],
        typer.Option(..., help="file system operations to perform (default: all)"),
    ] = list,
    dry_run: Annotated[bool, typer.Option(..., help="perform a dry run (no changes)")] = True,
):
    """sync hpc-access state to HPC LDAP"""
    settings = load_settings(config_path).model_copy(
        update={
            "ldap_user_ops": ldap_user_ops or list(StateOperation),
            "ldap_group_ops": ldap_group_ops or list(StateOperation),
            "fs_ops": fs_ops or list(StateOperation),
            "dry_run": dry_run,
        }
    )
    # console_err.print_json(data=settings.model_dump(mode="json"))
    src_state = gather_system_state(settings)
    dst_builder = TargetStateBuilder(settings.hpc_access, src_state)
    dst_state = dst_builder.run()
    comparison = TargetStateComparison(settings.hpc_access, src_state, dst_state)
    operations = comparison.run()
    # console_err.print_json(data=operations.model_dump(mode="json"))
    connection = LdapConnection(settings.ldap_hpc)
    console_err.log(f"applying LDAP group operations now, dry_run={dry_run}")
    for group_op in operations.ldap_group_ops:
        connection.apply_group_op(group_op, dry_run)
    console_err.log(f"applying LDAP user operations now, dry_run={dry_run}")
    for user_op in operations.ldap_user_ops:
        connection.apply_user_op(user_op, dry_run)
    console_err.log(f"applying file system operations now, dry_run={dry_run}")
    fs_mgr = FsResourceManager(prefix="/data/sshfs" if os.environ.get("DEBUG", "0") == "1" else "")
    for fs_op in operations.fs_ops:
        fs_mgr.apply_fs_op(fs_op, dry_run)


@app.command("storage-usage-sync")
def sync_storage_usage(
    config_path: Annotated[
        str, typer.Option(..., help="path to configuration file")
    ] = "/etc/hpc-access-cli/config.json",
    dry_run: Annotated[bool, typer.Option(..., help="perform a dry run (no changes)")] = True,
):
    """sync storage usage to hpc-access"""
    settings = load_settings(config_path)
    src_state = gather_system_state(settings)
    dst_state = gather_hpcaccess_state(settings.hpc_access)
    hpcaccess = {e: dict() for e in ENTITIES}

    for entity in hpcaccess.keys():
        for d in getattr(dst_state, "hpc_%s" % entity).values():
            d.resources_used = {}
            name = d.username if entity == ENTITY_USERS else d.name
            hpcaccess[entity][name] = d

    for fs_data in src_state.fs_directories.values():
        try:
            entity, name, resource = fs_validation(fs_data)
        except ValueError as e:
            console_err.log(f"WARNING: {e}")
            continue

        if not hpcaccess.get(entity, {}).get(name):
            console_err.log(f"WARNING: folder not present in hpc-access: {entity}/{name}")
            continue

        # The following lines update the entries in dst_state (!)
        d = getattr(dst_state, f"hpc_{entity}")
        p = 4 - int(entity == ENTITY_USERS)
        d[hpcaccess[entity][name].uuid].resources_used[resource] = fs_data.rbytes / 1024**p

    if not dry_run:
        deploy_hpcaccess_state(settings.hpc_access, dst_state)

    console_err.log(f"syncing storage usage to hpc-access now, dry_run={dry_run}")


if __name__ == "__main__":
    app()
