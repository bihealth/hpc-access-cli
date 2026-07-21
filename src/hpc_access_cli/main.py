import sys
from typing import List

import mechanize
import typer
from rich.console import Console
from typing_extensions import Annotated

from hpc_access_cli.config import load_settings
from hpc_access_cli.constants import ENTITIES, ENTITY_USERS
from hpc_access_cli.fs import FS_GROUP_OPS, FS_PROJECT_OPS, FS_USER_OPS
from hpc_access_cli.models import StateOperation
from hpc_access_cli.states import (
    TargetStateBuilder,
    TargetStateComparison,
    convert_to_hpcaccess_state,
    deploy_hpcaccess_state,
    fs_validation,
    gather_hpcaccess_state,
    gather_system_state,
    user_dn,
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
    hpcaccess_state = gather_hpcaccess_state(settings.hpc_access)
    dst_builder = TargetStateBuilder(hpcaccess_state, src_state)
    dst_state = dst_builder.run()
    comparison = TargetStateComparison(src_state, dst_state)
    operations = comparison.run()
    group_by_gid = {g.gid: g for g in hpcaccess_state.hpc_groups.values()}
    user_by_uuid = {u.uuid: u for u in hpcaccess_state.hpc_users.values()}
    owner_by_dn = {
        user_dn(user_by_uuid[g.owner]): user_by_uuid[g.owner].username
        for g in hpcaccess_state.hpc_groups.values()
    }
    # console_err.print_json(data=operations.model_dump(mode="json"))
    with open("ldap_user_ops.ldif", "w") as fh_ldap_user_ops:
        for user_op in operations.ldap_user_ops:
            if user_op.operation == StateOperation.CREATE:
                console_err.log(f"create user {user_op.user.dn}")
                fh_ldap_user_ops.write(f"dn: {user_op.user.dn}\n")
                fh_ldap_user_ops.write("changetype: add\n")
                fh_ldap_user_ops.write("objectClass: inetOrgPerson\n")
                fh_ldap_user_ops.write("objectClass: posixAccount\n")
                fh_ldap_user_ops.write("objectClass: ldapPublicKey\n")
                fh_ldap_user_ops.write("objectClass: bih-expireDates\n")
                fh_ldap_user_ops.write("objectClass: top\n")
                fh_ldap_user_ops.write(f"cn: {user_op.user.cn}\n")
                fh_ldap_user_ops.write(f"gidNumber: {user_op.user.gid_number}\n")
                fh_ldap_user_ops.write(f"homeDirectory: {user_op.user.home_directory}\n")
                fh_ldap_user_ops.write(f"sn: {user_op.user.sn}\n")
                fh_ldap_user_ops.write(f"uid: {user_op.user.uid}\n")
                fh_ldap_user_ops.write(f"uidNumber: {user_op.user.uid_number}\n")
                if user_op.user.given_name:
                    fh_ldap_user_ops.write(f"givenName: {user_op.user.given_name}\n")
                if user_op.user.login_shell:
                    fh_ldap_user_ops.write(f"loginShell: {user_op.user.login_shell}\n")
                if user_op.user.mail:
                    fh_ldap_user_ops.write(f"mail: {user_op.user.mail}\n")
                if user_op.user.telephone_number:
                    fh_ldap_user_ops.write(f"telephoneNumber: {user_op.user.telephone_number}\n")
                fh_ldap_user_ops.write("\n")

                group_folders = group_by_gid[user_op.user.gid_number].folders
                users_folder = f"users/{user_op.user.uid}"

                with open(f"fs_user_ops_{user_op.user.uid}.sh", "w") as fh_fs_user_ops:
                    fh_fs_user_ops.write(
                        FS_USER_OPS
                        % {
                            "username": user_op.user.uid,
                            "folder_home": user_op.user.home_directory,
                            "folder_work": f"{group_folders.tier1_work}/{users_folder}",
                            "folder_scratch": f"{group_folders.tier1_scratch}/{users_folder}",
                            "folder_group_work": group_folders.tier1_work,
                            "folder_group_scratch": group_folders.tier1_scratch,
                        }
                    )

            elif user_op.operation == StateOperation.UPDATE:
                console_err.log(f"update user {user_op.user.dn}")
                fh_ldap_user_ops.write(f"dn: {user_op.user.dn}\n")
                fh_ldap_user_ops.write("changetype: modify\n")
                for i, (key, value) in enumerate(user_op.diff.items(), 1):
                    if not value:
                        fh_ldap_user_ops.write(f"delete: {key}\n")
                    else:
                        fh_ldap_user_ops.write(f"replace: {key}\n")
                        fh_ldap_user_ops.write(f"{key}: {value}\n")
                    if i < len(user_op.diff):
                        fh_ldap_user_ops.write("-\n")
                fh_ldap_user_ops.write("\n")

            elif user_op.operation == StateOperation.DISABLE:
                console_err.log(f"disable user {user_op.user.dn}")
                fh_ldap_user_ops.write(f"dn: {user_op.user.dn}\n")
                fh_ldap_user_ops.write("changetype: modify\n")
                fh_ldap_user_ops.write("replace: login_shell\n")
                fh_ldap_user_ops.write("login_shell: /usr/sbin/nologin\n\n")

    with open("ldap_group_ops.ldif", "w") as fh_ldap_group_ops:
        for group_op in operations.ldap_group_ops:
            if group_op.operation == StateOperation.CREATE:
                console_err.log(f"create group {group_op.group.dn}")
                fh_ldap_group_ops.write(f"dn: {group_op.group.dn}\n")
                fh_ldap_group_ops.write("changetype: add\n")
                fh_ldap_group_ops.write("objectClass: groupOfNames\n")
                fh_ldap_group_ops.write("objectClass: top\n")
                fh_ldap_group_ops.write(f"cn: {group_op.group.cn}\n")
                for member in group_op.group.member_uids:
                    fh_ldap_group_ops.write(f"member: {member}\n")
                fh_ldap_group_ops.write("\n")
                FS_OPS = FS_PROJECT_OPS if group_op.group.cn.startswith("hpc-prj") else FS_GROUP_OPS
                group = group_by_gid[group_op.group.gid_number]
                with open(f"fs_group_ops_{group_op.group.cn}.sh", "w") as fh_fs_group_ops:
                    fh_fs_group_ops.write(
                        FS_OPS
                        % {
                            "owner": owner_by_dn[group_op.group.owner_dn],
                            "group": group_op.group.cn,
                            "quota1": int(group.resources_requested.tier1_work),
                            "quota2": int(group.resources_requested.tier1_scratch),
                            "folder_work": group.folders.tier1_work,
                            "folder_scratch": group.folders.tier1_scratch,
                            "folder_unmirrored": group.folders.tier2_unmirrored,
                        }
                    )

            elif group_op.operation == StateOperation.UPDATE:
                console_err.log(f"update group {group_op.group.dn}")
                fh_ldap_group_ops.write(f"dn: {group_op.group.dn}\n")
                fh_ldap_group_ops.write("changetype: modify\n")
                for i, (key, value) in enumerate(group_op.diff.items(), 1):
                    if not value:
                        fh_ldap_group_ops.write(f"delete: {key}\n")
                    else:
                        fh_ldap_group_ops.write(f"replace: {key}\n")
                        fh_ldap_group_ops.write(f"{key}: {value}\n")
                    if i < len(group_op.diff):
                        fh_ldap_group_ops.write("-\n")
                fh_ldap_group_ops.write("\n")

    # connection = LdapConnection(settings.ldap_hpc)
    # console_err.log(f"applying LDAP group operations now, dry_run={dry_run}")
    # for group_op in operations.ldap_group_ops:
    #     connection.apply_group_op(group_op, dry_run)
    # console_err.log(f"applying LDAP user operations now, dry_run={dry_run}")
    # for user_op in operations.ldap_user_ops:
    #     connection.apply_user_op(user_op, dry_run)
    # console_err.log(f"applying file system operations now, dry_run={dry_run}")
    # fs_mgr = FsResourceManager("")
    # for fs_op in operations.fs_ops:
    #     fs_mgr.apply_fs_op(fs_op, dry_run)


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
