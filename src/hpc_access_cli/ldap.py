"""Code for interfacing with LDAP servers."""

import sys
from typing import Any, Dict, List, Optional

import humps
import ldap3
from rich.console import Console

from hpc_access_cli.config import LdapSettings
from hpc_access_cli.models import (
    LOGIN_SHELL_DISABLED,
    # Gecos,
    LdapGroup,
    LdapGroupOp,
    LdapUser,
    LdapUserOp,
    StateOperation,
)

#: The rich console to use for output.
console_err = Console(file=sys.stderr)

#: The object classes for users.
USER_OBJ_CLASSES = ("inetOrgPerson", "posixAccount", "ldapPublicKey", "bih-expireDates", "top")


def attribute_as_str(attribute: ldap3.Attribute) -> Optional[str]:
    """Get attribute as string or None if empty."""
    if len(attribute):
        return str(attribute[0])
    else:
        return None


def attribute_list_as_str_list(
    attribute: ldap3.Attribute,
) -> List[str]:
    """Get attribute as list of strings."""
    return [str(x) for x in attribute]


class LdapConnection:
    """Wrapper around an ``ldap3`` connection."""

    def __init__(self, config: LdapSettings):
        #: The configuration for the LDAP connection.
        self.config = config
        #: Server to connect to.
        self.server = ldap3.Server(
            host=config.server_host,
            port=config.server_port,
        )
        console_err.log(f"Connecting to {self.server.host}:{self.server.port}...")
        #: Connection to the LDAP server.
        self.connection = ldap3.Connection(
            server=self.server,
            user=config.bind_dn,
            password=config.bind_pw.get_secret_value(),
            auto_bind=True,
        )
        if not self.connection.bind():
            raise Exception("Failed to bind to LDAP server.")
        console_err.log("... connected.")

    def load_users(self) -> List[LdapUser]:
        """Load ``LdapUser`` records from the LDAP server."""
        search_filter = "(&(objectClass=posixAccount)(uid=*))"

        console_err.log(f"Searching for users with filter {search_filter}...")
        if not self.connection.search(
            search_base=self.config.search_base,
            search_filter=search_filter,
            search_scope=ldap3.SUBTREE,
            attributes=[
                "sn",
                "givenName",
                "cn",
                "uid",
                "uidNumber",
                "gidNumber",
                "homeDirectory",
                # "gecos",
                "loginShell",
                "mail",
                "displayName",
                "sshPublicKey",
                "telephoneNumber",
            ],
        ):
            raise Exception("Failed to search for users.")
        result = []
        for entry in self.connection.entries:
            # gecos_str = attribute_as_str(entry.gecos)
            # gecos = Gecos.from_string(gecos_str) if gecos_str else None
            uid_str = attribute_as_str(entry.uidNumber)
            uid_number = int(uid_str) if uid_str else None
            if not uid_number:
                raise ValueError(f"Missing LDAP attribute uidNumber for {entry.entry_dn}")
            gid_str = attribute_as_str(entry.gidNumber)
            gid_number = int(gid_str) if gid_str else None
            if not gid_number:
                raise ValueError(f"Missing LDAP attribute gidNumber for {entry.entry_dn}")
            cn = attribute_as_str(entry.cn)
            if not cn:
                raise ValueError(f"Missing LDAP attribute cn for {entry.entry_dn}")
            uid = attribute_as_str(entry.uid)
            if not uid:
                raise ValueError(f"Missing LDAP attribute uid for {entry.entry_dn}")
            sn = attribute_as_str(entry.sn)
            given_name = attribute_as_str(entry.givenName)
            display_name = attribute_as_str(entry.displayName)
            home_directory = attribute_as_str(entry.homeDirectory)
            if not home_directory:
                raise ValueError(f"Missing LDAP attribute homeDirectory for {entry.entry_dn}")
            login_shell = attribute_as_str(entry.loginShell)
            if not login_shell:
                raise ValueError(f"Missing LDAP attribute loginShell for {entry.entry_dn}")
            result.append(
                LdapUser(
                    dn=entry.entry_dn,
                    cn=cn,
                    uid=uid,
                    sn=sn,
                    mail=attribute_as_str(entry.mail),
                    given_name=given_name,
                    display_name=display_name,
                    uid_number=uid_number,
                    gid_number=gid_number,
                    home_directory=home_directory,
                    login_shell=login_shell,
                    telephone_number=attribute_as_str(entry.telephoneNumber),
                    # gecos=None,
                    # ssh_public_key=attribute_list_as_str_list(entry.sshPublicKey),
                )
            )
        return result

    def apply_user_op(self, op: LdapUserOp, dry_run: bool):
        """Apply a user operation to the LDAP server."""
        if op.operation == StateOperation.CREATE:
            self._user_op_create(op.user, dry_run)
        elif op.operation == StateOperation.DISABLE:
            self._user_op_disable(op.user, dry_run)
        elif op.operation == StateOperation.UPDATE:
            self._user_op_update(op.user, op.diff, dry_run)

    def _user_op_create(self, user: LdapUser, dry_run: bool):
        user_data = {
            "cn": user.cn,
            "uid": user.uid,
            "uidNumber": user.uid_number,
            "homeDirectory": user.home_directory,
            "mail": user.mail,
            "telephoneNumber": user.telephone_number,
            "loginShell": user.login_shell,
            "gidNumber": user.gid_number,
            "displayName": user.display_name,
        }
        if user.sn:
            user_data["sn"] = user.sn
        if user.given_name:
            user_data["givenName"] = user.given_name
        console_err.log(
            f"+ create LDAP user\nDN={user.dn}\nclasses={USER_OBJ_CLASSES}\ndata={user_data}"
        )
        if not dry_run:
            self.connection.add(
                user.dn,
                USER_OBJ_CLASSES,
                user_data,
            )

    def _user_op_disable(self, user: LdapUser, dry_run: bool):
        console_err.log(f"+ disable LDAP user DN: {user.dn}")
        search_params = {
            "search_base": self.config.search_base,
            "search_filter": f"(&(objectClass=posixAccount)(uid={user.uid}))",
            "search_scope": ldap3.SUBTREE,
            "attributes": [
                "objectclass",
                "uid",
                "uidNumber",
                "mail",
                "telephoneNumber",
                "displayName",
                "sshPublicKey",
                "loginShell",
                "sn",
                "givenName",
            ],
            "paged_size": 20,
            "generator": False,
        }
        if not self.connection.extend.standard.paged_search(**search_params):
            msg = f"FATAL: could not find users with search base {self.config.search_base}"
            raise Exception(msg)
        writable = self.connection.entries[0].entry_writable()
        writable["loginShell"] = LOGIN_SHELL_DISABLED
        if not dry_run:
            if not writable.entry_commit_changes():
                raise Exception(f"Failed to disable user {user.uid}.")
            else:
                console_err.log(f"user diabled CN: {user.cn}")

    def _user_op_update(
        self,
        user: LdapUser,
        diff: Dict[str, None | int | str | List[str] | Dict[str, Any]],
        dry_run: bool,
    ):
        search_params = {
            "search_base": self.config.search_base,
            "search_filter": f"(&(objectClass=posixAccount)(uid={user.uid}))",
            "search_scope": ldap3.SUBTREE,
            "attributes": [
                "objectclass",
                "uid",
                "uidNumber",
                "mail",
                "telephoneNumber",
                "displayName",
                "sshPublicKey",
                "loginShell",
                "sn",
                "givenName",
            ],
            "paged_size": 20,
            "generator": False,
        }
        if not self.connection.extend.standard.paged_search(**search_params):
            msg = f"FATAL: could not find users with search base {self.config.search_base}"
            raise Exception(msg)
        writable = self.connection.entries[0].entry_writable()
        applied_diff = {}
        for key, value in diff.items():
            key = humps.camelize(key)
            # if key == "gecos":
            #     gecos: Gecos = value or Gecos()  # type: ignore
            #     applied_diff[key] = Gecos.model_validate(gecos).to_string()
            if key == "sshPublicKey":
                # We only support clearing this list for now which is fine as the
                # SSH keys live in the upstream ADs only.
                applied_diff[key] = [(ldap3.MODIFY_DELETE, x) for x in writable[key]]
            else:
                applied_diff[key] = value or ""
            writable[key] = value or ""
        console_err.log(f"+ update LDAP user DN: {user.dn}, diff: {applied_diff}")
        if not dry_run:
            if not writable.entry_commit_changes():
                raise Exception(f"Failed to disable user {user.uid}.")
            else:
                console_err.log(f"upser updated DN: {user.dn}")

    def load_groups(self) -> List[LdapGroup]:
        """Load group names from the LDAP server."""
        search_filter = "(&(objectClass=posixGroup)(cn=*))"

        console_err.log(f"Searching for groups with filter {search_filter}...")
        if not self.connection.search(
            search_base=self.config.search_base,
            search_filter=search_filter,
            search_scope=ldap3.SUBTREE,
            attributes=[
                "cn",
                "gidNumber",
                "bih-groupOwnerDN",
                "bih-groupDelegateDNs",
                "memberUid",
                "description",
            ],
        ):
            raise Exception("Failed to search for groups.")
        result = []
        for entry in self.connection.entries:
            cn = attribute_as_str(entry.cn)
            if not cn:
                raise ValueError(f"Missing LDAP attribute cn for {entry.entry_dn}")
            gid_str = attribute_as_str(entry.gidNumber)
            gid_number = int(gid_str) if gid_str else None
            if not gid_number:
                raise ValueError(f"Missing LDAP attribute gidNumber for {entry.entry_dn}")
            owner_dn = attribute_as_str(entry["bih-groupOwnerDN"])
            delegate_dns = attribute_list_as_str_list(entry["bih-groupDelegateDNs"])
            member_uids = sorted(attribute_list_as_str_list(entry.memberUid))
            result.append(
                LdapGroup(
                    dn=entry.entry_dn,
                    cn=cn,
                    gid_number=gid_number,
                    description=attribute_as_str(entry.description),
                    owner_dn=owner_dn,
                    delegate_dns=delegate_dns,
                    member_uids=member_uids,
                )
            )
        return result

    def apply_group_op(self, op: LdapGroupOp, dry_run: bool):
        """Apply a group operation to the LDAP server."""
        if op == StateOperation.CREATE:
            self._group_op_create(op.group, dry_run)
        elif op == StateOperation.DISABLE:
            self._group_op_disable(op.group, dry_run)
        elif op == StateOperation.UPDATE:
            self._group_op_update(op.group, op.diff, dry_run)

    def _group_op_create(self, group: LdapGroup, dry_run: bool):
        pass

    def _group_op_disable(self, group: LdapGroup, dry_run: bool):
        """Disabling a group in LDAP currently is a no-op as this is applied on
        the file system by setting the file count quota to 0.
        """
        _, _ = group, dry_run

    def _group_op_update(
        self,
        group: LdapGroup,
        diff: Dict[str, None | int | str | List[str] | Dict[str, Any]],
        dry_run: bool,
    ):
        console_err.log(f"+ update LDAP group DN: {group.dn}, diff: {diff}")
        search_params = {
            "search_base": self.config.search_base,
            "search_filter": f"(&(objectClass=gidNumber)(gidNumber={group.gid_number}))",
            "search_scope": ldap3.SUBTREE,
            "attributes": ["*"],
            "paged_size": 20,
            "generator": False,
        }
        if not self.connection.extend.standard.paged_search(**search_params):
            msg = f"FATAL: could not find group with search base {self.config.search_base}"
            raise Exception(msg)
        writable = self.connection.entries[0].entry_writable()

        for key, value in diff.items():
            writable[key] = value
        if not dry_run:
            if not writable.entry_commit_changes():
                raise Exception(f"Failed to update DN: {group.dn}.")
            else:
                console_err.log(f"group updated DN: {group.dn}")
