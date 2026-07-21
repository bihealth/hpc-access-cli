"""Configuration for hpc-access-cli."""

import sys
from pathlib import Path
from typing import List

import typer
from pydantic import BaseModel, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from rich.console import Console

from hpc_access_cli.models import StateOperation

#: The rich console to use for output.
console_err = Console(file=sys.stderr)


class LdapSettings(BaseModel):
    """Configuration of LDAP."""

    #: The hostname of the LDAP server.
    server_host: str
    #: The port of the LDAP server.
    server_port: int = 389
    #: The distinguished name of the user to bind to the server.
    bind_dn: str
    #: The password of the user to bind to the server.
    bind_pw: SecretStr
    #: The base DN to search for users.
    search_base: str


class SmtpSettings(BaseModel):
    """Configuration for sending out emails via SMTP."""

    #: The hostname of the SMTP server.
    server_host: str
    #: The username for the SMTP server.
    sender_email: str


class MailmanSettings(BaseModel):
    """Configuration for managing mailman subscriptions."""

    #: URL to server to use.
    server_url: HttpUrl
    #: Password to use for logging into mailman.
    admin_password: SecretStr


class HpcaccessSettings(BaseModel):
    """Configuration for the hpc-access server."""

    #: The server base url.
    server_url: HttpUrl
    #: The token to use.
    api_token: SecretStr


class Settings(BaseSettings):
    """Configuration of hpc-access-cli."""

    #: Configuration for internal LDAP.
    ldap_hpc: LdapSettings
    #: Configuration for sending out emails via SMTP.
    smtp: SmtpSettings
    #: Configuration for managing mailman subscriptions.
    mailman: MailmanSettings
    #: HPC access server configuration.
    hpc_access: HpcaccessSettings

    #: Operations to perform on LDAP users.
    ldap_user_ops: List[StateOperation] = [
        StateOperation.CREATE,
        StateOperation.UPDATE,
        StateOperation.DISABLE,
    ]
    #: Operations to perform on LDAP groups.
    ldap_group_ops: List[StateOperation] = [
        StateOperation.CREATE,
        StateOperation.UPDATE,
        StateOperation.DISABLE,
    ]
    #: Operations to perform on file system directories.
    fs_ops: List[StateOperation] = [
        StateOperation.CREATE,
        StateOperation.UPDATE,
        StateOperation.DISABLE,
    ]
    #: Whether try run is enabled.
    dry_run: bool = False

    #: Obtaining configuration from environment variables.
    model_config = SettingsConfigDict(env_prefix="HPC_ACCESS_")


def load_settings(config_path: str) -> Settings:
    """Load configuration from the given path.

    :param path: The path to the configuration file.
    :return: The loaded configuration.
    :raises typer.Exit: If the configuration file does not exist.
    """
    if not Path(config_path).exists():
        console_err.log(f"ERROR: Configuration file {config_path} does not exist.", style="red")
        raise typer.Exit(1)
    with open(config_path, "rt") as f:
        return Settings.model_validate_json(f.read())
