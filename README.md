# hpc-access-cli

CLI tool for [hpc-access](https://github.com/bihealth/hpc-access) -- syncing HPC cluster state between the web application, LDAP directories, and the Ceph file system.

## Installation

```bash
pip install hpc-access-cli
```

## Commands

| Command | Description |
|---|---|
| `hpc-access-cli state-dump` | Dump current system state (LDAP + filesystem) as hpc-access state |
| `hpc-access-cli state-sync` | Sync hpc-access state to HPC LDAP and filesystem (generates LDIF files and shell scripts) |
| `hpc-access-cli storage-usage-sync` | Sync storage usage from filesystem back to hpc-access |
| `hpc-access-cli mailman-sync` | Sync active user email addresses to a Mailman mailing list |

## Configuration

The CLI reads its configuration from a JSON file (default: `/etc/hpc-access-cli/config.json`) or from environment variables with the `HPC_ACCESS_` prefix.

### Config file format

```json
{
  "ldap_hpc": {
    "server_host": "ldap.example.com",
    "server_port": 389,
    "bind_dn": "cn=admin,dc=hpc,dc=bihealth,dc=org",
    "bind_pw": "secret",
    "search_base": "dc=hpc,dc=bihealth,dc=org"
  },
  "smtp": {
    "server_host": "smtp.example.com",
    "sender_email": "hpc-admin@example.com"
  },
  "mailman": {
    "server_url": "https://mailman.example.com/admin/hpc-users",
    "admin_password": "secret"
  },
  "hpc_access": {
    "server_url": "https://hpc-access.example.com",
    "api_token": "your-api-token"
  }
}
```

## Development

```bash
git clone https://github.com/bihealth/hpc-access-cli.git
cd hpc-access-cli
uv sync --group dev
make lint
make format
```

## License

MIT
