#!/usr/bin/env python3
"""Generate Pydantic models from the hpc-access OpenAPI schema.

Reads the schema shipped inside the *hpc-access* package
(``hpc_access.openapi-schema.json``) and emits a self-contained
``api_models.py`` module under ``src/hpc_access_cli/``.

The file is auto-formatted with ``ruff`` after generation so the
output always passes lint and format checks.

Usage:
    python scripts/generate_models.py
"""

from __future__ import annotations

import importlib.resources
import json
import pathlib
import subprocess
import sys

SCHEMA_PACKAGE = "hpc_access"
SCHEMA_RESOURCE = "openapi-schema.json"
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = _REPO_ROOT / "src" / "hpc_access_cli" / "api_models.py"

# Schema-derived models to generate (order matters for dependencies).
SCHEMA_MODELS = ["HpcUser", "HpcGroup", "HpcProject"]

# Maps OpenAPI type+format to Python type annotations.
TYPE_MAP: dict[tuple[str, str | None], str] = {
    ("string", "date-time"): "datetime.datetime",
    ("string", "uuid"): "UUID",
    ("string", None): "str",
    ("integer", None): "int",
    ("boolean", None): "bool",
    ("number", None): "float",
}

# Hand-maintained types for JSON fields and enums that the schema doesn't expose.
HAND_MAINTAINED = '''\


@enum.unique
class Status(enum.Enum):
    """Status of a hpc user, group, or project."""

    INITIAL = "INITIAL"
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    EXPIRED = "EXPIRED"


class ResourceData(BaseModel):
    """A resource request/usage for a group or project."""

    #: Storage on tier 1 in TiB (work).
    tier1_work: float = 0.0
    #: Storage on tier 1 in TiB (scratch).
    tier1_scratch: float = 0.0
    #: Storage on tier 2 (mirrored) in TiB.
    tier2_mirrored: float = 0.0
    #: Storage on tier 2 (unmirrored) in TiB.
    tier2_unmirrored: float = 0.0


class ResourceDataUser(BaseModel):
    """A resource request/usage for a user."""

    #: Storage on tier 1 in GiB (home).
    tier1_home: float = 0.0


class GroupFolders(BaseModel):
    """Folders for a group or project."""

    #: The work directory.
    tier1_work: str
    #: The scratch directory.
    tier1_scratch: str
    #: The mirrored directory.
    tier2_mirrored: str
    #: The unmirrored directory.
    tier2_unmirrored: str
'''


def _resolve_type(prop: dict) -> str:
    """Resolve an OpenAPI property schema to a Python type string."""
    t = prop.get("type")
    fmt = prop.get("format")
    if t == "array":
        items = prop.get("items", {})
        return f"list[{_resolve_type(items)}]"
    return TYPE_MAP.get((t, fmt), "Any")


def _gen_model(name: str, schema: dict) -> str:
    """Generate a Pydantic model class from an OpenAPI schema object."""
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    desc = schema.get("description", "")

    lines: list[str] = []
    if desc:
        lines.append(f'    """{desc}"""')
        lines.append("")

    for fname, fdef in props.items():
        ftype = _resolve_type(fdef)
        fdesc = fdef.get("description", "")

        if fname not in required:
            ftype = f"{ftype} | None"
            default = "None"
        else:
            default = "..."

        comment = f"  # {fdesc}" if fdesc else ""
        lines.append(f"    {fname}: {ftype} = {default}{comment}")

    return "\n".join(lines)


def main() -> None:
    schema_ref = importlib.resources.files(SCHEMA_PACKAGE).joinpath(SCHEMA_RESOURCE)
    with importlib.resources.as_file(schema_ref) as schema_path:
        if not schema_path.exists():
            sys.exit(f"Schema not found: {schema_path}")
        schema = json.loads(schema_path.read_text())

    schemas = schema.get("components", {}).get("schemas", {})

    parts: list[str] = [
        '"""Auto-generated Pydantic models from the hpc-access OpenAPI schema.',
        "",
        "Regenerate with:  python scripts/generate_models.py",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "import datetime",
        "import enum",
        "from typing import Any",
        "from uuid import UUID",
        "",
        "from pydantic import BaseModel",
        "",
        HAND_MAINTAINED.strip(),
    ]

    for name in SCHEMA_MODELS:
        spec = schemas.get(name)
        if spec is None:
            sys.exit(f"Schema '{name}' not found")

        model_body = _gen_model(name, spec)
        parts.append(f"\n\nclass {name}(BaseModel):")
        parts.append(model_body)

    parts.append("")  # trailing newline

    OUTPUT_PATH.write_text("\n".join(parts))
    print(f"Wrote {OUTPUT_PATH}")

    # Let ruff fix imports and formatting.
    subprocess.run(["uv", "run", "ruff", "check", "--fix", str(OUTPUT_PATH)], check=True)
    subprocess.run(["uv", "run", "ruff", "format", str(OUTPUT_PATH)], check=True)
    print("Formatted with ruff")


if __name__ == "__main__":
    main()
