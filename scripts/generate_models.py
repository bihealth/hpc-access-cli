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
import pathlib
import subprocess
import sys
import tempfile

from datamodel_code_generator import DataModelType, Formatter, PythonVersion, generate

SCHEMA_PACKAGE = "hpc_access"
SCHEMA_RESOURCE = "openapi-schema.json"
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = _REPO_ROOT / "src" / "hpc_access_cli" / "api_models.py"

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


def main() -> None:
    schema_ref = importlib.resources.files(SCHEMA_PACKAGE).joinpath(SCHEMA_RESOURCE)
    with importlib.resources.as_file(schema_ref) as schema_path:
        if not schema_path.exists():
            sys.exit(f"Schema not found: {schema_path}")

        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as tmp:
            tmp_path = pathlib.Path(tmp.name)

        try:
            generate(
                input_=schema_path,
                input_file_type="openapi",
                output=tmp_path,
                output_model_type=DataModelType.PydanticV2BaseModel,
                target_python_version=PythonVersion.PY_312,
                use_standard_collections=True,
                use_union_operator=True,
                field_constraints=True,
                snake_case_field=True,
                formatters=[Formatter.BUILTIN],
            )
            raw = tmp_path.read_text()
        finally:
            tmp_path.unlink(missing_ok=True)

    generated_lines = raw.splitlines()

    # Collect needed imports from the generated code.
    extra_imports: set[str] = set()
    for line in generated_lines:
        stripped = line.strip()
        if stripped.startswith("class ") or line.startswith("    "):
            if "AnyUrl" in stripped:
                extra_imports.add("AnyUrl")
            if "AwareDatetime" in stripped:
                extra_imports.add("AwareDatetime")
            if "Field(" in stripped:
                extra_imports.add("Field")

    # Build the header.
    pydantic_imports = ["BaseModel"] + sorted(extra_imports)
    pydantic_line = f"from pydantic import {', '.join(pydantic_imports)}"

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
        pydantic_line,
    ]

    parts.append("")
    parts.append(HAND_MAINTAINED.strip())

    # Extract class definitions from the generated code.
    in_class = False
    for line in generated_lines:
        stripped = line.strip()
        if stripped.startswith("class "):
            in_class = True
        if in_class:
            parts.append(line)

    OUTPUT_PATH.write_text("\n".join(parts) + "\n")
    print(f"Wrote {OUTPUT_PATH}")

    subprocess.run(["uv", "run", "ruff", "check", "--fix", str(OUTPUT_PATH)])
    subprocess.run(["uv", "run", "ruff", "format", str(OUTPUT_PATH)])


if __name__ == "__main__":
    main()
