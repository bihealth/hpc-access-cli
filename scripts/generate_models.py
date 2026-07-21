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

SCHEMA_PACKAGE = "hpc_access"
SCHEMA_RESOURCE = "openapi-schema.json"
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = _REPO_ROOT / "src" / "hpc_access_cli" / "api_models.py"


def main() -> None:
    schema_ref = importlib.resources.files(SCHEMA_PACKAGE).joinpath(SCHEMA_RESOURCE)
    with importlib.resources.as_file(schema_ref) as schema_path:
        if not schema_path.exists():
            sys.exit(f"Schema not found: {schema_path}")

        subprocess.run(
            [
                "uv",
                "run",
                "datamodel-codegen",
                "--input",
                str(schema_path),
                "--input-file-type",
                "openapi",
                "--output",
                str(OUTPUT_PATH),
                "--output-model-type",
                "pydantic_v2.BaseModel",
                "--target-python-version",
                "3.12",
                "--use-standard-collections",
                "--use-union-operator",
                "--field-constraints",
                "--snake-case-field",
                "--allof-merge-mode",
                "all",
                "--allow-remote-refs",
                "--disable-timestamp",
                "--formatters",
                "builtin",
            ],
            check=True,
        )

    _rewrite_header()
    _rename_status_enum()
    subprocess.run(["uv", "run", "ruff", "check", "--fix", str(OUTPUT_PATH)])
    subprocess.run(["uv", "run", "ruff", "format", str(OUTPUT_PATH)])
    print(f"Wrote {OUTPUT_PATH}")


def _rewrite_header() -> None:
    lines = OUTPUT_PATH.read_text().splitlines()
    code_start = 0
    for i, line in enumerate(lines):
        if line.startswith("from ") or line.startswith("import "):
            code_start = i
            break

    header = [
        '"""Auto-generated Pydantic models from the hpc-access OpenAPI schema.',
        "",
        "Regenerate with:  python scripts/generate_models.py",
        '"""',
        "",
    ]

    OUTPUT_PATH.write_text("\n".join(header + lines[code_start:]) + "\n")


def _rename_status_enum() -> None:
    content = OUTPUT_PATH.read_text()
    content = content.replace("StatusEnum", "Status")
    OUTPUT_PATH.write_text(content)


if __name__ == "__main__":
    main()
