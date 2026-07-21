#!/usr/bin/env python3
"""Generate Pydantic models from the hpc-access OpenAPI schema.

Reads the schema shipped inside the *hpc-access* package
(``hpc_access.openapi-schema.json``) and emits a self-contained
``api_models.py`` module under ``src/hpc_access_cli/``.

Usage:
    python scripts/generate_models.py
"""

from __future__ import annotations

import importlib.resources
import pathlib
import sys
import tempfile

from datamodel_code_generator import DataModelType, Formatter, PythonVersion, generate

SCHEMA_PACKAGE = "hpc_access"
SCHEMA_RESOURCE = "openapi-schema.json"
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = _REPO_ROOT / "src" / "hpc_access_cli" / "api_models.py"


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

    # Build the output file with clean imports.
    parts: list[str] = [
        '"""Auto-generated Pydantic models from the hpc-access OpenAPI schema.',
        "",
        "Regenerate with:  python scripts/generate_models.py",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "import datetime",
        "from typing import Any, Optional",
        "from uuid import UUID",
        "",
        "from pydantic import BaseModel, Field",
        "",
    ]

    # Collect needed imports from the generated code.
    generated_lines = raw.splitlines()
    extra_imports: set[str] = set()
    for line in generated_lines:
        stripped = line.strip()
        if stripped.startswith("class ") or stripped.startswith("    "):
            if "AnyUrl" in stripped:
                extra_imports.add("AnyUrl")
            if "AwareDatetime" in stripped:
                extra_imports.add("AwareDatetime")

    if extra_imports:
        # Insert after the existing imports block.
        idx = 7  # after "from pydantic import BaseModel, Field"
        for imp in sorted(extra_imports):
            if imp == "AwareDatetime":
                parts.insert(idx, "from datetime import datetime")
                idx += 1
            elif imp == "AnyUrl":
                parts.insert(idx, "from pydantic import AnyUrl")
                idx += 1

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


if __name__ == "__main__":
    main()
