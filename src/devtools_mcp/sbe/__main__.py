"""python -m devtools_mcp.sbe compile SCHEMA --output HEADER --expect-sha256 HASH"""

import argparse
import sys
from pathlib import Path

from . import CompilerBudgets, SchemaError, compile_schema, generate_cpp, generate_encoder_cpp
from .codegen import select_templates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("compile")
    command.add_argument("schema", type=Path)
    command.add_argument("--output", required=True, type=Path)
    command.add_argument("--namespace", default="sbe_generated")
    command.add_argument("--expect-sha256", required=True)
    command.add_argument("--max-group-entries", type=int, default=65535)
    command.add_argument("--max-data-bytes", type=int, default=1024 * 1024)
    command.add_argument("--encoder", action="store_true", help="emit standalone namespace::encoder native encoder")
    command.add_argument(
        "--template-id", type=int, action="append", help="emit only these exact template IDs; repeatable"
    )
    args = parser.parse_args()
    try:
        budgets = CompilerBudgets(group_entries=args.max_group_entries, data_bytes=args.max_data_bytes)
        with args.schema.open("rb") as source:
            xml = source.read(budgets.file_bytes + 1)
        ir = compile_schema(xml, budgets=budgets, expect_sha256=args.expect_sha256)
        emitted_count = len(select_templates(ir, args.template_id).messages)
        header = (generate_encoder_cpp if args.encoder else generate_cpp)(
            ir, namespace=args.namespace, template_ids=args.template_id
        )
        args.output.write_text(header, encoding="utf-8")
    except (SchemaError, OSError) as error:
        print(f"SBE compilation failed: {error}", file=sys.stderr)
        return 1
    print(f"compiled {emitted_count} messages; schema={ir.schema_id}/{ir.version}; sha256={ir.sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
