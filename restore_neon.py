#!/usr/bin/env python3
"""
Resumable PostgreSQL restore for Neon from the backup directory format.

Restore order:
1. pre_data.dump
2. each file in tables/ (sorted by filename)
3. sequences.dump (if present)
4. post_data.dump

If restore is interrupted, rerun this script and it resumes from the last
unfinished step using restore_state.json in the backup directory.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


PRE_DATA_FILENAME = "pre_data.dump"
POST_DATA_FILENAME = "post_data.dump"
SEQUENCES_FILENAME = "sequences.dump"
TABLES_DIRNAME = "tables"
RESTORE_STATE_FILENAME = "restore_state.json"
BACKUP_STATE_FILENAME = "backup_state.json"

# Known Supabase-specific objects that frequently fail on Neon restores.
PRE_DATA_TOC_SKIP_PATTERNS = [
    "EXTENSION - supabase_vault",
    "COMMENT - EXTENSION supabase_vault",
    "FUNCTION realtime list_changes(",
]

# Supabase vault tables depend on extension objects not available on Neon.
TABLE_DUMP_SKIP_PATTERNS = [
    "_vault_",
]


def fail(message):
    print(f"❌ {message}")
    sys.exit(1)


def run_command(command, environment, capture_output=False):
    if capture_output:
        return subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
        )
    return subprocess.run(command, env=environment)


def check_required_tools():
    missing_tools = [tool for tool in ("pg_restore", "psql") if shutil.which(tool) is None]
    if missing_tools:
        fail(
            "Missing PostgreSQL tools: "
            + ", ".join(missing_tools)
            + ". Install PostgreSQL client tools and ensure they are on PATH."
        )


def load_neon_connection_params():
    env_file = Path.cwd() / ".env"
    if not env_file.exists():
        fail(f".env file not found at {env_file}")

    load_dotenv(env_file)
    neon_url = os.getenv("NEON_DB_URL")
    if not neon_url:
        fail("NEON_DB_URL not found in .env")

    try:
        parsed = urlparse(neon_url)
        connection_params = {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "user": parsed.username,
            "password": parsed.password,
            "database": parsed.path.lstrip("/"),
        }
    except Exception as error:
        fail(f"Failed to parse NEON_DB_URL: {error}")

    missing = [
        field
        for field in ("host", "user", "password", "database")
        if not connection_params.get(field)
    ]
    if missing:
        fail(f"Missing connection values in NEON_DB_URL: {', '.join(missing)}")

    return connection_params


def build_command_env(connection_params):
    environment = os.environ.copy()
    environment["PGPASSWORD"] = connection_params["password"]
    return environment


def test_connection(connection_params):
    print("🔄 Testing Neon connection...")
    result = run_command(
        [
            "psql",
            "-h",
            connection_params["host"],
            "-p",
            str(connection_params["port"]),
            "-U",
            connection_params["user"],
            "-d",
            connection_params["database"],
            "-c",
            "SELECT 1;",
        ],
        build_command_env(connection_params),
    )
    if result.returncode != 0:
        fail("Neon connection test failed")
    print("✅ Neon connection successful")


def load_restore_state(backup_dir):
    state_path = backup_dir / RESTORE_STATE_FILENAME
    if not state_path.exists():
        return None
    with state_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_restore_state(backup_dir, state):
    state_path = backup_dir / RESTORE_STATE_FILENAME
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def initialize_restore_state(backup_dir, connection_params):
    state = {
        "version": 1,
        "status": "in_progress",
        "created_at": datetime.now().isoformat(),
        "backup_dir": str(backup_dir),
        "neon_host": connection_params["host"],
        "neon_database": connection_params["database"],
        "completed_steps": [],
        "completed_tables": [],
        "skipped_tables": [],
        "last_error": None,
    }
    save_restore_state(backup_dir, state)
    return state


def load_backup_source_info(backup_dir):
    backup_state_path = backup_dir / BACKUP_STATE_FILENAME
    if not backup_state_path.exists():
        return None

    try:
        with backup_state_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        return None


def print_restore_preflight(backup_dir, connection_params, state):
    source_info = load_backup_source_info(backup_dir)
    source_db = source_info.get("database") if source_info else "unknown"
    source_host = source_info.get("host") if source_info else "unknown"

    tables_dir = backup_dir / TABLES_DIRNAME
    table_count = len(list(tables_dir.glob("*.dump")))
    has_sequences = (backup_dir / SEQUENCES_FILENAME).exists()
    pre_data_already_restored = "pre_data" in state.get("completed_steps", [])

    print("\n" + "=" * 72)
    print("Restore Preflight")
    print("=" * 72)
    print(f"Source backup dir: {backup_dir}")
    print(f"Source database : {source_db}")
    print(f"Source host     : {source_host}")
    print(
        "Target Neon DB : "
        f"{connection_params['user']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    )
    print(f"Table dumps     : {table_count}")
    print(f"Sequences dump  : {'present' if has_sequences else 'not present'}")
    print(
        "Pre-data status : "
        f"{'already restored (will be skipped)' if pre_data_already_restored else 'pending (will run with --clean --if-exists)'}"
    )
    print("=" * 72)


def require_pre_data_confirmation(state):
    if "pre_data" in state.get("completed_steps", []):
        return

    print("⚠️  Pre-data restore will run with --clean --if-exists on the target Neon DB.")
    answer = input("Continue? Type 'yes' to proceed: ").strip().lower()
    if answer != "yes":
        fail("Restore cancelled by user before pre-data clean operation.")


def find_latest_backup_dir():
    candidates = []
    for path in Path.cwd().glob("backup_*"):
        if not path.is_dir():
            continue
        if not (path / PRE_DATA_FILENAME).exists():
            continue
        if not (path / POST_DATA_FILENAME).exists():
            continue
        if not (path / TABLES_DIRNAME).is_dir():
            continue
        candidates.append(path)

    if not candidates:
        fail("No backup directory found. Pass --backup-dir or create a backup first.")

    candidates.sort(key=lambda path: path.name, reverse=True)
    return candidates[0]


def resolve_backup_dir(backup_dir_arg):
    if backup_dir_arg:
        backup_dir = Path(backup_dir_arg).resolve()
        if not backup_dir.exists() or not backup_dir.is_dir():
            fail(f"Backup directory not found: {backup_dir}")
    else:
        backup_dir = find_latest_backup_dir()

    required_files = [
        backup_dir / PRE_DATA_FILENAME,
        backup_dir / POST_DATA_FILENAME,
        backup_dir / TABLES_DIRNAME,
    ]
    missing = [str(path.name) for path in required_files if not path.exists()]
    if missing:
        fail(
            f"Backup directory is missing required files/directories: {', '.join(missing)}"
        )

    return backup_dir


def restore_dump_file(connection_params, dump_path, label, clean=False):
    print(f"📦 {label}: {dump_path.name}")
    command = [
        "pg_restore",
        "--host",
        connection_params["host"],
        "--port",
        str(connection_params["port"]),
        "--username",
        connection_params["user"],
        "--dbname",
        connection_params["database"],
        "--no-owner",
        "--no-privileges",
    ]
    if clean:
        command.extend(["--clean", "--if-exists"])

    command.append(str(dump_path))
    result = run_command(command, build_command_env(connection_params))
    if result.returncode != 0:
        fail(f"Restore failed for {dump_path.name}")


def build_filtered_pre_data_toc(connection_params, pre_data_path):
    list_result = run_command(
        ["pg_restore", "--list", str(pre_data_path)],
        build_command_env(connection_params),
        capture_output=True,
    )
    if list_result.returncode != 0:
        fail("Failed to inspect pre_data.dump contents")

    original_lines = list_result.stdout.splitlines()
    kept_lines = []
    skipped_lines = []

    for line in original_lines:
        if any(pattern in line for pattern in PRE_DATA_TOC_SKIP_PATTERNS):
            skipped_lines.append(line)
            continue
        kept_lines.append(line)

    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".list",
        delete=False,
    )
    with temp_file as handle:
        for line in kept_lines:
            handle.write(line + "\n")

    return Path(temp_file.name), skipped_lines


def restore_pre_data_with_compat(connection_params, pre_data_path):
    toc_path, skipped_lines = build_filtered_pre_data_toc(connection_params, pre_data_path)
    try:
        print(f"📦 Restoring pre-data schema: {pre_data_path.name}")
        if skipped_lines:
            print("⚠️  Applying Neon compatibility filter for pre-data restore")
            print(f"   Skipping {len(skipped_lines)} known incompatible object(s)")

        command = [
            "pg_restore",
            "--host",
            connection_params["host"],
            "--port",
            str(connection_params["port"]),
            "--username",
            connection_params["user"],
            "--dbname",
            connection_params["database"],
            "--no-owner",
            "--no-privileges",
            "--clean",
            "--if-exists",
            "--use-list",
            str(toc_path),
            str(pre_data_path),
        ]

        result = run_command(command, build_command_env(connection_params))
        if result.returncode != 0:
            fail(f"Restore failed for {pre_data_path.name}")
    finally:
        try:
            toc_path.unlink(missing_ok=True)
        except OSError:
            pass


def restore_pre_data(backup_dir, connection_params, state):
    if "pre_data" in state["completed_steps"]:
        print("⏭️  Skipping pre-data (already restored)")
        return

    restore_pre_data_with_compat(connection_params, backup_dir / PRE_DATA_FILENAME)
    state["completed_steps"].append("pre_data")
    state["last_error"] = None
    save_restore_state(backup_dir, state)
    print("✅ Pre-data restored")


def restore_tables(backup_dir, connection_params, state):
    tables_dir = backup_dir / TABLES_DIRNAME
    table_files = sorted(tables_dir.glob("*.dump"), key=lambda path: path.name)
    total = len(table_files)
    if total == 0:
        print("ℹ️  No table dumps found")
        return

    print(f"📚 Table dumps to restore: {total}")

    for index, dump_path in enumerate(table_files, start=1):
        if any(pattern in dump_path.name for pattern in TABLE_DUMP_SKIP_PATTERNS):
            print(f"⏭️  Skipping incompatible table dump {dump_path.name}")
            skipped_tables = state.setdefault("skipped_tables", [])
            if dump_path.name not in skipped_tables:
                skipped_tables.append(dump_path.name)
                save_restore_state(backup_dir, state)
            continue

        if dump_path.name in state["completed_tables"]:
            continue

        print(f"📄 Restoring table dump {dump_path.name} ({index}/{total})")
        try:
            restore_dump_file(connection_params, dump_path, "Restoring table")
            state["completed_tables"].append(dump_path.name)
            state["last_error"] = None
            save_restore_state(backup_dir, state)
            print(f"✅ Restored {dump_path.name}")
        except SystemExit:
            state["last_error"] = {
                "file": dump_path.name,
                "step": "tables",
                "updated_at": datetime.now().isoformat(),
            }
            save_restore_state(backup_dir, state)
            raise


def restore_sequences(backup_dir, connection_params, state):
    sequence_path = backup_dir / SEQUENCES_FILENAME
    if not sequence_path.exists():
        print("ℹ️  No sequences dump present")
        return

    if "sequences" in state["completed_steps"]:
        print("⏭️  Skipping sequences (already restored)")
        return

    restore_dump_file(connection_params, sequence_path, "Restoring sequences")
    state["completed_steps"].append("sequences")
    state["last_error"] = None
    save_restore_state(backup_dir, state)
    print("✅ Sequences restored")


def restore_post_data(backup_dir, connection_params, state):
    if "post_data" in state["completed_steps"]:
        print("⏭️  Skipping post-data (already restored)")
        return

    restore_dump_file(
        connection_params,
        backup_dir / POST_DATA_FILENAME,
        "Restoring post-data schema",
    )
    state["completed_steps"].append("post_data")
    state["last_error"] = None
    save_restore_state(backup_dir, state)
    print("✅ Post-data restored")


def mark_restore_completed(backup_dir, state):
    state["status"] = "completed"
    state["completed_at"] = datetime.now().isoformat()
    state["last_error"] = None
    save_restore_state(backup_dir, state)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Restore backup directory to Neon in resumable order."
    )
    parser.add_argument(
        "--backup-dir",
        help="Path to backup_<db>_<timestamp> directory. Defaults to latest backup_*."
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Start restore from scratch by deleting restore_state.json first."
    )
    return parser.parse_args()


def main():
    print("=" * 72)
    print("Resumable Neon Restore Tool")
    print("=" * 72)

    args = parse_args()
    check_required_tools()

    backup_dir = resolve_backup_dir(args.backup_dir)
    connection_params = load_neon_connection_params()
    test_connection(connection_params)

    state_file = backup_dir / RESTORE_STATE_FILENAME
    if args.restart and state_file.exists():
        state_file.unlink()
        print("🧹 Removed previous restore_state.json (restart requested)")

    state = load_restore_state(backup_dir)
    if state and state.get("status") == "in_progress":
        print(f"🔁 Resuming restore for: {backup_dir.name}")
        print(
            "   Progress: "
            f"{len(state.get('completed_tables', []))} table dumps restored"
        )
    elif state and state.get("status") == "completed" and not args.restart:
        print("✅ Restore already marked as completed for this backup directory")
        print("   Use --restart if you want to restore it again from scratch")
        return
    else:
        print(f"🆕 Starting restore for: {backup_dir.name}")
        state = initialize_restore_state(backup_dir, connection_params)

    print_restore_preflight(backup_dir, connection_params, state)
    require_pre_data_confirmation(state)

    restore_pre_data(backup_dir, connection_params, state)
    restore_tables(backup_dir, connection_params, state)
    restore_sequences(backup_dir, connection_params, state)
    restore_post_data(backup_dir, connection_params, state)
    mark_restore_completed(backup_dir, state)

    print("\n✅ Restore completed successfully")
    print(f"   Backup source: {backup_dir}")
    print(
        "   Target Neon DB: "
        f"{connection_params['user']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    )


if __name__ == "__main__":
    main()
