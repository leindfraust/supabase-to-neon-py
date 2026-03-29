#!/usr/bin/env python3
"""
Resumable PostgreSQL backup for Supabase to Neon migration.

Why this approach:
- A single pg_dump custom archive cannot resume after a dropped SSL connection.
- This script creates a resumable backup directory instead:
  - pre-data schema dump
  - one data dump per table
  - sequence dump
  - post-data schema dump
- If the connection drops midway, rerunning the script automatically continues
  from the last unfinished table.
"""

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


STATE_FILENAME = "backup_state.json"
PRE_DATA_FILENAME = "pre_data.dump"
POST_DATA_FILENAME = "post_data.dump"
SEQUENCES_FILENAME = "sequences.dump"
TABLES_DIRNAME = "tables"
MAX_TABLE_RETRIES = 3
RETRY_DELAY_SECONDS = 5


def fail(message):
    print(f"❌ {message}")
    sys.exit(1)


def load_env_variables():
    env_file = Path.cwd() / ".env"
    if not env_file.exists():
        fail(f".env file not found at {env_file}")

    load_dotenv(env_file)
    database_url = os.getenv("DATABASE_DIRECT_URL")
    if not database_url:
        fail("DATABASE_DIRECT_URL not found in .env")

    return database_url


def parse_database_url(database_url):
    try:
        parsed = urlparse(database_url)
        connection_params = {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "user": parsed.username,
            "password": parsed.password,
            "database": parsed.path.lstrip("/"),
        }
    except Exception as error:
        fail(f"Failed to parse DATABASE_DIRECT_URL: {error}")

    missing = [
        field
        for field in ("host", "user", "password", "database")
        if not connection_params.get(field)
    ]
    if missing:
        fail(f"Missing connection values in DATABASE_DIRECT_URL: {', '.join(missing)}")

    return connection_params


def build_command_env(connection_params):
    environment = os.environ.copy()
    environment["PGPASSWORD"] = connection_params["password"]
    return environment


def run_command(command, environment, show_output=False):
    """Run subprocess with optional output display.
    
    Args:
        command: Command to run
        environment: Environment variables
        show_output: If True, stream output to console for user feedback
    """
    if show_output:
        # Stream output in real-time for user visibility during long operations
        return subprocess.run(
            command,
            env=environment,
        )
    else:
        # Capture output silently for error reporting
        return subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
        )


def check_required_tools():
    missing_tools = [tool for tool in ("pg_dump", "psql", "pg_restore") if shutil.which(tool) is None]
    if missing_tools:
        fail(
            "Missing PostgreSQL tools: "
            + ", ".join(missing_tools)
            + ". Install PostgreSQL client tools and ensure they are on PATH."
        )


def test_connection(connection_params):
    print("🔄 Testing database connection...")
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
        fail(f"Connection failed: {result.stderr.strip()}")
    print("✅ Connection successful")


def warn_about_legacy_partial_archives():
    legacy_files = sorted(Path.cwd().glob("*.tmp.bak"))
    if not legacy_files:
        return

    print("⚠️  Found legacy partial .tmp.bak archives.")
    print("   Those files cannot be resumed automatically because pg_dump does not")
    print("   support restarting a single interrupted custom archive.")
    for legacy_file in legacy_files:
        size_mb = legacy_file.stat().st_size / (1024 * 1024)
        print(f"   - {legacy_file.name} ({size_mb:.2f} MB)")
    print("   The new backup format resumes automatically using per-table dumps.\n")


def load_state(backup_dir):
    state_path = backup_dir / STATE_FILENAME
    if not state_path.exists():
        return None
    with state_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(backup_dir, state):
    state_path = backup_dir / STATE_FILENAME
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def find_resumable_backup_dir(database_name):
    candidates = []
    for path in Path.cwd().glob("backup_*"):
        if not path.is_dir():
            continue
        state = load_state(path)
        if not state:
            continue
        if state.get("status") != "in_progress":
            continue
        if state.get("database") != database_name:
            continue
        candidates.append((path, state))

    if not candidates:
        return None, None

    candidates.sort(key=lambda item: item[1].get("created_at", ""), reverse=True)
    return candidates[0]


def create_backup_dir(database_name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path.cwd() / f"backup_{database_name}_{timestamp}"
    (backup_dir / TABLES_DIRNAME).mkdir(parents=True, exist_ok=False)
    return backup_dir


def list_user_tables(connection_params):
    print("📝 Scanning database tables...")
    query = (
        "SELECT format('%I.%I', schemaname, tablename) "
        "FROM pg_tables "
        "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY pg_total_relation_size(format('%I.%I', schemaname, tablename)::regclass) DESC, 1;"
    )
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
            "-At",
            "-c",
            query,
        ],
        build_command_env(connection_params),
    )
    if result.returncode != 0:
        fail(f"Failed to list tables: {result.stderr.strip()}")

    tables = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    print(f"✅ Found {len(tables)} tables")
    return tables


def list_sequences(connection_params):
    print("📝 Scanning database sequences...")
    query = (
        "SELECT format('%I.%I', sequence_schema, sequence_name) "
        "FROM information_schema.sequences "
        "WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY 1;"
    )
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
            "-At",
            "-c",
            query,
        ],
        build_command_env(connection_params),
    )
    if result.returncode != 0:
        fail(f"Failed to list sequences: {result.stderr.strip()}")

    sequences = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if sequences:
        print(f"✅ Found {len(sequences)} sequences")
    else:
        print(f"ℹ️  No sequences found")
    return sequences


def sanitize_table_filename(index, table_name):
    safe_name = []
    for character in table_name:
        if character.isalnum():
            safe_name.append(character)
        else:
            safe_name.append("_")
    collapsed = "".join(safe_name).strip("_")
    return f"{index:04d}_{collapsed}.dump"


def initialize_state(backup_dir, connection_params, tables):
    table_entries = []
    for index, table_name in enumerate(tables, start=1):
        table_entries.append(
            {
                "name": table_name,
                "file": sanitize_table_filename(index, table_name),
            }
        )

    state = {
        "version": 2,
        "status": "in_progress",
        "created_at": datetime.now().isoformat(),
        "database": connection_params["database"],
        "host": connection_params["host"],
        "backup_dir": str(backup_dir),
        "completed_tables": [],
        "tables": table_entries,
        "last_error": None,
    }
    save_state(backup_dir, state)
    return state


def pg_dump_base_command(connection_params):
    return [
        "pg_dump",
        "--host",
        connection_params["host"],
        "--port",
        str(connection_params["port"]),
        "--username",
        connection_params["user"],
        "--format=custom",
        "--compress=9",
        connection_params["database"],
    ]


def dump_component(connection_params, command, label):
    """Execute pg_dump for schema components with user feedback."""
    print(f"   Running pg_dump...")
    result = run_command(
        command,
        build_command_env(connection_params),
        show_output=True,  # Show verbose pg_dump output
    )

    if result.returncode == 0:
        print(f"✅ {label}")
        return

    error_text = (result.stderr or result.stdout or "Unknown pg_dump error").strip()
    fail(f"{label} failed:\n{error_text}")


def ensure_pre_data_dump(backup_dir, connection_params):
    target_path = backup_dir / PRE_DATA_FILENAME
    if target_path.exists():
        print(f"⏭️  Reusing {PRE_DATA_FILENAME}")
        return

    print("📦 Dumping pre-data schema...")
    command = pg_dump_base_command(connection_params) + [
        "--section=pre-data",
        "--file",
        str(target_path),
    ]
    dump_component(connection_params, command, "Pre-data schema dump completed")


def ensure_post_data_dump(backup_dir, connection_params):
    target_path = backup_dir / POST_DATA_FILENAME
    if target_path.exists():
        print(f"⏭️  Reusing {POST_DATA_FILENAME}")
        return

    print("📦 Dumping post-data schema...")
    command = pg_dump_base_command(connection_params) + [
        "--section=post-data",
        "--file",
        str(target_path),
    ]
    dump_component(connection_params, command, "Post-data schema dump completed")


def ensure_sequences_dump(backup_dir, connection_params, sequences):
    target_path = backup_dir / SEQUENCES_FILENAME
    if target_path.exists():
        print(f"⏭️  Reusing {SEQUENCES_FILENAME}")
        return
    if not sequences:
        print("ℹ️  No user sequences found")
        return

    print("📦 Dumping sequence values...")
    command = pg_dump_base_command(connection_params) + [
        "--data-only",
        "--file",
        str(target_path),
    ]
    for sequence_name in sequences:
        command.extend(["--table", sequence_name])
    dump_component(connection_params, command, "Sequence dump completed")


def dump_table_with_retries(backup_dir, connection_params, table_entry, state):
    table_name = table_entry["name"]
    target_path = backup_dir / TABLES_DIRNAME / table_entry["file"]
    if table_name in state["completed_tables"] and target_path.exists():
        return True

    for attempt in range(1, MAX_TABLE_RETRIES + 1):
        if target_path.exists():
            target_path.unlink()

        progress = len(state['completed_tables']) + 1
        print(
            f"📄 Dumping table {table_name} "
            f"({progress}/{len(state['tables'])}, attempt {attempt}/{MAX_TABLE_RETRIES})"
        )
        print(f"   Running pg_dump (this may take a while)...")

        command = pg_dump_base_command(connection_params) + [
            "--data-only",
            "--table",
            table_name,
            "--file",
            str(target_path),
        ]
        result = run_command(
            command,
            build_command_env(connection_params),
            show_output=True,  # Show verbose pg_dump output for visibility
        )
        if result.returncode == 0:
            state["completed_tables"].append(table_name)
            state["last_error"] = None
            save_state(backup_dir, state)
            size_mb = target_path.stat().st_size / (1024 * 1024)
            print(f"✅ Finished {table_name} ({size_mb:.2f} MB)")
            return True

        error_text = (result.stderr or result.stdout or "Unknown pg_dump error").strip()
        state["last_error"] = {
            "table": table_name,
            "attempt": attempt,
            "message": error_text,
            "updated_at": datetime.now().isoformat(),
        }
        save_state(backup_dir, state)

        print(f"⚠️  Failed table {table_name} on attempt {attempt}/{MAX_TABLE_RETRIES}")
        print(error_text)
        if attempt < MAX_TABLE_RETRIES:
            print(f"   Retrying in {RETRY_DELAY_SECONDS} seconds...\n")
            time.sleep(RETRY_DELAY_SECONDS)

    return False


def backup_tables(backup_dir, connection_params, state):
    total_tables = len(state["tables"])
    completed_count = len(state["completed_tables"])
    print(f"📚 Tables completed: {completed_count}/{total_tables}")

    for table_entry in state["tables"]:
        if table_entry["name"] in state["completed_tables"]:
            continue
        success = dump_table_with_retries(backup_dir, connection_params, table_entry, state)
        if not success:
            fail(
                "Automatic retry limit reached. Rerun the script and it will continue "
                f"from {table_entry['name']}."
            )


def mark_completed(backup_dir, state):
    state["status"] = "completed"
    state["completed_at"] = datetime.now().isoformat()
    state["last_error"] = None
    save_state(backup_dir, state)


def print_restore_instructions(backup_dir):
    print("\n" + "=" * 72)
    print("Restore Order For Neon")
    print("=" * 72)
    print("1. Restore pre-data schema")
    print(
        f"   pg_restore --host <neon-host> --port 5432 --username <neon-user> --dbname <neon-db> --clean --if-exists \"{backup_dir / PRE_DATA_FILENAME}\""
    )
    print("2. Restore all table dumps from the tables directory")
    print(
        f"   PowerShell: Get-ChildItem \"{backup_dir / TABLES_DIRNAME}\" -Filter *.dump | Sort-Object Name | ForEach-Object {{ pg_restore --host <neon-host> --port 5432 --username <neon-user> --dbname <neon-db> $_.FullName }}"
    )
    if (backup_dir / SEQUENCES_FILENAME).exists():
        print("3. Restore sequence values")
        print(
            f"   pg_restore --host <neon-host> --port 5432 --username <neon-user> --dbname <neon-db> \"{backup_dir / SEQUENCES_FILENAME}\""
        )
        print("4. Restore post-data schema")
    else:
        print("3. Restore post-data schema")
    print(
        f"   pg_restore --host <neon-host> --port 5432 --username <neon-user> --dbname <neon-db> \"{backup_dir / POST_DATA_FILENAME}\""
    )
    print("=" * 72)


def start_or_resume_backup(connection_params):
    backup_dir, state = find_resumable_backup_dir(connection_params["database"])
    if backup_dir and state:
        print(f"🔁 Resuming existing backup: {backup_dir.name}")
        print(
            f"   Progress: {len(state['completed_tables'])}/{len(state['tables'])} tables completed\n"
        )
        return backup_dir, state

    backup_dir = create_backup_dir(connection_params["database"])
    tables = list_user_tables(connection_params)
    print(f"🆕 Starting new resumable backup: {backup_dir.name}")
    print(f"   Tables to dump: {len(tables)}\n")
    state = initialize_state(backup_dir, connection_params, tables)
    return backup_dir, state


def main():
    print("=" * 72)
    print("Resumable PostgreSQL Backup Tool")
    print("=" * 72)

    check_required_tools()
    database_url = load_env_variables()
    connection_params = parse_database_url(database_url)
    warn_about_legacy_partial_archives()
    test_connection(connection_params)

    backup_dir, state = start_or_resume_backup(connection_params)
    ensure_pre_data_dump(backup_dir, connection_params)
    backup_tables(backup_dir, connection_params, state)
    ensure_sequences_dump(backup_dir, connection_params, list_sequences(connection_params))
    ensure_post_data_dump(backup_dir, connection_params)
    mark_completed(backup_dir, state)

    print(f"\n✅ Backup completed successfully in {backup_dir}")
    print_restore_instructions(backup_dir)


if __name__ == "__main__":
    main()
