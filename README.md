# Database Backup Script - Supabase to Neon Migration

This project provides scripts to create a resumable PostgreSQL backup from Supabase and restore it to Neon.

## Scripts

- `backup_db.py`: Creates a resumable backup directory format.
- `restore_neon.py`: Restores backup directory contents to Neon in dependency-safe order.

## Prerequisites

### 1. PostgreSQL Client Tools

You need `pg_dump`, `pg_restore`, and `psql` installed on your system.

- Windows: Download from [postgresql.org](https://www.postgresql.org/download/windows/)
    - During installation, select **Command Line Tools**.
    - Or install with: `winget install PostgreSQL.PostgreSQL`
- macOS: `brew install postgresql`
- Linux (Ubuntu/Debian): `sudo apt-get install postgresql-client`

Verify installation:

```bash
pg_dump --version
pg_restore --version
psql --version
```

### 2. Python Dependencies

```bash
pip install -r requirements-backup.txt
```

## Environment Variables

Store credentials in `.env`:

```env
DATABASE_DIRECT_URL=postgresql://<supabase-user>:<supabase-password>@<supabase-host>:5432/<supabase-db>?sslmode=require
NEON_DB_URL=postgresql://<neon-user>:<neon-password>@<neon-host>:5432/<neon-db>?sslmode=require
```

## Usage

### Create Backup

```bash
python backup_db.py
```

What backup does:

1. Validates required PostgreSQL tools.
2. Loads `DATABASE_DIRECT_URL`.
3. Tests connection to source DB.
4. Starts or resumes `backup_<database>_<timestamp>`.
5. Dumps `pre_data.dump` once.
6. Dumps each table to `tables/*.dump` with retries.
7. Dumps `sequences.dump`.
8. Dumps `post_data.dump`.

### Restore to Neon

```bash
python restore_neon.py
```

Optional arguments:

- `--backup-dir <path>`: restore a specific backup directory.
- `--restart`: reset restore progress and start from step 1.

What restore does:

1. Validates required tools.
2. Loads `NEON_DB_URL`.
3. Tests Neon connection.
4. Prints source/target preflight summary.
5. Prompts confirmation before pre-data clean operation.
6. Restores in order:
    1. `pre_data.dump`
    2. `tables/*.dump` in filename order
    3. `sequences.dump` (if present)
    4. `post_data.dump`
7. Saves progress in `restore_state.json` for resume.

## Automatic Resume

If backup or restore is interrupted, rerun the same script.

- Backup resume state: `backup_state.json`
- Restore resume state: `restore_state.json`

## Restore Compatibility Notes

`restore_neon.py` includes Neon compatibility filters for known Supabase-only objects (for example, Vault extension objects/tables) so restore can continue without failing on unsupported objects.

## Troubleshooting

### Connection errors

- Verify `.env` values.
- Confirm source/target DBs are reachable.
- Ensure your IP/network access is allowed.

### Missing tool errors

Install PostgreSQL command-line tools and ensure they are on `PATH`.

## Legacy Files

Old `*.tmp.bak` single-archive files are from the previous non-resumable design and can be removed when no longer needed.
