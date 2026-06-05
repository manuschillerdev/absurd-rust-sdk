# Test suites

## Database integration tests

The Rust database integration suite is ignored during normal `cargo test` runs because it needs a Postgres database initialized with Absurd SQL.

1. Start Postgres 16 or newer and create a test database.
2. Install upstream Absurd SQL tag `0.4.0` into that database:

   ```sh
   psql "$ABSURD_DATABASE_URL" -v ON_ERROR_STOP=1 -f /path/to/absurd/sql/absurd.sql
   ```

3. Run the ignored database tests:

   ```sh
   ABSURD_DATABASE_URL=postgresql://localhost/absurd_test cargo test-db
   ```

CI follows the same path with Postgres 16 and upstream Absurd SQL tag `0.4.0`.
