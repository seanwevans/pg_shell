import subprocess
import uuid
import psycopg2

from workers.cleanup_agent import cleanup_once


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS commands")
        cur.execute("DROP TABLE IF EXISTS environments")
        cur.execute("DROP TABLE IF EXISTS users")
        cur.execute("CREATE TABLE users (id uuid PRIMARY KEY)")
        cur.execute(
            "CREATE TABLE environments (user_id uuid PRIMARY KEY, cwd text NOT NULL, env jsonb NOT NULL, updated_at timestamp NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE commands (id serial PRIMARY KEY, user_id uuid, command text, submitted_at timestamp, status text)"
        )
    conn.commit()


def test_cleanup_once_resets_env():
    subprocess.run(["service", "postgresql", "start"], check=False)
    subprocess.run(
        ["sudo", "-u", "postgres", "createdb", "-O", "root", "pgshell_test"],
        check=False,
    )
    conn = psycopg2.connect(dbname="pgshell_test", user="root")
    init_db(conn)
    uid_str = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (id) VALUES (%s)", (uid_str,))
        cur.execute(
            "INSERT INTO environments (user_id, cwd, env, updated_at) VALUES (%s, '/tmp', %s::jsonb, now() - interval '100 days')",
            (uid_str, '{"k":1}'),
        )
        cur.execute(
            "INSERT INTO commands (user_id, command, submitted_at, status) VALUES (%s, 'ls', now() - interval '100 days', 'done')",
            (uid_str,),
        )
    conn.commit()

    cleanup_once(conn, 90)

    with conn.cursor() as cur:
        cur.execute("SELECT cwd, env FROM environments WHERE user_id = %s", (uid_str,))
        cwd, env = cur.fetchone()
    conn.close()
    assert cwd == '/home/sandbox'
    assert env == {}
