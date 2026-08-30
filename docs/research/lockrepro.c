/* Reproduces the sequences.c read-cursor + UPDATE scenario on the vendored SQLite. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include "sqlite3.h"

static const char *DB = "/tmp/lockrepro/t.db";

static void die(const char *what, sqlite3 *db)
{
	fprintf(stderr, "FATAL %s: %s\n", what, db ? sqlite3_errmsg(db) : "");
	exit(1);
}

/* child: mimics `pgcopydb list progress` / `stream sentinel get` */
static void child(int do_write)
{
	sqlite3 *db = NULL;
	if (sqlite3_open(DB, &db) != SQLITE_OK) die("child open", db);
	if (do_write) {
		char *err = NULL;
		int rc = sqlite3_exec(db, "insert into command_log(cmdline) values('list progress')", NULL, NULL, &err);
		printf("  [child] insert rc=%d (%s)\n", rc, rc ? sqlite3_errmsg(db) : "ok");
	} else {
		sqlite3_stmt *st = NULL;
		sqlite3_prepare_v2(db, "select count(*) from s_seq", -1, &st, NULL);
		int rc = sqlite3_step(st);
		printf("  [child] pure read rc=%d\n", rc);
		sqlite3_finalize(st);
	}
	sqlite3_close(db);
	_exit(0);
}

static void run_case(const char *label, int spawn_child, int child_writes)
{
	printf("\n=== %s ===\n", label);
	system("rm -rf /tmp/lockrepro && mkdir -p /tmp/lockrepro");

	sqlite3 *db = NULL;
	char *err = NULL;
	if (sqlite3_open(DB, &db) != SQLITE_OK) die("open", db);
	if (sqlite3_exec(db, "PRAGMA journal_mode = WAL", NULL, NULL, &err)) die("wal", db);
	if (sqlite3_exec(db,
		"create table s_seq(nspname text, relname text, last_value integer, isCalled integer);"
		"create table command_log(cmdline text);"
		"insert into s_seq values('public','a',1,1),('public','b',1,1),"
		"('public','c',1,1),('public','d',1,1),('public','invoice_number_seq',1,1);",
		NULL, NULL, &err)) die("schema", db);

	/* catalog_iter_s_seq: open read cursor on s_seq, same connection */
	sqlite3_stmt *iter = NULL;
	if (sqlite3_prepare_v2(db, "select nspname, relname from s_seq order by nspname, relname",
						   -1, &iter, NULL) != SQLITE_OK) die("prepare iter", db);

	int n = 0;
	while (sqlite3_step(iter) == SQLITE_ROW) {
		const char *nsp = (const char *) sqlite3_column_text(iter, 0);
		const char *rel = (const char *) sqlite3_column_text(iter, 1);
		++n;

		/* after the 2nd row, let the "operator exec" run, as it would mid-loop */
		if (n == 2 && spawn_child) {
			pid_t p = fork();
			if (p == 0) child(child_writes);
			int st; waitpid(p, &st, 0);
		}

		/* catalog_update_sequence_values on the SAME connection */
		sqlite3_stmt *upd = NULL;
		sqlite3_prepare_v2(db, "update s_seq set last_value=$1, isCalled=$2 "
								"where nspname=$3 and relname=$4", -1, &upd, NULL);
		sqlite3_bind_int64(upd, 1, 42);
		sqlite3_bind_int64(upd, 2, 1);
		sqlite3_bind_text(upd, 3, nsp, -1, SQLITE_TRANSIENT);
		sqlite3_bind_text(upd, 4, rel, -1, SQLITE_TRANSIENT);

		int rc = sqlite3_step(upd);
		int ext = sqlite3_extended_errcode(db);
		printf("  update %s.%s -> rc=%d (%s) extended=%d (%s)\n",
			   nsp, rel, rc, sqlite3_errstr(rc), ext, sqlite3_errstr(ext));

		if (rc != SQLITE_DONE) {
			/* mimic catalog_sql_step: retry the same stmt, read txn still open */
			for (int i = 0; i < 4; i++) {
				int r2 = sqlite3_step(upd);
				printf("    retry %d -> rc=%d extended=%d\n",
					   i + 1, r2, sqlite3_extended_errcode(db));
				if (r2 == SQLITE_DONE) break;
			}
		}
		sqlite3_finalize(upd);
	}
	sqlite3_finalize(iter);
	sqlite3_close(db);
}

int main(void)
{
	printf("sqlite %s\n", sqlite3_libversion());
	run_case("A: no external process (control)", 0, 0);
	run_case("B: external PURE READER mid-loop", 1, 0);
	run_case("C: external WRITER mid-loop (one INSERT, like catalog_log_command)", 1, 1);
	return 0;
}
