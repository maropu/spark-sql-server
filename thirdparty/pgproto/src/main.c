/*
 * Copyright (c) 2017	Tatsuo Ishii
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 */

#include "../config.h"
#include "pgproto.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include "fe_memutils.h"
#include <libpq-fe.h>
#include "read.h"
#include "send.h"
#include "buffer.h"
#include "extended_query.h"

#undef DEBUG

static void show_version(void);
static void usage(void);
static FILE *openfile(char *filename);
static PGconn *connect_db(char *host, char *port, char *user, char *database);
static void read_and_process(FILE *fd, PGconn *conn);
static int process_a_line(char *buf, PGconn *con);
static int process_message_type(int kind, char *buf, PGconn *conn);

int main(int argc, char **argv)
{
	int opt;
	int optindex;
	char *env;
	char *host = "";
	char *port = "5432";
	char *user = "";
	char *database = "";
	char *data_file = PGPROTODATA;
	int debug = 0;
	FILE *fd;
	PGconn *con;
	int var;

	static struct option long_options[] = {
		{"host", optional_argument, NULL, 'h'},
		{"port", optional_argument, NULL, 'p'},
		{"username", optional_argument, NULL, 'u'},
		{"database", optional_argument, NULL, 'd'},
		{"proto-data-file", optional_argument, NULL, 'f'},
		{"debug", no_argument, NULL, 'D'},
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	if ((env = getenv("PGHOST")) != NULL && *env != '\0')
		host = env;
	if ((env = getenv("PGPORT")) != NULL && *env != '\0')
		port = env;
	if ((env = getenv("PGDATABASE")) != NULL && *env != '\0')
		database = env;
	if ((env = getenv("PGUSER")) != NULL && *env != '\0')
		user = env;

	while ((opt = getopt_long(argc, argv, "v?Dh:p:u:d:f:", long_options, &optindex)) != -1)
	{
		switch (opt)
		{
			case 'v':
				show_version();
				exit(0);
				break;

			case '?':
				usage();
				exit(0);
				break;

			case 'D':
				debug++;
				break;

			case 'h':
				host = pg_strdup(optarg);
				break;

			case 'p':
				port = pg_strdup(optarg);
				break;

			case 'u':
				user = pg_strdup(optarg);
				break;

			case 'd':
				database = pg_strdup(optarg);
				break;

			case 'f':
				data_file = pg_strdup(optarg);
				break;

			default:
				usage();
				exit(1);
		}
	}

	fd = openfile(data_file);
	con = connect_db(host, port, user, database);
	var = fcntl(PQsocket(con), F_GETFL, 0);
	if (var == -1)
	{
		fprintf(stderr, "fcntl failed (%s)\n", strerror(errno));
		exit(1);
	}

	if (fcntl(PQsocket(con), F_SETFL, var & ~O_NONBLOCK) == -1)
	{
		fprintf(stderr, "fcntl failed (%s)\n", strerror(errno));
		exit(1);
	}

	read_and_process(fd, con);

	return 0;
}

static void show_version(void)
{
	fprintf(stderr, "%s version %s\n",	PACKAGE, PGPROTO_VERSION);
}

static void usage(void)
{
	printf("Usage: %s\n"
		   "-h, --hostname=HOSTNAME (default: UNIX domain socket)\n"
		   "-p, --port=PORT (default: 5432)\n"
		   "-u, --user USERNAME (default: OS user)\n"
		   "-d, --database DATABASENAME (default: same as user)\n"
		   "-f, --proto-data-file FILENAME (default: pgproto.data)\n"
		   "-D, --debug\n"
		   "-?, --help\n"
		   "-v, --version\n",
		PACKAGE);
}

/*
 * Open protocol data and return the file descriptor.  If failed to open the
 * file, do not return and exit within this function.
 */
static FILE *openfile(char *filename)
{
	FILE *fd = fopen(filename,"r");

	if (fd == NULL)
	{
		fprintf(stderr, "Failed to open protocol data file: %s.\n", filename);
		exit(1);
	}
	return fd;
}

/*
 * Connect to the specifed PostgreSQL. If failed, do not return and exit
 * within this function.
 */
static PGconn *connect_db(char *host, char *port, char *user, char *database)
{
	char conninfo[1024];
	PGconn *conn;

	conninfo[0] = '\0';

	if (host && host[0] != '\0')
	{
		strcat(conninfo, "host=");
		strcat(conninfo, host);
	}

	if (port && port[0] != '\0')
	{
		strcat(conninfo, " port=");
		strcat(conninfo, port);
	}

	if (user && user[0] != '\0')
	{
		strcat(conninfo, " user=");
		strcat(conninfo, user);
	}

	if (database && database[0] != '\0')
	{
		strcat(conninfo, " dbname=");
		strcat(conninfo, database);
	}

	conn = PQconnectdb(conninfo);

	if (conn == NULL || PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "Failed to connect to %s.\n", conninfo);
		exit(1);
	}

	return conn;
}

/*
 * Read the protocol data file and process it.
 */
static void read_and_process(FILE *fd, PGconn *conn)
{
	int status;
	char buf[4096];
	char *p;

	for (;;)
	{
		p = fgets(buf, sizeof(buf), fd);
		if (p == NULL)
			break;

		status = process_a_line(buf, conn);

		if (status < 0)
		{
			exit(1);
		}
	}
	PQfinish(conn);
}

/*
 * Process a line of protocol data.
 */
static int process_a_line(char *buf, PGconn *conn)
{
	char *p = buf;
	int kind;

#ifdef DEBUG
	fprintf(stderr, "buf: %s", buf);
#endif

	if (p == NULL)
	{
		fprintf(stderr, "process_a_line: buf is NULL\n");
		return -1;
	}

	/* An empty line or a line starting with '#' is ignored. */
	if (*p == '\n' || *p == '#')
	{
		return 0;
	}
	else if (*p != '\'')
	{
		fprintf(stderr, "process_a_line: first character is not \' but %x\n", *p);
		return -1;
	}

	p++;

	kind = (unsigned char)(*p++);

	if (*p != '\'')
	{
		fprintf(stderr, "process_a_line: message kind is not followed by \' but %x\n", *p);
		return -1;
	}

	p++;

	process_message_type(kind, p, conn);

	return 0;
}

/*
 * Read a line of message from buf and process it.
 */
static int process_message_type(int kind, char *buf, PGconn *conn)
{
#ifdef DEBUG
	fprintf(stderr, "message kind is %c\n", kind);
#endif

	char *query;
	char *bufp;

	switch(kind)
	{
		case 'Y':
			read_until_ready_for_query(conn);
			break;

		case 'X':
			fprintf(stderr, "FE=> Terminate\n");
			send_char((char)kind, conn);
			send_int(sizeof(int), conn);
			break;

		case 'S':
			fprintf(stderr, "FE=> Sync\n");
			send_char((char)kind, conn);
			send_int(sizeof(int), conn);
			break;

		case 'H':
			fprintf(stderr, "FE=> Flush\n");
			send_char((char)kind, conn);
			send_int(sizeof(int), conn);
			break;

		case 'Q':
			buf++;
			SKIP_TABS(buf);
			query = buffer_read_string(buf, &bufp);
			fprintf(stderr, "FE=> Query(query=\"%s\")\n", query);
			send_char((char)kind, conn);
			send_int(sizeof(int)+strlen(query)+1, conn);
			send_string(query, conn);
			pg_free(query);
			break;

		case 'P':
			process_parse(buf, conn);
			break;

		case 'B':
			process_bind(buf, conn);
			break;

		case 'E':
			process_execute(buf, conn);
			break;

		case 'D':
			process_describe(buf, conn);
			break;

		case 'C':
			process_close(buf, conn);
			break;

		default:
			break;
	}
	return 0;
}
