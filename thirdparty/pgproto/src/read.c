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
 *
 * Functions to read packets from connection to PostgreSQL.
 */

#include "../config.h"
#include "pgproto.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include "fe_memutils.h"
#include <libpq-fe.h>
#include "read.h"

static char read_char(PGconn *conn);
static int read_int32(PGconn *conn);
static int read_int16(PGconn *conn);
static char *read_bytes(int len, PGconn *conn);
static void read_and_discard(PGconn *conn);

/*
 * Read message from connection until ready for query message is received.
 */
void read_until_ready_for_query(PGconn *conn)
{
	int kind;
	int len;
	char *buf;
	char c;
	char *p;

	int cont = 1;

	while (cont)
	{
		kind = read_char(conn);
		switch(kind)
		{
			case '1':	/* Parse Complete */
				fprintf(stderr, "<= BE ParseComplete\n");
				read_and_discard(conn);
				break;

			case '2':	/* Bind Complete */
				fprintf(stderr, "<= BE BindComplete\n");
				read_and_discard(conn);
				break;

			case '3':	/* Close Complete */
				fprintf(stderr, "<= BE CloseComplete\n");
				read_and_discard(conn);
				break;

			case 'n':	/* No data */
				fprintf(stderr, "<= BE NoData\n");
				read_and_discard(conn);
				break;

			case 'T':	/* Row Description */
				fprintf(stderr, "<= BE RowDescription\n");
				read_and_discard(conn);
				break;

			case 'D':	/* Data Row */
				fprintf(stderr, "<= BE DataRow\n");
				read_and_discard(conn);
				break;

			case 'C':	/* Command Complete */
				len = read_int32(conn);
				buf = read_bytes(len - sizeof(int), conn);
				fprintf(stderr, "<= BE CommandComplete(%s)\n", buf);
				pg_free(buf);
				break;

			case 'E':	/* Error Response */
			case 'N':	/* Notice Response */
				if (kind == 'E')
					fprintf(stderr, "<= BE ErrorResponse(");
				else
					fprintf(stderr, "<= BE NoticeResponse(");
				len = read_int32(conn);
				p = buf = read_bytes(len - sizeof(int), conn);
				while (*p)
				{
					fprintf(stderr, "%c ", *p);
					p++;
					fprintf(stderr, "%s ", p);
					p += strlen(p)+1;
				}

				fprintf(stderr, ")\n");

				pg_free(buf);
				break;

			case 'Z':	/* Ready for Query */
				len = read_int32(conn);
				c = read_char(conn);
				fprintf(stderr, "<= BE ReadyForQuery(%c)\n", c);
				cont = 0;
				break;

			case 't':	/* Parameter description */
				fprintf(stderr, "<= BE ParameterDescription\n");
				read_and_discard(conn);
				break;
				
			default:
				fprintf(stderr, "<= BE (%c)\n", kind);
				read_and_discard(conn);
				break;
		}
	}
}

/*
 * Read a character from connection.
 */
static char read_char(PGconn *conn)
{
	char c;
	int sts;

	sts = read(PQsocket(conn), &c, sizeof(c));

	if (sts == 0)
	{
		fprintf(stderr, "read_char: EOF detected");
		exit(1);
	}

	else if (sts < 0)
	{
		fprintf(stderr, "read_char: read(2) returns error (%s)\n", strerror(errno));
		exit(1);
	}

	return c;
}

/*
 * Read an integer from connection.
 */
static int read_int32(PGconn *conn)
{
	int len;
	int sts;

	sts = read(PQsocket(conn), &len, sizeof(len));

	if (sts == 0)
	{
		fprintf(stderr, "read_int32: EOF detected");
		exit(1);
	}

	else if (sts < 0)
	{
		fprintf(stderr, "read_int32: read(2) returns error\n");
		exit(1);
	}

	return ntohl(len);
}

/*
 * Read a short integer from connection.
 */
static int read_int16(PGconn *conn)
{
	short len;
	int sts;

	sts = read(PQsocket(conn), &len, sizeof(len));

	if (sts == 0)
	{
		fprintf(stderr, "read_int16: EOF detected");
		exit(1);
	}

	else if (sts < 0)
	{
		fprintf(stderr, "read_int16: read(2) returns error\n");
		exit(1);
	}

	return ntohs(len);
}

/*
 * Read specified length of bytes from connection.
 * pg_malloc'ed buffer is returned.
 */
static char *read_bytes(int len, PGconn *conn)
{
	int sts;
	char *buf;

	buf = pg_malloc(len);

	sts = read(PQsocket(conn), buf, len);

	if (sts == 0)
	{
		fprintf(stderr, "read_bytes: EOF detected");
		exit(1);
	}

	else if (sts < 0)
	{
		fprintf(stderr, "read_bytes: read(2) returns error\n");
		exit(1);
	}

	return buf;
}

/*
 * Read and discard a packet.
 */
static void read_and_discard(PGconn *conn)
{
	int len;
	char *buf;

	len = read_int32(conn);

	if (len > sizeof(int))
	{
		buf = read_bytes(len - sizeof(int), conn);
		pg_free(buf);
	}
}
