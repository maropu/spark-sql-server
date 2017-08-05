#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(pgproto, 0.1, [ishii@sraoss.co.jp])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/main.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC

dnl Check compiler option
dnl check -Wall option
OLD_CFLAGS=$CFLAGS
CFLAGS="$CFLAGS -Wall"
AC_MSG_CHECKING(for -Wall option)
AC_CACHE_VAL(ac_cv_wall,
  AC_TRY_COMPILE([], [char a;], ac_cv_wall=yes, ac_cv_wall=no))
echo $ac_cv_wall
if test $ac_cv_wall = no; then
   CFLAGS=$OLD_CFLAGS
fi

dnl check -Wmissing-prototypes
OLD_CFLAGS=$CFLAGS
CFLAGS="$CFLAGS -Wmissing-prototypes"
AC_MSG_CHECKING(for -Wmissing-prototypes option)
AC_CACHE_VAL(ac_cv_wmissing_prototypes,
  AC_TRY_COMPILE([], [char a;], ac_cv_wmissing_prototypes=yes,
  ac_cv_wmissing_prototypes=no))
echo $ac_cv_wmissing_prototypes
if test $ac_cv_wmissing_prototypes = no; then
   CFLAGS=$OLD_CFLAGS
fi

dnl check -Wmissing-declarations
OLD_CFLAGS=$CFLAGS
CFLAGS="$CFLAGS -Wmissing-declarations"
AC_MSG_CHECKING(for -Wmissing-declarations option)
AC_CACHE_VAL(ac_cv_wmissing_declarations,
  AC_TRY_COMPILE([], [char a;], ac_cv_wmissing_declarations=yes,
  ac_cv_wmissing_declarations=no))
echo $ac_cv_wmissing_declarations
if test $ac_cv_wmissing_declarations = no; then
   CFLAGS=$OLD_CFLAGS
fi

# Checks for libraries.

dnl Checks for pg_config command.
AC_CHECK_PROGS(PGCONFIG, pg_config)

if test -z $PGCONFIG
then
  PGSQL_INCLUDE_DIR=/usr/local/pgsql/include
  PGSQL_LIB_DIR=/usr/local/pgsql/lib
else
  PGSQL_INCLUDE_DIR=`pg_config --includedir`
  PGSQL_LIB_DIR=`pg_config --libdir`
fi

AC_ARG_WITH(pgsql,
    [  --with-pgsql=DIR     site header files for PostgreSQL in DIR],
    [
	case "$withval" in
	"" | y | ye | yes | n | no)
	    AC_MSG_ERROR([*** You must supply an argument to the --with-pgsql option.])
	  ;;
	esac
	PGSQL_INCLUDE_DIR="$withval"/include
	PGSQL_LIB_DIR="$withval"/lib
    ])

AC_ARG_WITH(pgsql-includedir,
    [  --with-pgsql-includedir=DIR     site header files for PostgreSQL in DIR],
    [
	case "$withval" in
	"" | y | ye | yes | n | no)
	    AC_MSG_ERROR([*** You must supply an argument to the --with-pgsql-includedir option.])
	  ;;
	esac
	PGSQL_INCLUDE_DIR="$withval"
    ])

AC_ARG_WITH(pgsql-libdir,
    [  --with-pgsql-libdir=DIR     site library files for PostgreSQL in DIR],
    [
	case "$withval" in
	"" | y | ye | yes | n | no)
	    AC_MSG_ERROR([*** You must supply an argument to the --with-pgsql-libdir option.])
	  ;;
	esac
	PGSQL_LIB_DIR="$withval"
    ])

OLD_LDFLAGS="$LDFLAGS"
LDFLAGS="-L$PGSQL_LIB_DIR"
OLD_LIBS="$LIBS"
AC_CHECK_LIB(pq, PQexecPrepared, [], [AC_MSG_ERROR(libpq is not installed or libpq is old)])
AC_CHECK_FUNCS(PQprepare)
LDFLAGS="$OLD_LDFLAGS"
LIBS="$OLD_LIBS"

AC_SUBST(PGSQL_INCLUDE_DIR)
AC_SUBST(PGSQL_LIB_DIR)

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([Makefile
                 src/Makefile])
AC_OUTPUT
