MODULE_big = gsheets

OBJS = gsheets.o \
	   utils/http_helpers.o

EXTENSION = gsheets
DATA = gsheets--0.1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

SHLIB_LINK = -lcurl