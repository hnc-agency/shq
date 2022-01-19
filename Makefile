PROJECT = shq
PROJECT_DESCRIPTION = Simple Shared Queue
PROJECT_VERSION = 0.0.2

TEST_DEPS = proper ct_helper
dep_ct_helper = git https://github.com/ninenines/ct_helper master

include erlang.mk
