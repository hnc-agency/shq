-module(shq_SUITE).

-import(ct_helper, [doc/1]).

-export([all/0, groups/0]).
-export([out_wait/1, out_r_wait/1]).
-export([in_wait_0/1, in_r_wait_0/1, in_wait_1/1, in_r_wait_1/1]).

all() ->
	[{group, out_wait}, {group, in_wait}].

groups() ->
	[
		{
			out_wait,
			[],
			[
				out_wait,
				out_r_wait
			]
		},
		{
			in_wait,
			[],
			[
				in_wait_0,
				in_r_wait_0,
				in_wait_1,
				in_r_wait_1
			]
		}
	].

out_wait(_) ->
	doc("Ensure that waiting out works."),
	do_out_wait(out).

out_r_wait(_) ->
	doc("Ensure that waiting out_r works."),
	do_out_wait(out_r).

do_out_wait(Op) ->
	{ok, Pid}=shq:start_link(#{}),
	timer:apply_after(1000, shq, in, [Pid, test]),
	empty=shq:Op(Pid),
	empty=shq:Op(Pid, 100),
	{ok, test}=shq:Op(Pid, 2000),
	ok=shq:stop(Pid),
	ok.

in_wait_0(_) ->
	doc("Ensure that waiting in works with max=0."),
	do_in_wait(in, 0).

in_r_wait_0(_) ->
	doc("Ensure that waiting in_r works with max=0."),
	do_in_wait(in_r, 0).

in_wait_1(_) ->
	doc("Ensure that waiting in works with max=1."),
	do_in_wait(in, 1).

in_r_wait_1(_) ->
	doc("Ensure that waiting in_r works with max=1."),
	do_in_wait(in_r, 1).

do_in_wait(Op, Max) ->
	{ok, Pid}=shq:start_link(#{max => Max}),
	_=[ok=shq:in(Pid, N) || N <- lists:seq(1, Max)],
	timer:apply_after(1000, shq, out, [Pid]),
	full=shq:Op(Pid, test),
	full=shq:Op(Pid, test, 100),
	ok=shq:Op(Pid, test, 2000),
	ok=shq:stop(Pid),
	ok.
