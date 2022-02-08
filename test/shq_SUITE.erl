%% Copyright (c) 2022, Maria Scott <maria-12648430@hnc-agency.org>
%% Copyright (c) 2022, Jan Uhlig <juhlig@hnc-agency.org>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(shq_SUITE).

-import(ct_helper, [doc/1]).

-export([all/0, groups/0]).
-export([out_wait/1, out_r_wait/1]).
-export([in_wait_0/1, in_r_wait_0/1, in_wait_1/1, in_r_wait_1/1]).
-export([order_wait_0/1, order_wait_r_0/1, order_wait_1/1, order_wait_r_1/1]).
-export([open_close/1]).

all() ->
	[{group, out_wait}, {group, in_wait}, {group, order}, open_close].

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
		},
		{
			order,
			[],
			[
				order_wait_0,
				order_wait_r_0,
				order_wait_1,
				order_wait_r_1
			]
		}
	].

out_wait(_) ->
	doc("Ensure that waiting out works."),
	do_out_wait(out),
	do_out_wait_exit(out).

out_r_wait(_) ->
	doc("Ensure that waiting out_r works."),
	do_out_wait(out_r),
	do_out_wait_exit(out_r).

do_out_wait(Op) ->
	{ok, Pid}=shq:start_link(#{}),
	timer:apply_after(1000, shq, in, [Pid, test]),
	empty=shq:Op(Pid),
	empty=shq:Op(Pid, 100),
	{ok, test}=shq:Op(Pid, 2000),
	ok=shq:stop(Pid),
	ok.

do_out_wait_exit(Op) ->
	{ok, Pid}=shq:start_link(#{}),
	{CPid, CMon}=spawn_monitor(fun () -> shq:Op(Pid, 1000) end),
	timer:sleep(100),
	exit(CPid, kill),
	receive {'DOWN', CMon, process, CPid, killed} -> ok end,
	timer:sleep(1000),
	ok=shq:in(Pid, test),
	{ok, test}=shq:Op(Pid),
	ok=shq:stop(Pid),
	ok.

in_wait_0(_) ->
	doc("Ensure that waiting in works with max=0."),
	do_in_wait(in, 0),
	do_in_wait_exit(in, 0).

in_r_wait_0(_) ->
	doc("Ensure that waiting in_r works with max=0."),
	do_in_wait(in_r, 0),
	do_in_wait_exit(in_r, 0).

in_wait_1(_) ->
	doc("Ensure that waiting in works with max=1."),
	do_in_wait(in, 1),
	do_in_wait_exit(in, 1).

in_r_wait_1(_) ->
	doc("Ensure that waiting in_r works with max=1."),
	do_in_wait(in_r, 1),
	do_in_wait_exit(in_r, 1).

do_in_wait(Op, Max) ->
	{ok, Pid}=shq:start_link(#{max => Max}),
	_=[ok=shq:Op(Pid, N) || N <- lists:seq(1, Max)],
	timer:apply_after(1000, shq, out, [Pid]),
	full=shq:Op(Pid, test),
	full=shq:Op(Pid, test, 100),
	ok=shq:Op(Pid, test, 2000),
	ok=shq:stop(Pid),
	ok.

do_in_wait_exit(Op, Max) ->
	{ok, Pid}=shq:start_link(#{max => Max}),
	_=[ok=shq:Op(Pid, N) || N <- lists:seq(1, Max)],
	{CPid, CMon}=spawn_monitor(fun () -> shq:Op(Pid, test, 1000) end),
	timer:sleep(100),
	exit(CPid, kill),
	receive {'DOWN', CMon, process, CPid, killed} -> ok end,
	timer:sleep(1000),
	true=lists:all(
		fun (_) -> {ok, I}=shq:out(Pid), I=/=test end,
		lists:seq(1, shq:size(Pid))
	),
	ok=shq:stop(Pid),
	ok.

order_wait_0(_) ->
	doc("Ensure that waiting in are inserted and retrieved in correct order "
	    "with max=0"),
	do_order_wait(in, out, 0).

order_wait_r_0(_) ->
	doc("Ensure that waiting in_r are inserted and retrieved in correct order "
	    "with max=0"),
	do_order_wait(in_r, out_r, 0).

order_wait_1(_) ->
	doc("Ensure that waiting in are inserted and retrieved in correct order "
	    "with max=1"),
	do_order_wait(in, out, 1).

order_wait_r_1(_) ->
	doc("Ensure that waiting in_r are inserted and retrieved in correct order "
	    "with max=1"),
	do_order_wait(in_r, out_r, 1).

do_order_wait(InOp, OutOp, Max) ->
	{ok, Pid}=shq:start_link(#{max => Max}),
	Items=lists:seq(1, 10),
	_=[begin spawn_link(shq, InOp, [Pid, N, infinity]), timer:sleep(100) end || N <- Items],
	lists:foreach(
		fun (N) ->
			{ok, N}=shq:OutOp(Pid)
		end,
		Items
	),
	ok=shq:stop(Pid),
	ok.

open_close(_) ->
	doc(""),
	{ok, Pid}=shq:start_link(#{}),
	open=shq:status(Pid),
	ok=shq:in(Pid, a),
	ok=shq:in_r(Pid, b),
	ok=shq:close(Pid),
	closed=shq:status(Pid),
	closed=shq:in(Pid, c),
	closed=shq:in_r(Pid, d),
	{ok, b}=shq:out(Pid),
	{ok, a}=shq:out_r(Pid),
	empty=shq:out(Pid),
	empty=shq:out_r(Pid),
	ok=shq:open(Pid),
	open=shq:status(Pid),
	ok=shq:in(Pid, c),
	ok=shq:in_r(Pid, d),
	ok=shq:stop(Pid).
