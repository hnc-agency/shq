%% Copyright (c) 2022-2024, Maria Scott <maria-12648430@hnc-agency.org>
%% Copyright (c) 2022-2024, Jan Uhlig <juhlig@hnc-agency.org>
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
-export([start_anonymous/1, start_local/1, start_global/1, start_via/1]).
-export([start_link_anonymous/1, start_link_local/1, start_link_global/1, start_link_via/1]).
-export([start_monitor_anonymous/1, start_monitor_local/1, start_monitor_global/1, start_monitor_via/1]).
-export([out_wait_anonymous/1, out_wait_local/1, out_wait_global/1, out_wait_via/1]).
-export([out_r_wait_anonymous/1, out_r_wait_local/1, out_r_wait_global/1, out_r_wait_via/1]).
-export([in_wait_anonymous_0/1, in_wait_local_0/1, in_wait_global_0/1, in_wait_via_0/1]).
-export([in_wait_anonymous_1/1, in_wait_local_1/1, in_wait_global_1/1, in_wait_via_1/1]).
-export([in_r_wait_anonymous_0/1, in_r_wait_local_0/1, in_r_wait_global_0/1, in_r_wait_via_0/1]).
-export([in_r_wait_anonymous_1/1, in_r_wait_local_1/1, in_r_wait_global_1/1, in_r_wait_via_1/1]).
-export([order_wait_0/1, order_wait_r_0/1, order_wait_1/1, order_wait_r_1/1]).
-export([open_close/1]).

all() ->
	[{group, start}, {group, out_wait}, {group, in_wait}, {group, order}, open_close].

groups() ->
	[
	 	{
			start,
			[],
			[
				{
					non_linked,
					[],
					[
						start_anonymous,
						start_local,
						start_global,
						start_via
					]
				},
				{
					linked,
					[],
					[
						start_link_anonymous,
						start_link_local,
						start_link_global,
						start_link_via
					]
				},
				{
					monitored,
					[],
					[
						start_monitor_anonymous,
						start_monitor_local,
						start_monitor_global,
						start_monitor_via
					]
				}
			]
		},
		{
			out_wait,
			[],
			[
				out_wait_anonymous,
				out_wait_local,
				out_wait_global,
				out_wait_via,
				out_r_wait_anonymous,
				out_r_wait_local,
				out_r_wait_global,
				out_r_wait_via
			]
		},
		{
			in_wait,
			[],
			[
				in_wait_anonymous_0,
				in_wait_local_0,
				in_wait_global_0,
				in_wait_via_0,
				in_wait_anonymous_1,
				in_wait_local_1,
				in_wait_global_1,
				in_wait_via_1,
				in_r_wait_anonymous_0,
				in_r_wait_local_0,
				in_r_wait_global_0,
				in_r_wait_via_0,
				in_r_wait_anonymous_1,
				in_r_wait_local_1,
				in_r_wait_global_1,
				in_r_wait_via_1
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

start_anonymous(_) ->
	doc("Ensure that starting a non-linked anonymous queue works."),
	do_start(undefined).

start_local(_) ->
	doc("Ensure that starting a non-linked locally registered queue works."),
	do_start({local, ?MODULE}).

start_global(_) ->
	doc("Ensure that starting a non-linked globally registered queue works."),
	do_start({global, ?MODULE}).

start_via(_) ->
	doc("Ensure that starting a non-linked via-registered queue works."),
	do_start({via, global, ?MODULE}).

do_start(ServerName) ->
	Self=self(),
	CPid=spawn_link(
		fun () ->
			{Pid, Name}=do_start_shq(ServerName, start, #{}),
			open=shq:status(Name),
			Self ! {self(), Pid, Name},
			ok=receive {Self, ok} -> ok after 1000 -> exit(timeout) end,
			unlink(Self),
			exit(crash)
		end
	),
	CMon=monitor(process, CPid),
	{Pid, Name}=receive {CPid, QPid, QName} -> {QPid, QName} after 1000 -> exit(timeout) end,
	Mon=monitor(process, Pid),
	CPid ! {self(), ok},
	ok=receive {'DOWN', CMon, process, CPid, crash} -> ok after 1000 -> exit(timeout) end,
	ok=receive {'DOWN', Mon, process, Pid, Reason} -> exit({unexpected_exit, Reason}) after 1000 -> ok end,
	demonitor(Mon, [flush]),
	ok=shq:stop(Name),
	ok.

start_link_anonymous(_) ->
	doc("Ensure that starting a linked anonymous queue works."),
	do_start_link(undefined).

start_link_local(_) ->
	doc("Ensure that starting a linked locally registered queue works."),
	do_start_link({local, ?MODULE}).

start_link_global(_) ->
	doc("Ensure that starting a linked globally registered queue works."),
	do_start_link({global, ?MODULE}).

start_link_via(_) ->
	doc("Ensure that starting a linked via-registered queue works."),
	do_start_link({via, global, ?MODULE}).

do_start_link(ServerName) ->
	Self=self(),
	CPid=spawn_link(
		fun () ->
			{Pid, Name}=do_start_shq(ServerName, start_link, #{}),
			open=shq:status(Name),
			Self ! {self(), Pid, Name},
			ok=receive {Self, ok} -> ok after 1000 -> exit(timeout) end,
			unlink(Self),
			exit(crash)
		end
	),
	CMon=monitor(process, CPid),
	{Pid, _Name}=receive {CPid, QPid, QName} -> {QPid, QName} after 1000 -> exit(timeout) end,
	Mon=monitor(process, Pid),
	CPid ! {self(), ok},
	ok=receive {'DOWN', CMon, process, CPid, crash} -> ok after 1000 -> exit(timeout) end,
	ok=receive {'DOWN', Mon, process, Pid, crash} -> ok after 1000 -> exit(timeout) end,
	demonitor(Mon, [flush]),
	ok.

start_monitor_anonymous(_) ->
	doc("Ensure that starting a monitored anonymous queue works."),
	do_start_monitor(undefined).

start_monitor_local(_) ->
	doc("Ensure that starting a monitored locally registered queue works."),
	do_start_monitor({local, ?MODULE}).

start_monitor_global(_) ->
	doc("Ensure that starting a monitored globally registered queue works."),
	do_start_monitor({global, ?MODULE}).

start_monitor_via(_) ->
	doc("Ensure that starting a monitored via-registered queue works."),
	do_start_monitor({via, global, ?MODULE}).

do_start_monitor(ServerName) ->
	{{Pid, Mon}, Name}=do_start_shq(ServerName, start_monitor, #{}),
	open=shq:status(Name),
	exit(Pid, kill),
	receive {'DOWN', Mon, process, Pid, killed} -> ok after 1000 -> exit(timeout) end,
	ok.

do_start_shq(Op, Opts) ->
	case shq:Op(Opts) of
		{ok, {Pid, Mon}} ->
			{{Pid, Mon}, Pid};
		{ok, Pid} ->
			{Pid, Pid}
	end.

do_start_shq(undefined, Op, Opts) ->
	do_start_shq(Op, Opts);
do_start_shq(Name={local, LocalName}, Op, Opts) ->
	{ok, Res}=shq:Op(Name, Opts),
	{Res, LocalName};
do_start_shq(Name={global, _}, Op, Opts) ->
	{ok, Res}=shq:Op(Name, Opts),
	{Res, Name};
do_start_shq(Name={via, _, _}, Op, Opts) ->
	{ok, Res}=shq:Op(Name, Opts),
	{Res, Name}.

out_wait_anonymous(_) ->
	doc("Ensure that waiting out works for an anonymous queue."),
	do_out_wait(undefined, out),
	do_out_wait_exit(undefined, out).

out_wait_local(_) ->
	doc("Ensure that waiting out works for a locally named queue."),
	do_out_wait({local, ?MODULE}, out),
	do_out_wait_exit({local, ?MODULE}, out).

out_wait_global(_) ->
	doc("Ensure that waiting out works for a globally named queue."),
	do_out_wait({global, ?MODULE}, out),
	do_out_wait_exit({global, ?MODULE}, out).

out_wait_via(_) ->
	doc("Ensure that waiting out works for a via-named queue."),
	do_out_wait({via, global, ?MODULE}, out),
	do_out_wait_exit({via, global, ?MODULE}, out).

out_r_wait_anonymous(_) ->
	doc("Ensure that waiting out_r works for an anonymous queue."),
	do_out_wait(undefined, out_r),
	do_out_wait_exit(undefined, out_r).

out_r_wait_local(_) ->
	doc("Ensure that waiting out_r works for a locally named queue."),
	do_out_wait({local, ?MODULE}, out_r),
	do_out_wait_exit({local, ?MODULE}, out_r).

out_r_wait_global(_) ->
	doc("Ensure that waiting out_r works for a globally named queue."),
	do_out_wait({global, ?MODULE}, out_r),
	do_out_wait_exit({global, ?MODULE}, out_r).

out_r_wait_via(_) ->
	doc("Ensure that waiting out_r works for a via-named queue."),
	do_out_wait({via, global, ?MODULE}, out_r),
	do_out_wait_exit({via, global, ?MODULE}, out_r).

do_out_wait(ServerName, Op) ->
	{_Pid, Name}=do_start_shq(ServerName, start_link, #{}),
	timer:apply_after(1000, shq, in, [Name, test]),
	empty=shq:Op(Name),
	empty=shq:Op(Name, 100),
	{ok, test}=shq:Op(Name, 2000),
	ok=shq:stop(Name),
	ok.

do_out_wait_exit(ServerName, Op) ->
	{_Pid, Name}=do_start_shq(ServerName, start_link, #{}),
	{CPid, CMon}=spawn_monitor(fun () -> shq:Op(Name, 1000) end),
	timer:sleep(100),
	exit(CPid, kill),
	receive {'DOWN', CMon, process, CPid, killed} -> ok end,
	timer:sleep(1000),
	ok=shq:in(Name, test),
	{ok, test}=shq:Op(Name),
	ok=shq:stop(Name),
	ok.

in_wait_anonymous_0(_) ->
	doc("Ensure that waiting in works with max=0 for an anonymous queue."),
	do_in_wait(undefined, in, 0),
	do_in_wait_exit(undefined, in, 0).

in_wait_local_0(_) ->
	doc("Ensure that waiting in works with max=0 for a locally named queue."),
	do_in_wait({local, ?MODULE}, in, 0),
	do_in_wait_exit({local, ?MODULE}, in, 0).

in_wait_global_0(_) ->
	doc("Ensure that waiting in works with max=0 for a globally named queue."),
	do_in_wait({global, ?MODULE}, in, 0),
	do_in_wait_exit({global, ?MODULE}, in, 0).

in_wait_via_0(_) ->
	doc("Ensure that waiting in works with max=0 for a via-named queue."),
	do_in_wait({via, global, ?MODULE}, in, 0),
	do_in_wait_exit({via, global, ?MODULE}, in, 0).

in_r_wait_anonymous_0(_) ->
	doc("Ensure that waiting in_r works with max=0 for an anonymous queue."),
	do_in_wait(undefined, in_r, 0),
	do_in_wait_exit(undefined, in_r, 0).

in_r_wait_local_0(_) ->
	doc("Ensure that waiting in_r works with max=0 for a locally named queue."),
	do_in_wait({local, ?MODULE}, in_r, 0),
	do_in_wait_exit({local, ?MODULE}, in_r, 0).

in_r_wait_global_0(_) ->
	doc("Ensure that waiting in_r works with max=0 for a globally named queue."),
	do_in_wait({global, ?MODULE}, in_r, 0),
	do_in_wait_exit({global, ?MODULE}, in_r, 0).

in_r_wait_via_0(_) ->
	doc("Ensure that waiting in_r works with max=0 for a via-named queue."),
	do_in_wait({via, global, ?MODULE}, in_r, 0),
	do_in_wait_exit({via, global, ?MODULE}, in_r, 0).

in_wait_anonymous_1(_) ->
	doc("Ensure that waiting in works with max=1 for an anonymous queue."),
	do_in_wait(undefined, in, 1),
	do_in_wait_exit(undefined, in, 1).

in_wait_local_1(_) ->
	doc("Ensure that waiting in works with max=1 for a locally named queue."),
	do_in_wait({local, ?MODULE}, in, 1),
	do_in_wait_exit({local, ?MODULE}, in, 1).

in_wait_global_1(_) ->
	doc("Ensure that waiting in works with max=1 for a globally named queue."),
	do_in_wait({global, ?MODULE}, in, 1),
	do_in_wait_exit({global, ?MODULE}, in, 1).

in_wait_via_1(_) ->
	doc("Ensure that waiting in works with max=1 for a via-named queue."),
	do_in_wait({via, global, ?MODULE}, in, 1),
	do_in_wait_exit({via, global, ?MODULE}, in, 1).

in_r_wait_anonymous_1(_) ->
	doc("Ensure that waiting in_r works with max=1 for an anonymous queue."),
	do_in_wait(undefined, in_r, 1),
	do_in_wait_exit(undefined, in_r, 1).

in_r_wait_local_1(_) ->
	doc("Ensure that waiting in_r works with max=1 for a locally named queue."),
	do_in_wait({local, ?MODULE}, in_r, 1),
	do_in_wait_exit({local, ?MODULE}, in_r, 1).

in_r_wait_global_1(_) ->
	doc("Ensure that waiting in_r works with max=1 for a globally named queue."),
	do_in_wait({global, ?MODULE}, in_r, 1),
	do_in_wait_exit({global, ?MODULE}, in_r, 1).

in_r_wait_via_1(_) ->
	doc("Ensure that waiting in_r works with max=1 for a via-named queue."),
	do_in_wait({via, global, ?MODULE}, in_r, 1),
	do_in_wait_exit({via, global, ?MODULE}, in_r, 1).

do_in_wait(ServerName, Op, Max) ->
	{_Pid, Name}=do_start_shq(ServerName, start_link, #{max => Max}),
	_=[ok=shq:Op(Name, N) || N <- lists:seq(1, Max)],
	timer:apply_after(1000, shq, out, [Name]),
	full=shq:Op(Name, test),
	full=shq:Op(Name, test, 100),
	ok=shq:Op(Name, test, 2000),
	ok=shq:stop(Name),
	ok.

do_in_wait_exit(ServerName, Op, Max) ->
	{_Pid, Name}=do_start_shq(ServerName, start_link, #{max => Max}),
	_=[ok=shq:Op(Name, N) || N <- lists:seq(1, Max)],
	{CPid, CMon}=spawn_monitor(fun () -> shq:Op(Name, test, 1000) end),
	timer:sleep(100),
	exit(CPid, kill),
	receive {'DOWN', CMon, process, CPid, killed} -> ok end,
	timer:sleep(1000),
	true=lists:all(
		fun (_) -> {ok, I}=shq:out(Name), I=/=test end,
		lists:seq(1, shq:size(Name))
	),
	ok=shq:stop(Name),
	ok.

order_wait_0(_) ->
	doc("Ensure that waiting in are inserted and retrieved in correct order "
	    "with max=0."),
	do_order_wait(in, out, 0).

order_wait_r_0(_) ->
	doc("Ensure that waiting in_r are inserted and retrieved in correct order "
	    "with max=0."),
	do_order_wait(in_r, out_r, 0).

order_wait_1(_) ->
	doc("Ensure that waiting in are inserted and retrieved in correct order "
	    "with max=1."),
	do_order_wait(in, out, 1).

order_wait_r_1(_) ->
	doc("Ensure that waiting in_r are inserted and retrieved in correct order "
	    "with max=1."),
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
	doc("Ensure that items cannot be inserted when a queue is closed."),
	{ok, Pid}=shq:start_link(#{max=>2}),
	open=shq:status(Pid),
	ok=shq:in(Pid, a),
	ok=shq:in_r(Pid, b),
	Self=self(),
	CPid1=spawn_link(fun () -> Self ! {self(), shq:in(Pid, c, infinity)} end),
	CPid2=spawn_link(fun () -> Self ! {self(), shq:in_r(Pid, d, infinity)} end),
	ok=shq:close(Pid),
	closed=receive {CPid1, CReply1} -> CReply1 after 1000 -> exit(timeout) end,
	closed=receive {CPid2, CReply2} -> CReply2 after 1000 -> exit(timeout) end,
	closed=shq:status(Pid),
	closed=shq:in(Pid, e),
	closed=shq:in_r(Pid, f),
	{ok, b}=shq:out(Pid),
	{ok, a}=shq:out_r(Pid),
	empty=shq:out(Pid),
	empty=shq:out_r(Pid),
	ok=shq:open(Pid),
	open=shq:status(Pid),
	ok=shq:in(Pid, g),
	ok=shq:in_r(Pid, h),
	ok=shq:stop(Pid).
