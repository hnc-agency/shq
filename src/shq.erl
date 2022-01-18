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

-module(shq).

-behavior(gen_server).

-export([start/1, start/2]).
-export([start_link/1, start_link/2]).
-export([start_monitor/1, start_monitor/2]).
-export([stop/1]).
-export([in/2, in/3, in_r/2, in_r/3]).
-export([out/1, out/2, out_r/1, out_r/2]).
-export([peek/1, peek_r/1]).
-export([size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state,
	{
		tab :: ets:tid(),
		front=0 :: integer(),
		rear=0 :: integer(),
		max=infinity :: non_neg_integer() | 'infinity',
		monitors=#{} :: #{},
		waiting_in=queue:new() :: queue:queue(),
		waiting_out=queue:new() :: queue:queue()
	}
).

-type server_name() :: {'local', Name :: atom()}
		     | {'global', GlobalName :: term()}
		     | {'via', Via :: module(), ViaName :: term()}.

-type server_ref() :: pid()
		    | Name :: atom()
		    | {Name :: atom(), Node :: node()}
		    | {'global', GlobalName :: term()}
		    | {'via', Via :: module(), ViaName :: term()}.

-type opts() :: opts_map() | opts_list().

-type opts_map() :: #{
			'max' => non_neg_integer() | 'infinity'
		     }.

-type opts_list() :: [
			{'max', non_neg_integer() | 'infinity'}
		     ].

-spec start(Opts :: opts()) -> {'ok', pid()}.
start(Opts) when is_map(Opts) ->
	gen_server:start(?MODULE, verify_opts(Opts), []);
start(Opts) when is_list(Opts) ->
	start(proplists:to_map(Opts)).

-spec start(ServerName :: server_name(), Opts :: opts()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start(ServerName, Opts) when is_map(Opts) ->
	gen_server:start(ServerName, ?MODULE, verify_opts(Opts), []);
start(ServerName, Opts) when is_list(Opts) ->
	start(ServerName, proplists:to_map(Opts)).

-spec start_link(Opts :: opts()) -> {'ok', pid()}.
start_link(Opts) when is_map(Opts) ->
	gen_server:start_link(?MODULE, verify_opts(Opts), []);
start_link(Opts) when is_list(Opts) ->
	start_link(proplists:to_map(Opts)).

-spec start_link(ServerName :: server_name(), Opts :: opts()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start_link(ServerName, Opts) when is_map(Opts) ->
	gen_server:start_link(ServerName, ?MODULE, verify_opts(Opts), []);
start_link(ServerName, Opts) when is_list(Opts) ->
	start_link(ServerName, proplists:to_map(Opts)).

-spec start_monitor(Opts :: opts()) -> {'ok', {pid(), reference()}}.
start_monitor(Opts) when is_map(Opts) ->
	gen_server:start_monitor(?MODULE, verify_opts(Opts), []);
start_monitor(Opts) when is_list(Opts) ->
	start_monitor(proplists:to_map(Opts)).

-spec start_monitor(ServerName :: server_name(), Opts :: opts()) -> {'ok', {pid(), reference()}} | {'error', Reason :: 'already_started'}.
start_monitor(ServerName, Opts) when is_map(Opts) ->
	gen_server:start_monitor(ServerName, ?MODULE, verify_opts(Opts), []);
start_monitor(ServerName, Opts) when is_list(Opts) ->
	start_monitor(ServerName, proplists:to_map(Opts)).

-spec stop(ServerRef :: server_ref()) -> 'ok'.
stop(ServerRef) ->
	gen_server:stop(ServerRef).

-spec in(ServerRef :: server_ref(), Value :: term()) -> 'ok' | 'full'.
in(ServerRef, Value) ->
	in(ServerRef, Value, 0).

-spec in(ServerRef :: server_ref(), Value :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in(ServerRef, Value, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(rear, Value, ServerRef, Timeout).

-spec in_r(ServerRef :: server_ref(), Value :: term()) -> 'ok' | 'full'.
in_r(ServerRef, Value) ->
	in_r(ServerRef, Value, 0).

-spec in_r(ServerRef :: server_ref(), Value :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in_r(ServerRef, Value, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(front, Value, ServerRef, Timeout).

-spec out(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef) ->
	out(ServerRef, 0).

-spec out(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(front, ServerRef, Timeout).

-spec out_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef) ->
	out_r(ServerRef, 0).

-spec out_r(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(rear, ServerRef, Timeout).

-spec peek(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
peek(ServerRef) ->
	gen_server:call(ServerRef, peek, infinity).

-spec peek_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
peek_r(ServerRef) ->
	gen_server:call(ServerRef, peek_r, infinity).

-spec size(ServerRef :: server_ref()) -> Size :: non_neg_integer().
size(ServerRef) ->
	gen_server:call(ServerRef, size, infinity).

-spec do_resolve(ServerRef :: server_ref()) -> Pid :: pid() | Name :: atom() | {Name :: atom(), Node :: atom()}.
do_resolve(Pid) when is_pid(Pid) ->
	Pid;
do_resolve(Name) when is_atom(Name) ->
	Name;
do_resolve({global, Name}) ->
	global:whereis_name(Name);
do_resolve({via, Via, Name}) ->
	Via:whereis_name(Name);
do_resolve({Name, Node}) when is_atom(Name), is_atom(Node), Node=:=node() ->
	Name;
do_resolve(Dest={Name, Node}) when is_atom(Name), is_atom(Node) ->
	Dest.

do_in_wait(Where, Value, ServerRef, Timeout) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	case gen_server:call(ServerRef, {in, Where, Value, Mon, Timeout}, infinity) of
		{wait, Tag} ->
			receive
				{Tag, accepting} ->
					gen_server:call(ServerRef, {accept, Tag, Where, Value}, infinity);
				{'DOWN', Mon, _, _, Reason} ->
					exit(Reason)
			after Timeout ->
				ok=gen_server:call(ServerRef, {cancel, Tag}, infinity),
				demonitor(Mon, [flush]),
				receive {Tag, accepting} -> ok after 0 -> ok end,
				full
			end;
		InstantReply ->
			demonitor(Mon, [flush]),
			InstantReply
	end.

do_out_wait(Where, ServerRef, Timeout) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	case gen_server:call(ServerRef, {out, Where, Mon, Timeout}, infinity) of
		{wait, Tag} ->
			receive
				{Tag, Reply} ->
					Reply;
				{'DOWN', Mon, _, _, Reason} ->
					exit(Reason)
			after Timeout ->
				ok=gen_server:call(ServerRef, {cancel, Tag}, infinity),
				demonitor(Mon, [flush]),
				receive
					{Tag, Reply} ->
						Reply
				after 0 ->
					empty
				end
			end;
		InstantReply ->
			demonitor(Mon, [flush]),
			InstantReply
	end.

verify_opts(Opts) ->
	maps:foreach(
		fun
			(max, infinity) ->
				ok;
			(max, N) when is_integer(N), N>=0 ->
				ok;
			(K, V) ->
				error({badoption, {K, V}})
		end,
		Opts
	),
	Opts.

-spec init(Opts :: opts()) -> {'ok', #state{}}.
init(Opts) ->
	Max=maps:get(max, Opts, infinity),
	Tab=ets:new(?MODULE, [protected, set]),
	{ok, #state{tab=Tab, max=Max}}.

-spec handle_call(Msg :: term(), From :: term(), State0 :: #state{}) -> {'reply', Reply :: term(), State1 :: #state{}} | {'noreply', State1 :: #state{}}.
handle_call({in, Where, Value, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, max=Max, monitors=Monitors0, waiting_in=WaitingIn, waiting_out=WaitingOut0}) ->
	case dequeue_waiting(WaitingOut0, Monitors0) of
		{undefined, WaitingOut1, Monitors1} when Max=:=infinity; Rear-Front<Max ->
			%% none waiting for out, queue not full -> insert
			{Front1, Rear1}=do_in(Where, Value, Front, Rear, Tab),
			{reply, ok, State#state{front=Front1, rear=Rear1, monitors=Monitors1, waiting_out=WaitingOut1}};
		{undefined, WaitingOut1, Monitors1} when Timeout=:=0 ->
			%% none waiting for out, queue full, in-timeout=0 -> full
			{reply, full, State#state{monitors=Monitors1, waiting_out=WaitingOut1}};
		{undefined, WaitingOut1, Monitors1} ->
			%% none waiting for out, queue full, in-timeout>0 -> wait
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{monitors=Monitors1#{Mon => in}, waiting_out=WaitingOut1, waiting_in=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, WaitingIn)}};
		{Tag, WaitingReplyTo, WaitingOut1, Monitors1} ->
			%% waiting out -> send value to waiting out, ok
			WaitingReplyTo ! {Tag, {ok, Value}},
			{reply, ok, State#state{monitors=Monitors1, waiting_out=WaitingOut1}}
	end;
handle_call({accept, Tag, Where, Value}, _From, State=#state{tab=Tab, front=Front, rear=Rear, monitors=Monitors0, waiting_out=WaitingOut0}) ->
	case maps:take(Tag, Monitors0) of
		{accepting, Monitors1} ->
			%% accepting tag
			case dequeue_waiting(WaitingOut0, Monitors1) of
				{undefined, WaitingOut1, Monitors2} ->
					%% none waiting for out -> insert
					{Front1, Rear1}=do_in(Where, Value, Front, Rear, Tab),
					{reply, ok, State#state{front=Front1, rear=Rear1, monitors=Monitors2, waiting_out=WaitingOut1}};
				{WaitingTag, WaitingReplyTo, WaitingOut1, Monitors2} ->
					%% waiting out -> send value to waiting out, ok
					WaitingReplyTo ! {WaitingTag, {ok, Value}},
					{reply, ok, State#state{monitors=Monitors2, waiting_out=WaitingOut1}}
			end;
		_ ->
			%% tag not accepted
			{reply, {error, not_accepted}, State}
	end;
handle_call({out, Where, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, monitors=Monitors0, waiting_in=WaitingIn0, waiting_out=WaitingOut}) ->
	case dequeue_waiting(WaitingIn0, Monitors0) of
		{undefined, WaitingIn1, Monitors1} when Front=/=Rear ->
			%% none waiting in, queue not empty -> value
			{Front1, Rear1, Value}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Value}, State#state{front=Front1, rear=Rear1, monitors=Monitors1, waiting_in=WaitingIn1}};
		{undefined, WaitingIn1, Monitors1} when Timeout=:=0 ->
			%% none waiting in, queue empty, out-timeout=0 -> empty
			{reply, empty, State#state{monitors=Monitors1, waiting_in=WaitingIn1}};
		{undefined, WaitingIn1, Monitors1} ->
			%% none waiting in, queue empty, out-timeout>0 -> wait
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{monitors=Monitors1#{Mon => out}, waiting_in=WaitingIn1, waiting_out=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, WaitingOut)}};
		{Tag, WaitingReplyTo, WaitingIn1, Monitors1} when Front=:=Rear ->
			%% waiting in, queue empty -> send accepting to waiting in, wait
			WaitingReplyTo ! {Tag, accepting},
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{monitors=Monitors1#{Tag => accepting, Mon => out}, waiting_in=WaitingIn1, waiting_out=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, WaitingOut)}};
		{Tag, WaitingReplyTo, WaitingIn1, Monitors1} ->
			%% waiting in, queue not empty -> send accepting to waiting in, value
			WaitingReplyTo ! {Tag, accepting},
			{Front1, Rear1, Value}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Value}, State#state{front=Front1, rear=Rear1, monitors=Monitors1#{Tag => accepting}, waiting_in=WaitingIn1}}
	end;
handle_call(peek, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(peek, _From, State=#state{tab=Tab, front=Front}) ->
	[{Front, Value}]=ets:lookup(Tab, Front),
	{reply, {ok, Value}, State};
handle_call(peek_r, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(peek_r, _From, State=#state{tab=Tab, rear=Rear0}) ->
	Rear1=Rear0-1,
	[{Rear1, Value}]=ets:lookup(Tab, Rear1),
	{reply, {ok, Value}, State};
handle_call({cancel, Tag}, _From, State=#state{monitors=Monitors0, waiting_in=WaitingIn, waiting_out=WaitingOut}) ->
	demonitor(Tag, [flush]),
	Fn=fun ({Mon, _MaxTS, _ReplyTo}) -> Tag=:=Mon end,
	case maps:take(Tag, Monitors0) of
		{accepting, Monitors1} ->
			%% Tag accepting
			{reply, ok, State#state{monitors=Monitors1}};
		{in, Monitors1} ->
			%% Tag waiting in
			{reply, ok, State#state{monitors=Monitors1, waiting_in=queue:delete_with(Fn, WaitingIn)}};
		{out, Monitors1} ->
			%% Tag waiting out
			{reply, ok, State#state{monitors=Monitors1, waiting_out=queue:delete_with(Fn, WaitingOut)}};
		_ ->
			%% Tag unknown
			{reply, ok, State}
	end;
handle_call(size, _From, State=#state{front=Front, rear=Rear}) ->
	{reply, Rear-Front, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

-spec handle_cast(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_cast(_Msg, State) ->
	{noreply, State}.

-spec handle_info(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State=#state{monitors=Monitors0, waiting_in=WaitingIn, waiting_out=WaitingOut}) ->
	Fn=fun ({WaitingMon, _MaxTS, _ReplyTo}) -> Mon=:=WaitingMon end,
	case maps:take(Mon, Monitors0) of
		{accepting, Monitors1} ->
			%% Mon accepting
			{noreply, State#state{monitors=Monitors1}};
		{in, Monitors1} ->
			%% Mon waiting in
			{noreply, State#state{monitors=Monitors1, waiting_in=queue:delete(Fn, WaitingIn)}};
		{out, Monitors1} ->
			%% Mon waiting out
			{noreply, State#state{monitors=Monitors1, waiting_out=queue:delete(Fn, WaitingOut)}};
		_ ->
			%% Mon unknown
			{noreply, State}
	end;
handle_info(_Msg, State) ->
	{noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> 'ok'.
terminate(_Reason, _State) ->
	ok.

-spec code_change(OldVsn :: (term() | {'down', term()}), State, Extra :: term()) -> {ok, State}
	when State :: #state{}.
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

do_in(rear, Value, Front, Rear, Tab) ->
	true=ets:insert(Tab, {Rear, Value}),
	{Front, Rear+1};
do_in(front, Value, Front, Rear, Tab) ->
	Front1=Front-1,
	true=ets:insert(Tab, {Front1, Value}),
	{Front1, Rear}.

do_out(front, Front, Rear, Tab) ->
	[{Front, Value}]=ets:take(Tab, Front),
	{Front+1, Rear, Value};
do_out(rear, Front, Rear, Tab) ->
	Rear1=Rear-1,
	[{Rear1, Value}]=ets:take(Tab, Rear1),
	{Front, Rear1, Value}.

dequeue_waiting(Waiting, Monitors) ->
	case queue:is_empty(Waiting) of
		true ->
			{undefined, Waiting, Monitors};
		false ->
			Now=erlang:monotonic_time(millisecond),
			dequeue_waiting(queue:out(Waiting), Monitors, Now)
	end.

dequeue_waiting({empty, Waiting}, Monitors, _Now) ->
	{undefined, Waiting, Monitors};
dequeue_waiting({{value, {Mon, infinity, ReplyTo}}, Waiting}, Monitors, _Now) ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting({{value, {Mon, MaxTS, ReplyTo}}, Waiting}, Monitors, Now) when MaxTS>Now ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting({{value, {Mon, _MaxTS, _ReplyTo}}, Waiting}, Monitors, Now) ->
	demonitor(Mon, [flush]),
	dequeue_waiting(queue:out(Waiting), maps:remove(Mon, Monitors), Now).

calc_maxts(infinity) ->
	infinity;
calc_maxts(Timeout) ->
	erlang:monotonic_time(millisecond)+Timeout.
