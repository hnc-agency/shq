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

%% @doc Shared inter-process queue.
%%
%% @author Maria Scott <maria-12648430@hnc-agency.org>
%% @author Jan Uhlig <juhlig@hnc-agency.org>
%%
%% @copyright 2022, Maria Scott, Jan Uhlig

-module(shq).

-behavior(gen_server).

-export([start/1, start/2]).
-export([start_link/1, start_link/2]).
-export([start_monitor/1, start_monitor/2]).
-export([stop/1]).
-export([in/2, in/3, in_r/2, in_r/3]).
-export([out/1, out/2, out_r/1, out_r/2]).
-export([drain/1, drain_r/1]).
-export([peek/1, peek_r/1]).
-export([size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(waiter,
	{
		monitor :: reference(),
		max_timestamp :: integer() | 'infinity',
		reply_to :: pid() | reference()
	}
).

-record(waiting,
	{
		monitors=#{} :: #{reference() => 'in' | 'out'},
		in=queue:new() :: queue:queue(#waiter{}),
		out=queue:new() :: queue:queue(#waiter{})
	}
).

-record(state,
	{
		tab :: ets:tid(),
		front=0 :: integer(),
		rear=0 :: integer(),
		max=infinity :: non_neg_integer() | 'infinity',
		waiting=#waiting{} :: #waiting{}
	}
).

-type server_name() :: {'local', Name :: atom()}
		     | {'global', GlobalName :: term()}
		     | {'via', Via :: module(), ViaName :: term()}.
%% See gen_server:start/3,4, gen_server:start_link/3,4, gen_server:start_monitor/3,4.

-type server_ref() :: pid()
		    | (Name :: atom())
		    | {Name :: atom(), Node :: node()}
		    | {'global', GlobalName :: term()}
		    | {'via', Via :: module(), ViaName :: term()}.
%% See gen_server:call/3,4, gen_server:cast/2.

-type opts() :: opts_map() | opts_list().
%% Queue options, either a map or proplist.

-type opts_map() :: #{
			'max' => non_neg_integer() | 'infinity'
		     }.

-type opts_list() :: [
			{'max', non_neg_integer() | 'infinity'}
		     ].

%% @doc Start an non-linked anonymous queue.
%% @param Opts Queue options.
%% @return `{ok, Pid}'
-spec start(Opts :: opts()) -> {'ok', pid()}.
start(Opts) when is_map(Opts) ->
	gen_server:start(?MODULE, verify_opts(Opts), []);
start(Opts) when is_list(Opts) ->
	start(proplists:to_map(Opts)).

%% @doc Start an non-linked named queue.
%% @param ServerName See gen_server:start/4.
%% @param Opts Queue options.
%% @return `{ok, Pid}'
-spec start(ServerName :: server_name(), Opts :: opts()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start(ServerName, Opts) when is_map(Opts) ->
	gen_server:start(ServerName, ?MODULE, verify_opts(Opts), []);
start(ServerName, Opts) when is_list(Opts) ->
	start(ServerName, proplists:to_map(Opts)).

%% @doc Start an linked anonymous queue.
%% @param Opts Queue options.
%% @return `{ok, Pid}'
-spec start_link(Opts :: opts()) -> {'ok', pid()}.
start_link(Opts) when is_map(Opts) ->
	gen_server:start_link(?MODULE, verify_opts(Opts), []);
start_link(Opts) when is_list(Opts) ->
	start_link(proplists:to_map(Opts)).

%% @doc Start an linked named queue.
%% @param ServerName See gen_server:start_link/4.
%% @param Opts Queue options.
%% @return `{ok, Pid}'
-spec start_link(ServerName :: server_name(), Opts :: opts()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start_link(ServerName, Opts) when is_map(Opts) ->
	gen_server:start_link(ServerName, ?MODULE, verify_opts(Opts), []);
start_link(ServerName, Opts) when is_list(Opts) ->
	start_link(ServerName, proplists:to_map(Opts)).

%% @doc Start an monitored anonymous queue.
%% @param Opts Queue options.
%% @return `{ok, {Pid, Monitor}}'
-spec start_monitor(Opts :: opts()) -> {'ok', {pid(), reference()}}.
start_monitor(Opts) when is_map(Opts) ->
	gen_server:start_monitor(?MODULE, verify_opts(Opts), []);
start_monitor(Opts) when is_list(Opts) ->
	start_monitor(proplists:to_map(Opts)).

%% @doc Start an monitored named queue.
%% @param ServerName See gen_server:start_monitor/4.
%% @param Opts Queue options.
%% @return `{ok, {Pid, Monitor}}'
-spec start_monitor(ServerName :: server_name(), Opts :: opts()) -> {'ok', {pid(), reference()}} | {'error', Reason :: 'already_started'}.
start_monitor(ServerName, Opts) when is_map(Opts) ->
	gen_server:start_monitor(ServerName, ?MODULE, verify_opts(Opts), []);
start_monitor(ServerName, Opts) when is_list(Opts) ->
	start_monitor(ServerName, proplists:to_map(Opts)).

%% @doc Stop a queue.
%%      All remaining items will be discarded.
%% @param ServerRef See gen_server:stop/1.
%% @return `ok'.
-spec stop(ServerRef :: server_ref()) -> 'ok'.
stop(ServerRef) ->
	gen_server:stop(ServerRef).

%% @doc Insert an item at the rear of a queue immediately.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @return `ok' if the item was inserted, or `full' if the queue was full.
-spec in(ServerRef :: server_ref(), Item :: term()) -> 'ok' | 'full'.
in(ServerRef, Item) ->
	in(ServerRef, Item, 0).

%% @doc Insert an item at the rear of a queue, waiting for the given timeout if the queue is full.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @param Timeout Timeout in milliseconds.
%% @return `ok' if the item was inserted, or `full' if the item could not be inserted within the given timeout.
-spec in(ServerRef :: server_ref(), Item :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in(ServerRef, Item, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(ServerRef, rear, Item, Timeout).

%% @doc Insert an item at the front of a queue immediately.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @return `ok' if the item was inserted, or `full' if the queue was full.
-spec in_r(ServerRef :: server_ref(), Item :: term()) -> 'ok' | 'full'.
in_r(ServerRef, Item) ->
	in_r(ServerRef, Item, 0).

%% @doc Insert an item at the front of a queue, waiting for the given timeout if the queue is full.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @param Timeout Timeout in milliseconds.
%% @return `ok' if the item was inserted, or `full' if the item could not be inserted within the given timeout.
-spec in_r(ServerRef :: server_ref(), Item :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in_r(ServerRef, Item, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(ServerRef, front, Item, Timeout).

%% @doc Retrieve and remove an item from the front of a queue immediately.
%% @param ServerRef See gen_server:call/2,3.
%% @return `{ok, Item}' if an item was retrieved, or `empty' if the queue was empty.
-spec out(ServerRef :: server_ref()) -> {'ok', Item :: term()} | 'empty'.
out(ServerRef) ->
	out(ServerRef, 0).

%% @doc Retrieve and remove an item from the front of a queue, waiting for the given timeout if the queue is empty.
%% @param ServerRef See gen_server:call/2,3.
%% @param Timeout Timeout in milliseconds.
%% @return `{ok, Item}'  if an item was retrieved, or `empty' if an item could not be retrieved within the given timeout.
-spec out(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Item :: term()} | 'empty'.
out(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(ServerRef, front, Timeout).

%% @doc Retrieve and remove an item from the rear of a queue immediately.
%% @param ServerRef See gen_server:call/2,3.
%% @return `{ok, Item}' if an item was retrieved, or `empty' if the queue was empty.
-spec out_r(ServerRef :: server_ref()) -> {'ok', Item :: term()} | 'empty'.
out_r(ServerRef) ->
	out_r(ServerRef, 0).

%% @doc Retrieve and remove an item from the rear of a queue, waiting for the given timeout if the queue is empty.
%% @param ServerRef See gen_server:call/2,3.
%% @param Timeout Timeout in milliseconds.
%% @return `{ok, Item}'  if an item was retrieved, or `empty' if an item could not be retrieved within the given timeout.
-spec out_r(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Item :: term()} | 'empty'.
out_r(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(ServerRef, rear, Timeout).

%% @doc Drain the queue from the front.
%% @param ServerRef See gen_server:call/2,3.
%% @return A list of all items in the queue, in queue order.
-spec drain(ServerRef :: server_ref()) -> [Item :: term()].
drain(ServerRef) ->
	gen_server:call(ServerRef, {drain, front}, infinity).

%% @doc Drain the queue from the rear.
%% @param ServerRef See gen_server:call/2,3.
%% @return A list of all items in the queue, in reverse queue order.
-spec drain_r(ServerRef :: server_ref()) -> [Item :: term()].
drain_r(ServerRef) ->
	gen_server:call(ServerRef, {drain, rear}, infinity).

%% @doc Retrieve an item from the front of a queue without removing it.
%% @param ServerRef See gen_server:call/2,3.
%% @return `{ok, Item}' if an item was retrieved, or `empty' if the queue was empty.
-spec peek(ServerRef :: server_ref()) -> {'ok', Item :: term()} | 'empty'.
peek(ServerRef) ->
	gen_server:call(ServerRef, peek, infinity).

%% @doc Retrieve an item from the rear of a queue without removing it.
%% @param ServerRef See gen_server:call/2,3.
%% @return `{ok, Item}' if an item was retrieved, or `empty' if the queue was empty.
-spec peek_r(ServerRef :: server_ref()) -> {'ok', Item :: term()} | 'empty'.
peek_r(ServerRef) ->
	gen_server:call(ServerRef, peek_r, infinity).

%% @doc Get the number of items currently in a queue.
-spec size(ServerRef :: server_ref()) -> Size :: non_neg_integer().
size(ServerRef) ->
	gen_server:call(ServerRef, size, infinity).

%% @doc Resolve a server_ref to a monitorable pid or name.
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

%% @doc Special handling for insert operations with a timeout.
%% @see do_op_wait/5
-spec do_in_wait(ServerRef :: server_ref(), Where :: ('front' | 'rear'), Item :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
do_in_wait(ServerRef, Where, Item, Timeout) ->
	do_op_wait(ServerRef, in, {Where, Item}, Timeout, full).

%% @doc Special handling for retrieval operations with a timeout.
%% @see do_op_wait/5
-spec do_out_wait(ServerRef :: server_ref(), Where :: ('front' | 'rear'), Timeout :: timeout()) -> {'ok', Item :: term()} | 'empty'.
do_out_wait(ServerRef, Where, Timeout) ->
	do_op_wait(ServerRef, out, Where, Timeout, empty).

%% @doc Special handling for insert/retrieval operations with a timeout, in order to guarantee
%%      that the operation either succeeds and returns an `ok' to the caller, or that the operation
%%      will not succeed later, ie after the timeout has expired and `full' or `empty' was returned
%%      to the caller.
%%
%%      To achieve this, we create a secondary alias and send it along with the request.
%%      The operation may either succeed/fail immediately, in which case the reply is returned
%%      to the caller, or the queue may respond with a `{wait, Tag}' message, indicating that
%%      it will send the actual reply later via the alias. If a `{Tag, Reply}' message
%%      arrives before the given timeout expires, the `Reply' will be sent returned to the
%%      caller. If a ``'DOWN''' message arrives, meaning the queue has exited and an exit exception is
%%      raised with exit reason `{error, Reason}'. If the timeout expires, the queue is instructed
%%      to cancel the request. Finally, we flush out a possibly remaining `{Tag, Reply}' message.
%%      If there is no such message, we return the given `TimeoutReply'.
-spec do_op_wait(ServerRef :: server_ref(), Op :: 'in', {Where :: ('front' | 'rear'), Item :: term()}, Timeout :: timeout(), TimeoutReply) -> 'ok' | 'full' | TimeoutReply
	when TimeoutReply :: 'full';
                (ServerRef :: server_ref(), Op :: 'out', Where :: ('front' | 'rear'), Timeout :: timeout(), TimeoutReply) -> {'ok', Item :: term()} | 'empty' | TimeoutReply
	when TimeoutReply :: 'empty'.
do_op_wait(ServerRef, Op, How, Timeout, TimeoutReply) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	case gen_server:call(ServerRef, {Op, How, Mon, Timeout}, infinity) of
		{wait, Tag} ->
			receive
				{Tag, Reply} ->
					Reply;
				{'DOWN', Mon, _, _, Reason} ->
					exit({error, Reason})
			after Timeout ->
				ok=gen_server:call(ServerRef, {cancel, Tag}, infinity),
				demonitor(Mon, [flush]),
				receive
					{Tag, Reply} ->
						Reply
				after 0 ->
					TimeoutReply
				end
			end;
		InstantReply ->
			demonitor(Mon, [flush]),
			InstantReply
	end.

%% @doc Verify queue options and option values.
-spec verify_opts(Opts) -> Opts
	when Opts :: opts().
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

%% @private
-spec init(Opts :: opts()) -> {'ok', #state{}}.
init(Opts) ->
	Max=maps:get(max, Opts, infinity),
	Tab=ets:new(?MODULE, [protected, set]),
	{ok, #state{tab=Tab, max=Max}}.

%% @private
-spec handle_call(Msg :: term(), From :: term(), State0 :: #state{}) -> {'reply', Reply :: term(), State1 :: #state{}} | {'noreply', State1 :: #state{}}.
handle_call({in, {Where, Item}, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, max=Max, waiting=Waiting0}) ->
	case dequeue_out(Waiting0, Tab) of
		{undefined, Waiting1} when Max=:=infinity; Rear-Front<Max ->
			%% none waiting for out, queue not full -> insert, reply with ok
			{Front1, Rear1}=do_in(Where, Item, Front, Rear, Tab),
			{reply, ok, State#state{front=Front1, rear=Rear1, waiting=Waiting1}};
		{undefined, Waiting1} when Timeout=:=0 ->
			%% none waiting for out, queue full, in-timeout=0 -> reply with full (we can never serve this request in time)
			{reply, full, State#state{waiting=Waiting1}};
		{undefined, Waiting1} ->
			%% none waiting for out, queue full, in-timeout>0 -> reply with wait
			Mon=monitor(process, Pid),
			ets:insert(Tab, {Mon, Where, Item}),
			{reply, {wait, Mon}, State#state{waiting=enqueue_in(#waiter{monitor=Mon, max_timestamp=calc_maxts(Timeout), reply_to=ReplyTo}, Waiting1)}};
		{Waiter=#waiter{}, Waiting1} ->
			%% waiting out -> send value to waiting out, reply with ok
			reply_demonitor(Waiter, {ok, Item}),
			{reply, ok, State#state{waiting=Waiting1}}
	end;
handle_call({out, Where, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, waiting=Waiting0}) ->
	case dequeue_in(Waiting0, Tab) of
		{undefined, Waiting1} when Front=/=Rear ->
			%% none waiting in, queue not empty -> reply with item
			{Front1, Rear1, Item}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Item}, State#state{front=Front1, rear=Rear1, waiting=Waiting1}};
		{undefined, Waiting1} when Timeout=:=0 ->
			%% none waiting in, queue empty, out-timeout=0 -> reply with empty (we can never serve this request in time)
			{reply, empty, State#state{waiting=Waiting1}};
		{undefined, Waiting1} ->
			%% none waiting in, queue empty, out-timeout>0 -> reply with wait
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{waiting=enqueue_out(#waiter{monitor=Mon, max_timestamp=calc_maxts(Timeout), reply_to=ReplyTo}, Waiting1)}};
		{Waiter=#waiter{monitor=Mon}, Waiting1} when Front=:=Rear ->
			%% waiting in, queue empty -> send ok to waiting in, reply with item
			[{Mon, _InWhere, Item}]=ets:take(Tab, Mon),
			reply_demonitor(Waiter, ok),
			{reply, {ok, Item}, State#state{waiting=Waiting1}};
		{Waiter=#waiter{monitor=Mon}, Waiting1} ->
			%% waiting in, queue not empty but full -> reply with value
			{Front1, Rear1, OutItem}=do_out(Where, Front, Rear, Tab),
			[{Mon, InWhere, InItem}]=ets:take(Tab, Mon),
			{Front2, Rear2}=do_in(InWhere, InItem, Front1, Rear1, Tab),
			reply_demonitor(Waiter, ok),
			{reply, {ok, OutItem}, State#state{front=Front2, rear=Rear2, waiting=Waiting1}}
	end;
handle_call({drain, Where}, _From, State=#state{tab=Tab, front=Front0, rear=Rear0, waiting=Waiting0}) ->
	{Front1, Rear1, Waiting1, Items}=do_drain(Where, Front0, Rear0, Tab, Waiting0),
	{reply, Items, State#state{front=Front1, rear=Rear1, waiting=Waiting1}};
handle_call(peek, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(peek, _From, State=#state{tab=Tab, front=Front}) ->
	[{Front, Item}]=ets:lookup(Tab, Front),
	{reply, {ok, Item}, State};
handle_call(peek_r, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(peek_r, _From, State=#state{tab=Tab, rear=Rear0}) ->
	Rear1=Rear0-1,
	[{Rear1, Item}]=ets:lookup(Tab, Rear1),
	{reply, {ok, Item}, State};
handle_call({cancel, Tag}, _From, State=#state{tab=Tab, waiting=Waiting0}) ->
	Waiting1=remove_waiter(Tag, Waiting0, Tab),
	{reply, ok, State#state{waiting=Waiting1}};
handle_call(size, _From, State=#state{front=Front, rear=Rear}) ->
	{reply, Rear-Front, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

%% @private
-spec handle_cast(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_cast(_Msg, State) ->
	{noreply, State}.

%% @private
-spec handle_info(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State=#state{tab=Tab, waiting=Waiting0}) ->
	Waiting1=remove_waiter(Mon, Waiting0, Tab),
	{noreply, State#state{waiting=Waiting1}};
handle_info(_Msg, State) ->
	{noreply, State}.

%% @private
-spec terminate(Reason :: term(), State :: #state{}) -> 'ok'.
terminate(_Reason, _State) ->
	ok.

%% @private
-spec code_change(OldVsn :: (term() | {'down', term()}), State, Extra :: term()) -> {ok, State}
	when State :: #state{}.
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

-spec do_drain('front' | 'rear', Front0 :: integer(), Rear0 :: integer(), Tab :: ets:tid(), Waiting0 :: #waiting{}) -> {Front1 :: integer(), Rear1 :: integer(), Waiting1 :: #waiting{}, [Item :: term()]}.
do_drain(Where, Front0, Rear0, Tab, Waiting0) ->
	do_drain(Where, Front0, Rear0, Tab, Waiting0, []).

-spec do_drain('front' | 'rear', Front0 :: integer(), Rear0 :: integer(), Tab :: ets:tid(), Waiting0 :: #waiting{}, Acc :: [term()]) -> {Front1 :: integer(), Rear1 :: integer(), Waiting1 :: #waiting{}, [Item :: term()]}.
do_drain(Where, Front0, Rear0, Tab, Waiting0, Acc) ->
	case dequeue_in(Waiting0, Tab) of
		{undefined, Waiting1} when Front0=:=Rear0 ->
			{Front0, Rear0, Waiting1, lists:reverse(Acc)};
		{undefined, Waiting1} ->
			{Front1, Rear1, Item}=do_out(Where, Front0, Rear0, Tab),
			do_drain(Where, Front1, Rear1, Tab, Waiting1, [Item|Acc]);
		{Waiter=#waiter{monitor=Mon}, Waiting1} when Front0=:=Rear0 ->
			[{Mon, _InWhere, InItem}]=ets:take(Tab, Mon),
			reply_demonitor(Waiter, ok),
			do_drain(Where, Front0, Rear0, Tab, Waiting1, [InItem|Acc]);
		{Waiter=#waiter{monitor=Mon}, Waiting1} ->
			{Front1, Rear1, Item}=do_out(Where, Front0, Rear0, Tab),
			[{Mon, InWhere, InItem}]=ets:take(Tab, Mon),
			{Front2, Rear2}=do_in(InWhere, InItem, Front1, Rear1, Tab),
			reply_demonitor(Waiter, ok),
			do_drain(Where, Front2, Rear2, Tab, Waiting1, [Item|Acc])
	end.

-spec do_in('front' | 'rear', Item :: term(), Front0 :: integer(), Rear0 :: integer(), Tab :: ets:tid()) -> {Front1 :: integer(), Rear1 :: integer()}.
do_in(rear, Item, Front, Rear, Tab) ->
	true=ets:insert(Tab, {Rear, Item}),
	{Front, Rear+1};
do_in(front, Item, Front, Rear, Tab) ->
	Front1=Front-1,
	true=ets:insert(Tab, {Front1, Item}),
	{Front1, Rear}.

-spec do_out('front' | 'rear', Front0 :: integer(), Rear0 :: integer(), Tab :: ets:tid()) -> {Front1 :: integer(), Rear1 :: integer(), Item :: term()}.
do_out(front, Front, Rear, Tab) ->
	[{Front, Item}]=ets:take(Tab, Front),
	{Front+1, Rear, Item};
do_out(rear, Front, Rear, Tab) ->
	Rear1=Rear-1,
	[{Rear1, Item}]=ets:take(Tab, Rear1),
	{Front, Rear1, Item}.

-spec reply(Waiter :: #waiter{}, Msg :: term()) -> 'ok'.
reply(#waiter{monitor=Mon, reply_to=ReplyTo}, Msg) ->
	ReplyTo ! {Mon, Msg},
	ok.

-spec reply_demonitor(Waiter :: #waiter{}, Msg :: term()) -> 'ok'.
reply_demonitor(Waiter=#waiter{monitor=Mon}, Msg) ->
	reply(Waiter, Msg),
	demonitor(Mon, [flush]),
	ok.

-spec remove_waiter(Mon :: reference(), Waiting0 :: #waiting{}, Tab :: ets:tid()) -> Waiting1 :: #waiting{}.
remove_waiter(Mon, Waiting0=#waiting{monitors=Monitors0, in=In0, out=Out0}, Tab) ->
	case maps:take(Mon, Monitors0) of
		error ->
			Waiting0;
		{Type, Monitors1} ->
			demonitor(Mon, [flush]),
			Waiting1=Waiting0#waiting{monitors=Monitors1},
			Fn=fun (#waiter{monitor=WaiterMon}) -> WaiterMon=:=Mon end,
			case Type of
				in ->
					ets:delete(Tab, Mon),
					In1=queue:delete_with(Fn, In0),
					Waiting1#waiting{in=In1};
				out ->
					Out1=queue:delete_with(Fn, Out0),
					Waiting1#waiting{out=Out1}
			end
	end.

-spec enqueue_in(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
enqueue_in(Waiter=#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors, in=In}) ->
	Waiting#waiting{monitors=Monitors#{Mon => in}, in=queue:in(Waiter, In)}.

-spec enqueue_out(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
enqueue_out(Waiter=#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors, out=Out}) ->
	Waiting#waiting{monitors=Monitors#{Mon => out}, out=queue:in(Waiter, Out)}.

-spec dequeue_in(Waiting0 :: #waiting{}, Tab :: ets:tid()) -> {'undefined' | Waiter :: #waiter{}, Waiting1 :: #waiting{}}.
dequeue_in(Waiting=#waiting{monitors=Monitors0, in=In0}, Tab) ->
	{Res, In1, Monitors1}=dequeue_waiting(in, In0, Monitors0, Tab),
	{Res, Waiting#waiting{monitors=Monitors1, in=In1}}.

-spec dequeue_out(Waiting0 :: #waiting{}, Tab :: ets:tid()) -> {'undefined' | Waiter :: #waiter{}, Waiting1 :: #waiting{}}.
dequeue_out(Waiting=#waiting{monitors=Monitors0, out=Out0}, Tab) ->
	{Res, Out1, Monitors1}=dequeue_waiting(out, Out0, Monitors0, Tab),
	{Res, Waiting#waiting{monitors=Monitors1, out=Out1}}.

-spec dequeue_waiting(Type :: ('in' | 'out'), Queue0 :: queue:queue(#waiter{}), Monitors0 :: map(), Tab :: ets:tid()) -> {'undefined' | Waiter :: #waiter{}, Queue1 :: queue:queue(#waiter{}), Monitors1 :: map()}.
dequeue_waiting(Type, Waiting, Monitors, Tab) ->
	case queue:is_empty(Waiting) of
		true ->
			{undefined, Waiting, Monitors};
		false ->
			Now=erlang:monotonic_time(millisecond),
			dequeue_waiting(Type, queue:out(Waiting), Monitors, Tab, Now)
	end.

-spec dequeue_waiting(Type, {'empty' | {'value', Waiter}, Queue0}, Monitors0, Tab :: ets:tid(), Now :: integer()) -> {'undefined' | Waiter, Queue1, Monitors1}
	when Type :: 'in' | 'out',
	     Waiter :: #waiter{},
	     Queue0 :: queue:queue(Waiter | #waiter{}),
	     Queue1 :: queue:queue(#waiter{}),
	     Monitors0 :: #{reference() => Type},
	     Monitors1 :: #{reference() => Type}.
dequeue_waiting(_Type, {empty, Waiting}, Monitors, _Tab, _Now) ->
	{undefined, Waiting, Monitors};
dequeue_waiting(_Type, {{value, Waiter=#waiter{monitor=Mon, max_timestamp=infinity}}, Waiting}, Monitors, _Tab, _Now) ->
	{Waiter, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting(_Type, {{value, Waiter=#waiter{monitor=Mon, max_timestamp=MaxTS}}, Waiting}, Monitors, _Tab, Now) when MaxTS>Now ->
	{Waiter, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting(in, {{value, #waiter{monitor=Mon}}, Waiting}, Monitors, Tab, Now) ->
	ets:delete(Tab, Mon),
	demonitor(Mon, [flush]),
	dequeue_waiting(in, queue:out(Waiting), maps:remove(Mon, Monitors), Tab, Now);
dequeue_waiting(out, {{value, #waiter{monitor=Mon}}, Waiting}, Monitors, Tab, Now) ->
	demonitor(Mon, [flush]),
	dequeue_waiting(out, queue:out(Waiting), maps:remove(Mon, Monitors), Tab, Now).

-spec calc_maxts('infinity' | Time :: non_neg_integer()) -> 'infinity' | Timestamp :: integer().
calc_maxts(infinity) ->
	infinity;
calc_maxts(Timeout) ->
	erlang:monotonic_time(millisecond)+Timeout.
