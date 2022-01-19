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
		monitors=#{} :: #{reference() => 'in' | 'out' | 'accepting'},
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
-spec in(ServerRef :: server_ref(), Item :: term()) -> 'ok' | 'full' | {'error', Reason :: 'not_accepted'}.
in(ServerRef, Item) ->
	in(ServerRef, Item, 0).

%% @doc Insert an item at the rear of a queue, waiting for the given timeout if the queue is full.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @param Timeout Timeout in milliseconds.
%% @return `ok' if the item was inserted, or `full' if the item could not be inserted within the given timeout.
-spec in(ServerRef :: server_ref(), Item :: term(), Timeout :: timeout()) -> 'ok' | 'full' | {'error', Reason :: 'not_accepted'}.
in(ServerRef, Item, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(rear, Item, ServerRef, Timeout).

%% @doc Insert an item at the front of a queue immediately.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @return `ok' if the item was inserted, or `full' if the queue was full.
-spec in_r(ServerRef :: server_ref(), Item :: term()) -> 'ok' | 'full' | {'error', Reason :: 'not_accepted'}.
in_r(ServerRef, Item) ->
	in_r(ServerRef, Item, 0).

%% @doc Insert an item at the front of a queue, waiting for the given timeout if the queue is full.
%% @param ServerRef See gen_server:call/2,3.
%% @param Item The item to insert.
%% @param Timeout Timeout in milliseconds.
%% @return `ok' if the item was inserted, or `full' if the item could not be inserted within the given timeout.
-spec in_r(ServerRef :: server_ref(), Item :: term(), Timeout :: timeout()) -> 'ok' | 'full' | {'error', Reason :: 'not_accepted'}.
in_r(ServerRef, Item, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_in_wait(front, Item, ServerRef, Timeout).

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
	do_out_wait(front, ServerRef, Timeout).

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
	do_out_wait(rear, ServerRef, Timeout).

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

%% @doc Special handling for insert operations with a timeout. When this function returns `full',
%%      it is guaranteed that the given item was not inserted into the queue.
%%
%%      To achieve this, we create a secondary alias for replies and send it along with the
%%      insert message to the queue.
%%
%%      The queue may reply instantly with either...
%%      <ul>
%%        <li>`ok' if the item could be immediately inserted</li>
%%        <li>`full' if the item was instantly rejected (only happens when `Timeout=0')</li>
%%      </ul>
%%
%%      If however it replies with `{wait, Tag}', we wait for either...
%%      <ul>
%%        <li>a `{Tag, accepting}' message (via the alias) by which the queue promises to accept
%%            the value we subsequently send in an `{accept, Tag, ...}' message, and return `ok'</li>
%%        <li>a ``'DOWN''' message, whereupon we exit with the same `Reason' as given in the
%%            ``'DOWN''' message</li>
%%        <li>the timeout to expire, in which case we send a `{cancel, Tag}' message to the queue,
%%            and flush out a possible `{Tag, accepting}' message from our message queue, and return
%%            with `full'</li>
%%      </ul>
-spec do_in_wait(Where :: ('front' | 'rear'), Item :: term(), ServerRef :: server_ref(), Timeout :: timeout()) -> 'ok' | 'full' | {'error', 'not_accepted'}.
do_in_wait(Where, Item, ServerRef, Timeout) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	case gen_server:call(ServerRef, {in, Where, Item, Mon, Timeout}, infinity) of
		{wait, Tag} ->
			receive
				{Tag, accepting} ->
					gen_server:call(ServerRef, {accept, Tag, Where, Item}, infinity);
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

%% @doc Special handling for retrieval operations with a timeout. When the function returns `empty',
%%      it is guaranteed that no item was removed from the queue.
%%
%%      To achieve this, we create a secondary alias for replies and send it along with the
%%      retrieval message to the queue.
%%
%%      The queue may reply instantly with either...
%%      <ul>
%%        <li>`{ok, Item}' if an item could be immediately retrieved</li>
%%        <li>`empty' if no item could be instantly retrieved (only happens when `Timeout=0')</li>
%%      </ul>
%%
%%      If however it replies with `{wait, Tag}', we wait for either...
%%      <ul>
%%        <li>a `{Tag, Reply}' message (via the alias), upon which we return the `Reply'</li>
%%        <li>a ``'DOWN''' message, whereupon we exit with the same `Reason' as given in the
%%            ``'DOWN''' message</li>
%%        <li>the timeout to expire, in which case we send a `{cancel, Tag}' message to the queue,
%%            and flush out a possible `{Tag, Reply}' from our message queue; if there is such a
%%            message, we return the `Reply', otherwise `empty'</li>
%%      </ul>
-spec do_out_wait(Where :: ('front' | 'rear'), ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Item :: term()} | 'empty'.
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
handle_call({in, Where, Item, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, max=Max, waiting=Waiting0}) ->
	case dequeue_out(Waiting0) of
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
			{reply, {wait, Mon}, State#state{waiting=enqueue_in(#waiter{monitor=Mon, max_timestamp=calc_maxts(Timeout), reply_to=ReplyTo}, Waiting1)}};
		{Waiter=#waiter{}, Waiting1} ->
			%% waiting out -> send value to waiting out, reply with ok
			reply_demonitor(Waiter, {ok, Item}),
			{reply, ok, State#state{waiting=Waiting1}}
	end;
handle_call({accept, Tag, Where, Item}, _From, State=#state{tab=Tab, front=Front, rear=Rear, waiting=Waiting0}) ->
	case remove_monitor(Tag, Waiting0) of
		{accepting, Waiting1} ->
			%% accepting tag
			demonitor(Tag, [flush]),
			case dequeue_out(Waiting1) of
				{undefined, Waiting2} ->
					%% none waiting for out -> insert
					{Front1, Rear1}=do_in(Where, Item, Front, Rear, Tab),
					{reply, ok, State#state{front=Front1, rear=Rear1, waiting=Waiting2}};
				{Waiter=#waiter{}, Waiting2} ->
					%% waiting out -> send value to waiting out, reply with ok
					reply_demonitor(Waiter, {ok, Item}),
					{reply, ok, State#state{waiting=Waiting2}}
			end;
		_ ->
			%% tag not accepted
			{reply, {error, not_accepted}, State}
	end;
handle_call({out, Where, ReplyTo, Timeout}, _From={Pid, _}, State=#state{tab=Tab, front=Front, rear=Rear, max=Max, waiting=Waiting0}) ->
	case dequeue_in(Waiting0) of
		{undefined, Waiting1} when Front=/=Rear ->
			%% none waiting in, queue not empty -> reply with value
			{Front1, Rear1, Item}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Item}, State#state{front=Front1, rear=Rear1, waiting=Waiting1}};
		{undefined, Waiting1} when Timeout=:=0 ->
			%% none waiting in, queue empty, out-timeout=0 -> reply with empty
			{reply, empty, State#state{waiting=Waiting1}};
		{undefined, Waiting1} ->
			%% none waiting in, queue empty, out-timeout>0 -> reply with wait
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{waiting=enqueue_out(#waiter{monitor=Mon, max_timestamp=calc_maxts(Timeout), reply_to=ReplyTo}, Waiting1)}};
		{Waiter=#waiter{}, Waiting1} when Front=:=Rear, Timeout=:=0 ->
			%% waiting in, queue empty and out-timeout=0 -> send accepting to waiting in, reply with empty (we can never serve this request in time)
			reply(Waiter, accepting),
			{reply, empty, State#state{waiting=set_accepting(Waiter, Waiting1)}};
		{Waiter=#waiter{}, Waiting1} when Front=:=Rear ->
			%% waiting in, queue empty and out-timeout>0 -> send accepting to waiting in, reply with wait
			reply(Waiter, accepting),
			Waiting2=set_accepting(Waiter, Waiting1),
			Mon=monitor(process, Pid),
			{reply, {wait, Mon}, State#state{waiting=enqueue_out(#waiter{monitor=Mon, max_timestamp=calc_maxts(Timeout), reply_to=ReplyTo}, Waiting2)}};
		{Waiter=#waiter{}, Waiting1} when Rear-Front=<Max ->
			%% waiting in, queue not empty and not full -> send accepting to waiting in, reply with value
			reply(Waiter, accepting),
			Waiting2=set_accepting(Waiter, Waiting1),
			{Front1, Rear1, Item}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Item}, State#state{front=Front1, rear=Rear1, waiting=Waiting2}};
		{Waiter=#waiter{}, Waiting1} ->
			%% waiting in, queue not empty but full -> reply with value
			{Front1, Rear1, Item}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Item}, State#state{front=Front1, rear=Rear1, waiting=requeue_in(Waiter, Waiting1)}}
	end;
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
handle_call({cancel, Tag}, _From, State=#state{waiting=Waiting0}) ->
	case remove_monitor(Tag, Waiting0) of
		error ->
			{reply, ok, State};
		{_Type, Waiting1} ->
			demonitor(Tag, [flush]),
			{reply, ok, State#state{waiting=Waiting1}}
	end;
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
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State=#state{waiting=Waiting0}) ->
	case remove_monitor(Mon, Waiting0) of
		error ->
			{noreply, State};
		{_Type, Waiting1} ->
			{noreply, State#state{waiting=Waiting1}}
	end;
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

-spec remove_monitor(Mon :: reference(), Waiting0 :: #waiting{}) -> 'error' | {Type :: ('in' | 'out' | 'accepting'), Waiting1 :: #waiting{}}.
remove_monitor(Mon, Waiting0=#waiting{monitors=Monitors0, in=In0, out=Out0}) ->
	case maps:take(Mon, Monitors0) of
		error ->
			error;
		{accepting, Monitors1} ->
			{accepting, Waiting0#waiting{monitors=Monitors1}};
		{Type, Monitors1} ->
			Waiting1=Waiting0#waiting{monitors=Monitors1},
			Fn=fun (#waiter{monitor=WaiterMon}) -> WaiterMon=:=Mon end,
			case Type of
				in ->
					In1=queue:delete_with(Fn, In0),
					{in, Waiting1#waiting{in=In1}};
				out ->
					Out1=queue:delete_with(Fn, Out0),
					{out, Waiting1#waiting{out=Out1}}
			end
	end.

-spec set_accepting(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
set_accepting(#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors}) ->
	Waiting#waiting{monitors=Monitors#{Mon => accepting}}.

-spec enqueue_in(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
enqueue_in(Waiter=#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors, in=In}) ->
	Waiting#waiting{monitors=Monitors#{Mon => in}, in=queue:in(Waiter, In)}.

-spec enqueue_out(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
enqueue_out(Waiter=#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors, out=Out}) ->
	Waiting#waiting{monitors=Monitors#{Mon => out}, out=queue:in(Waiter, Out)}.

-spec requeue_in(Waiter :: #waiter{}, Waiting0 :: #waiting{}) -> Waiting1 :: #waiting{}.
requeue_in(Waiter=#waiter{monitor=Mon}, Waiting=#waiting{monitors=Monitors, in=In}) ->
	Waiting#waiting{monitors=Monitors#{Mon => in}, in=queue:in_r(Waiter, In)}.

-spec dequeue_in(Waiting0 :: #waiting{}) -> {'undefined' | Waiter :: #waiter{}, Waiting1 :: #waiting{}}.
dequeue_in(Waiting=#waiting{monitors=Monitors0, in=In0}) ->
	{Res, In1, Monitors1}=dequeue_waiting(In0, Monitors0),
	{Res, Waiting#waiting{monitors=Monitors1, in=In1}}.

-spec dequeue_out(Waiting0 :: #waiting{}) -> {'undefined' | Waiter :: #waiter{}, Waiting1 :: #waiting{}}.
dequeue_out(Waiting=#waiting{monitors=Monitors0, out=Out0}) ->
	{Res, Out1, Monitors1}=dequeue_waiting(Out0, Monitors0),
	{Res, Waiting#waiting{monitors=Monitors1, out=Out1}}.

-spec dequeue_waiting(Queue0 :: queue:queue(#waiter{}), Monitors0 :: map()) -> {'undefined' | Waiter :: #waiter{}, Queue1 :: queue:queue(#waiter{}), Monitors1 :: map()}.
dequeue_waiting(Waiting, Monitors) ->
	case queue:is_empty(Waiting) of
		true ->
			{undefined, Waiting, Monitors};
		false ->
			Now=erlang:monotonic_time(millisecond),
			dequeue_waiting(queue:out(Waiting), Monitors, Now)
	end.

-spec dequeue_waiting({'empty' | {'value', Waiter}, Queue0 :: queue:queue(#waiter{})}, Monitors0 :: map(), Now :: integer()) -> {'undefined' | Waiter, Queue1 :: queue:queue(#waiter{}), Monitors1 :: map()}
	when Waiter :: #waiter{}.
dequeue_waiting({empty, Waiting}, Monitors, _Now) ->
	{undefined, Waiting, Monitors};
dequeue_waiting({{value, Waiter=#waiter{monitor=Mon, max_timestamp=infinity}}, Waiting}, Monitors, _Now) ->
	{Waiter, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting({{value, Waiter=#waiter{monitor=Mon, max_timestamp=MaxTS}}, Waiting}, Monitors, Now) when MaxTS>Now ->
	{Waiter, Waiting, maps:remove(Mon, Monitors)};
dequeue_waiting({{value, #waiter{monitor=Mon}}, Waiting}, Monitors, Now) ->
	demonitor(Mon, [flush]),
	dequeue_waiting(queue:out(Waiting), maps:remove(Mon, Monitors), Now).

-spec calc_maxts('infinity' | Time :: non_neg_integer()) -> 'infinity' | Timestamp :: integer().
calc_maxts(infinity) ->
	infinity;
calc_maxts(Timeout) ->
	erlang:monotonic_time(millisecond)+Timeout.
