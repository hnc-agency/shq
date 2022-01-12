-module(shq).

-behavior(gen_server).

-export([start/1, start/2]).
-export([start_link/1, start_link/2]).
-export([start_monitor/1, start_monitor/2]).
-export([stop/1, stop/3]).
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

-spec stop(ServerRef :: server_ref(), Reason :: term(), Timeout :: (pos_integer() | 'infinity')) -> 'ok'.
stop(ServerRef, Reason, Timeout) ->
	gen_server:stop(ServerRef, Reason, Timeout).

-spec in(ServerRef :: server_ref(), Value :: term()) -> 'ok' | 'full'.
in(ServerRef, Value) ->
	in(ServerRef, Value, 0).

-spec in(ServerRef :: server_ref(), Value :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in(ServerRef, Value, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_op_wait({in, rear, Value}, ServerRef, Timeout, full).

-spec in_r(ServerRef :: server_ref(), Value :: term()) -> 'ok' | 'full'.
in_r(ServerRef, Value) ->
	in_r(ServerRef, Value, 0).

-spec in_r(ServerRef :: server_ref(), Value :: term(), Timeout :: timeout()) -> 'ok' | 'full'.
in_r(ServerRef, Value, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_op_wait({in, front, Value}, ServerRef, Timeout, full).

-spec out(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef) ->
	out(ServerRef, 0).

-spec out(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_op_wait({out, front}, ServerRef, Timeout, empty).

-spec out_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef) ->
	out_r(ServerRef, 0).

-spec out_r(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_op_wait({out, rear}, ServerRef, Timeout, empty).

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

do_op_wait(Op, ServerRef, Timeout, TimeoutReply) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	try
		gen_server:call(ServerRef, {Op, Mon, Timeout}, infinity)
	of
		{queued, Type, Tag} ->
			receive
				{Tag, Reply} ->
					Reply;
				{'DOWN', Mon, _, _, Reason} ->
					exit(Reason)
			after Timeout ->
				ok=gen_server:call(ServerRef, {cancel, Type, Tag}, infinity),
				receive
					{Tag, Reply} ->
						Reply
				after 0 ->
					TimeoutReply
				end
			end;
		InstantReply ->
			InstantReply
	after
		demonitor(Mon, [flush])
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
handle_call({{in, Where, Value}, ReplyTo, Timeout}, _From={Pid, _}, State) ->
	handle_in(Where, Value, ReplyTo, Timeout, Pid, State);
handle_call({{out, Where}, ReplyTo, Timeout}, _From={Pid, _}, State) ->
	handle_out(Where, ReplyTo, Timeout, Pid, State);
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
handle_call({cancel, out, Tag}, _From, State=#state{waiting_out=Waiting0}) ->
	Waiting1=queue:delete_with(
		fun
			({Mon, _MaxTS, _ReplyTo}) when Tag=:=Mon ->
				demonitor(Mon, [flush]),
				true;
			(_Waiter) ->
				false
		 end,
		Waiting0
	),
	{reply, ok, State#state{waiting_out=Waiting1}};
handle_call({cancel, in, Tag}, _From, State=#state{tab=Tab, waiting_in=Waiting0}) ->
	Waiting1=queue:delete_with(
		fun
			({Mon, _MaxTS, _ReplyTo}) when Tag=:=Mon ->
				demonitor(Mon, [flush]),
				true=ets:delete(Tab, Mon),
				true;
			(_Waiter) ->
				false
		end,
		Waiting0
	),
	{reply, ok, State#state{waiting_in=Waiting1}};
handle_call(size, _From, State=#state{front=Front, rear=Rear}) ->
	{reply, Rear-Front, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

-spec handle_cast(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_cast(_Msg, State) ->
	{noreply, State}.

-spec handle_info(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State=#state{tab=Tab, waiting_in=WaitingIn0, waiting_out=WaitingOut0}) ->
	Fn=fun ({WaitingMon, _MaxTS, _ReplyTo}) -> Mon=:=WaitingMon end,
	case ets:take(Tab, Mon) of
		[] ->
			{noreply, State#state{waiting_out=queue:delete_with(Fn, WaitingOut0)}};
		[{Mon, _Op, _Value}] ->
			{noreply, State#state{waiting_in=queue:delete_with(Fn, WaitingIn0)}}
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

handle_in(Where, Value, ReplyTo, Timeout, Pid, State) ->
	#state{tab=Tab, front=Front, rear=Rear, max=Max, waiting_in=WaitingIn, waiting_out=WaitingOut0}=State,
	case dequeue_waiting(out, WaitingOut0) of
		{undefined, WaitingOut1} when Max=:=infinity; Rear-Front<Max ->
			{Front1, Rear1}=do_in(Where, Value, Front, Rear, Tab),
			{reply, ok, State#state{front=Front1, rear=Rear1, waiting_out=WaitingOut1}};
		{undefined, WaitingOut1} when Timeout=:=0 ->
			{reply, full, State#state{waiting_out=WaitingOut1}};
		{undefined, WaitingOut1} ->
			Mon=monitor(process, Pid),
			true=ets:insert(Tab, {Mon, Where, Value}),
			{reply, {queued, in, Mon}, State#state{waiting_out=WaitingOut1, waiting_in=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, WaitingIn)}};
		{Tag, WaitingReplyTo, WaitingOut1} ->
			WaitingReplyTo ! {Tag, {ok, Value}},
			{reply, ok, State#state{waiting_out=WaitingOut1}}
	end.

do_in(rear, Value, Front, Rear, Tab) ->
	true=ets:insert(Tab, {Rear, Value}),
	{Front, Rear+1};
do_in(front, Value, Front, Rear, Tab) ->
	Front1=Front-1,
	true=ets:insert(Tab, {Front1, Value}),
	{Front1, Rear}.

handle_out(Where, ReplyTo, Timeout, Pid, State) ->
	#state{tab=Tab, front=Front, rear=Rear, waiting_in=WaitingIn0, waiting_out=WaitingOut}=State,
	case dequeue_waiting({in, Tab}, WaitingIn0) of
		{undefined, WaitingIn1} when Front=/=Rear ->
			{Front1, Rear1, Value}=do_out(Where, Front, Rear, Tab),
			{reply, {ok, Value}, State#state{front=Front1, rear=Rear1, waiting_in=WaitingIn1}};
		{undefined, WaitingIn1} when Timeout=:=0 ->
			{reply, empty, State#state{waiting_in=WaitingIn1}};
		{undefined, WaitingIn1} ->
			Mon=monitor(process, Pid),
			{reply, {queued, out, Mon}, State#state{waiting_in=WaitingIn1, waiting_out=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, WaitingOut)}};
		{Tag, WaitingReplyTo, WaitingIn1} when Front=:=Rear ->
			{Tag, _Op, WaitingValue}=ets:take(Tab, Tag),
			WaitingReplyTo ! {Tag, ok},
			{reply, {ok, WaitingValue}, State#state{waiting_in=WaitingIn1}};
		{Tag, WaitingReplyTo, WaitingIn1} ->
			{Front1, Rear1, Value}=do_out(Where, Front, Rear, Tab),
			WaitingReplyTo ! {Tag, ok},
			[{Tag, WaitingWhere, WaitingValue}]=ets:take(Tab, Tag),
			{Front2, Rear2}=do_in(WaitingWhere, WaitingValue, Front1, Rear1, Tab),
			{reply, {ok, Value}, State#state{front=Front2, rear=Rear2, waiting_in=WaitingIn1}}
	end.

do_out(front, Front, Rear, Tab) ->
	[{Front, Value}]=ets:take(Tab, Front),
	{Front+1, Rear, Value};
do_out(rear, Front, Rear, Tab) ->
	Rear1=Rear-1,
	[{Rear1, Value}]=ets:take(Tab, Rear1),
	{Front, Rear1, Value}.

-spec dequeue_waiting(Type :: ('out' | {'in', Tab :: ets:tid()}), Waiting0) -> {Mon, ReplyTo, Waiting1} | {'undefined', Waiting1}
	when WaitItem :: {Mon, MaxTs :: (integer() | 'infinity'), ReplyTo},
	     Waiting0 :: queue:queue(WaitItem),
	     Waiting1 :: queue:queue(WaitItem),
	     Mon :: reference(),
	     ReplyTo :: pid() | reference().
dequeue_waiting(Type, Waiting) ->
	case queue:is_empty(Waiting) of
		true ->
			{undefined, Waiting};
		false ->
			Now=erlang:monotonic_time(millisecond),
			dequeue_waiting(Type, queue:out(Waiting), Now)
	end.

-spec dequeue_waiting(Type :: ('out' | {'in', Tab :: ets:tid()}), {'empty' | {'value', WaitItem}, Waiting}, Now :: integer()) -> {Mon, ReplyTo, Waiting} | {'undefined', Waiting}
	when WaitItem :: {Mon, MaxTs :: (integer() | 'infinity'), ReplyTo},
	     Waiting :: queue:queue(WaitItem),
	     Mon :: reference(),
	     ReplyTo :: pid() | reference().
dequeue_waiting(_Type, {empty, Waiting}, _Now) ->
	{undefined, Waiting};
dequeue_waiting(_Type, {{value, {Mon, infinity, ReplyTo}}, Waiting}, _Now) ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting};
dequeue_waiting(_Type, {{value, {Mon, MaxTS, ReplyTo}}, Waiting}, Now) when MaxTS>Now ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting};
dequeue_waiting(out, {{value, {Mon, _MaxTS, _ReplyTo}}, Waiting}, Now) ->
	demonitor(Mon, [flush]),
	dequeue_waiting(out, queue:out(Waiting), Now);
dequeue_waiting(Type={in, Tab}, {{value, {Mon, _MaxTS, _ReplyTo}}, Waiting}, Now) ->
	true=ets:delete(Tab, Mon),
	demonitor(Mon, [flush]),
	dequeue_waiting(Type, queue:out(Waiting), Now).

-spec calc_maxts(Timeout :: timeout()) -> integer() | 'infinity'.
calc_maxts(infinity) ->
	infinity;
calc_maxts(Timeout) ->
	erlang:monotonic_time(millisecond)+Timeout.
