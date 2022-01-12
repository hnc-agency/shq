-module(shq).

-behavior(gen_server).

-export([start/0, start/1]).
-export([start_link/0, start_link/1]).
-export([start_monitor/0, start_monitor/1]).
-export([stop/1, stop/3]).
-export([in/2, in_r/2]).
-export([out/1, out_r/1]).
-export([out_wait/1, out_wait/2, out_r_wait/1, out_r_wait/2]).
-export([peek/1, peek_r/1]).
-export([size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state,
	{
		tab :: ets:tid(),
		front=0 :: integer(),
		rear=0 :: integer(),
		wait_queue=queue:new() :: queue:queue()
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

-opaque tag() :: reference().
-export_type([tag/0]).

-spec start() -> {'ok', pid()}.
start() ->
	gen_server:start(?MODULE, [], []).

-spec start(ServerName :: server_name()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start(ServerName) ->
	gen_server:start(ServerName, ?MODULE, [], []).

-spec start_link() -> {'ok', pid()}.
start_link() ->
	gen_server:start_link(?MODULE, [], []).

-spec start_link(ServerName :: server_name()) -> {'ok', pid()} | {'error', Reason :: 'already_started'}.
start_link(ServerName) ->
	gen_server:start_link(ServerName, ?MODULE, [], []).

-spec start_monitor() -> {'ok', {pid(), reference()}}.
start_monitor() ->
	gen_server:start_monitor(?MODULE, [], []).

-spec start_monitor(ServerName :: server_name()) -> {'ok', {pid(), reference()}} | {'error', Reason :: 'already_started'}.
start_monitor(ServerName) ->
	gen_server:start_monitor(ServerName, ?MODULE, [], []).

-spec stop(ServerRef :: server_ref()) -> 'ok'.
stop(ServerRef) ->
	gen_server:stop(ServerRef).

-spec stop(ServerRef :: server_ref(), Reason :: term(), Timeout :: (pos_integer() | 'infinity')) -> 'ok'.
stop(ServerRef, Reason, Timeout) ->
	gen_server:stop(ServerRef, Reason, Timeout).

-spec in(ServerRef :: server_ref(), Value :: term()) -> 'ok'.
in(ServerRef, Value) ->
	gen_server:call(ServerRef, {in, Value}, infinity).

-spec in_r(ServerRef :: server_ref(), Value :: term()) -> 'ok'.
in_r(ServerRef, Value) ->
	gen_server:call(ServerRef, {in_r, Value}, infinity).

-spec out(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef) ->
	gen_server:call(ServerRef, out, infinity).

-spec out_wait(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_wait(ServerRef) ->
	out_wait(ServerRef, infinity).

-spec out_wait(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out_wait(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(out_wait, ServerRef, Timeout).

-spec out_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef) ->
	gen_server:call(ServerRef, out_r, infinity).

-spec out_r_wait(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_r_wait(ServerRef) ->
	out_r_wait(ServerRef, infinity).

-spec out_r_wait(ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
out_r_wait(ServerRef, Timeout) when Timeout=:=infinity; is_integer(Timeout), Timeout>=0 ->
	do_out_wait(out_r_wait, ServerRef, Timeout).

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

-spec do_out_wait(Op :: ('out_wait' | 'out_r_wait'), ServerRef :: server_ref(), Timeout :: timeout()) -> {'ok', Value :: term()} | 'empty'.
do_out_wait(Op, ServerRef, Timeout) ->
	Mon=monitor(process, do_resolve(ServerRef), [{alias, reply_demonitor}]),
	try
		gen_server:call(ServerRef, {Op, Mon, Timeout}, infinity)
	of
		{queued, Tag} ->
			receive
				{Tag, Reply} ->
					Reply;
				{'DOWN', Mon, _, _, Reason} ->
					exit(Reason)
			after Timeout ->
				ok=gen_server:call(ServerRef, {cancel, Tag}, infinity),
				receive
					{Tag, Reply} ->
						Reply
				after 0 ->
					empty
				end
			end;
		InstantReply={ok, _Value} ->
			InstantReply
	after
		demonitor(Mon, [flush])
	end.

-spec init([]) -> {'ok', #state{}}.
init([]) ->
	Tab=ets:new(?MODULE, [protected, set]),
	{ok, #state{tab=Tab}}.

-spec handle_call(Msg :: term(), From :: term(), State0 :: #state{}) -> {'reply', Reply :: term(), State1 :: #state{}} | {'noreply', State1 :: #state{}}.
handle_call({in, Value}, _From, State=#state{tab=Tab, front=Index, rear=Index, wait_queue=Waiting0}) ->
	case dequeue_waiting(Waiting0) of
		{undefined, Waiting1} ->
			true=ets:insert(Tab, {Index, Value}),
			{reply, ok, State#state{rear=Index+1, wait_queue=Waiting1}};
		{Tag, ReplyTo, Waiting1} ->
			ReplyTo ! {Tag, {ok, Value}},
			{reply, ok, State#state{wait_queue=Waiting1}}
	end;
handle_call({in, Value}, _From, State=#state{tab=Tab, rear=Rear}) ->
	ets:insert(Tab, {Rear, Value}),
	{reply, ok, State#state{rear=Rear+1}};
handle_call({in_r, Value}, _From, State=#state{tab=Tab, front=Index0, rear=Index0, wait_queue=Waiting0}) ->
	case dequeue_waiting(Waiting0) of
		{undefined, Waiting1} ->
			Index1=Index0-1,
			ets:insert(Tab, {Index1, Value}),
			{reply, ok, State#state{front=Index1, wait_queue=Waiting1}};
		{Tag, ReplyTo, Waiting1} ->
			ReplyTo ! {Tag, {ok, Value}},
			{reply, ok, State#state{wait_queue=Waiting1}}
	end;
handle_call({in_r, Value}, _From, State=#state{tab=Tab, front=Front0}) ->
	Front1=Front0-1,
	ets:insert(Tab, {Front1, Value}),
	{reply, ok, State#state{front=Front1}};
handle_call(out, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(out, _From, State=#state{tab=Tab, front=Front}) ->
	[{Front, Value}]=ets:take(Tab, Front),
	{reply, {ok, Value}, State#state{front=Front+1}};
handle_call({out_wait, ReplyTo, Timeout}, _From={Pid, _}, State=#state{front=Index, rear=Index, wait_queue=Waiting}) ->
	Mon=monitor(process, Pid),
	{reply, {queued, Mon}, State#state{wait_queue=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, Waiting)}};
handle_call({out_wait, _ReplyTo, _Timeout}, _From, State=#state{tab=Tab, front=Front}) ->
	[{Front, Value}]=ets:take(Tab, Front),
	{reply, {ok, Value}, State#state{front=Front+1}};
handle_call(out_r, _From, State=#state{front=Index, rear=Index}) ->
	{reply, empty, State};
handle_call(out_r, _From, State=#state{tab=Tab, rear=Rear0}) ->
	Rear1=Rear0-1,
	[{Rear1, Value}]=ets:take(Tab, Rear1),
	{reply, {ok, Value}, State#state{rear=Rear1}};
handle_call({out_r_wait, ReplyTo, Timeout}, _From={Pid, _}, State=#state{front=Index, rear=Index, wait_queue=Waiting}) ->
	Mon=monitor(process, Pid),
	{reply, {queued, Mon}, State#state{wait_queue=queue:in({Mon, calc_maxts(Timeout), ReplyTo}, Waiting)}};
handle_call({out_r_wait, _ReplyTo, _Timeout}, _From, State=#state{tab=Tab, rear=Rear0}) ->
	Rear1=Rear0-1,
	[{Rear1, Value}]=ets:take(Tab, Rear1),
	{reply, {ok, Value}, State#state{rear=Rear1}};
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
handle_call({cancel, Tag}, _From, State=#state{wait_queue=Waiting0}) ->
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
	{reply, ok, State#state{wait_queue=Waiting1}};
handle_call(size, _From, State=#state{front=Front, rear=Rear}) ->
	{reply, Rear-Front, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

-spec handle_cast(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_cast(_Msg, State) ->
	{noreply, State}.

-spec handle_info(Msg :: term(), State0 :: #state{}) -> {'noreply', State1 :: #state{}}.
handle_info({'DOWN', Mon, process, _Pid, _Reason}, State=#state{wait_queue=Waiting0}) ->
	Waiting1=queue:delete_with(fun ({WaitingMon, _MaxTS, _ReplyTo}) -> Mon=:=WaitingMon end, Waiting0),
	{noreply, State#state{wait_queue=Waiting1}};
handle_info(_Msg, State) ->
	{noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> 'ok'.
terminate(_Reason, _State) ->
	ok.

-spec code_change(OldVsn :: (term() | {'down', term()}), State, Extra :: term()) -> {ok, State}
	when State :: #state{}.
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

-spec dequeue_waiting(Waiting0) -> {Mon, ReplyTo, Waiting1} | {'undefined', Waiting1}
	when WaitItem :: {Mon, MaxTs :: (integer() | 'infinity'), ReplyTo},
	     Waiting0 :: queue:queue(WaitItem),
	     Waiting1 :: queue:queue(WaitItem),
	     Mon :: reference(),
	     ReplyTo :: pid() | reference().
dequeue_waiting(Waiting) ->
	case queue:is_empty(Waiting) of
		true ->
			{undefined, Waiting};
		false ->
			Now=erlang:monotonic_time(millisecond),
			dequeue_waiting(queue:out(Waiting), Now)
	end.

-spec dequeue_waiting({'empty' | {'value', WaitItem}, Waiting}, Now :: integer()) -> {Mon, ReplyTo, Waiting} | {'undefined', Waiting}
	when WaitItem :: {Mon, MaxTs :: (integer() | 'infinity'), ReplyTo},
	     Waiting :: queue:queue(WaitItem),
	     Mon :: reference(),
	     ReplyTo :: pid() | reference().
dequeue_waiting({empty, Waiting}, _Now) ->
	{undefined, Waiting};
dequeue_waiting({{value, {Mon, infinity, ReplyTo}}, Waiting}, _Now) ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting};
dequeue_waiting({{value, {Mon, MaxTS, ReplyTo}}, Waiting}, Now) when MaxTS>Now ->
	demonitor(Mon, [flush]),
	{Mon, ReplyTo, Waiting};
dequeue_waiting({{value, {Mon, _MaxTS, _ReplyTo}}, Waiting}, Now) ->
	demonitor(Mon, [flush]),
	dequeue_waiting(queue:out(Waiting), Now).

-spec calc_maxts(Timeout :: timeout()) -> integer() | 'infinity'.
calc_maxts(infinity) ->
	infinity;
calc_maxts(Timeout) ->
	erlang:monotonic_time(millisecond)+Timeout.
