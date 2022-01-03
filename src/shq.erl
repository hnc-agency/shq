-module(shq).

-behavior(gen_server).

-export([start/0, start/1]).
-export([start_link/0, start_link/1]).
-export([start_monitor/0, start_monitor/1]).
-export([stop/1, stop/3]).
-export([in/2, in_r/2]).
-export([out/1, out_r/1]).
-export([peek/1, peek_r/1]).
-export([size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state,
	{
		tab :: ets:tid(),
		front=0 :: integer(),
		rear=0 :: integer()
	}
).

-type server_name() :: {'local', Name :: atom()}
		     | {'global', GlobalName :: term()}
		     | {'via', Via :: module(), ViaName :: term()}.

-type server_ref() :: pid()
		    | Name :: atom()
		    | {Node :: node(), Name :: atom()}
		    | {'global', GlobalName :: term()}
		    | {'via', Via :: module(), ViaName :: term()}.

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
	gen_server:cast(ServerRef, {in, Value}).

-spec in_r(ServerRef :: server_ref(), Value :: term()) -> 'ok'.
in_r(ServerRef, Value) ->
	gen_server:cast(ServerRef, {in_r, Value}).

-spec out(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out(ServerRef) ->
	gen_server:call(ServerRef, out, infinity).

-spec out_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
out_r(ServerRef) ->
	gen_server:call(ServerRef, out_r, infinity).

-spec peek(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
peek(ServerRef) ->
	gen_server:call(ServerRef, peek, infinity).

-spec peek_r(ServerRef :: server_ref()) -> {'ok', Value :: term()} | 'empty'.
peek_r(ServerRef) ->
	gen_server:call(ServerRef, peek_r, infinity).

-spec size(ServerRef :: server_ref()) -> Size :: non_neg_integer().
size(ServerRef) ->
	gen_server:call(ServerRef, size, infinity).

init([]) ->
	Tab=ets:new(?MODULE, [protected, set]),
	{ok, #state{tab=Tab}}.

handle_call(out, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(out, _From, State=#state{tab=Tab, front=KF}) ->
	[{KF, V}]=ets:take(Tab, KF),
	{reply, {ok, V}, State#state{front=KF+1}};
handle_call(out_r, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(out_r, _From, State=#state{tab=Tab, rear=KR0}) ->
	KR1=KR0-1,
	[{KR1, V}]=ets:take(Tab, KR1),
	{reply, {ok, V}, State#state{rear=KR1}};
handle_call(peek, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(peek, _From, State=#state{tab=Tab, front=KF}) ->
	[{KF, V}]=ets:lookup(Tab, KF),
	{reply, {ok, V}, State};
handle_call(peek_r, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(peek_r, _From, State=#state{tab=Tab, rear=KR0}) ->
	KR1=KR0-1,
	[{KR1, V}]=ets:lookup(Tab, KR1),
	{reply, {ok, V}, State};
handle_call(size, _From, State=#state{front=KF, rear=KR}) ->
	{reply, KR-KF, State};
handle_call(_Msg, _From, State) ->
	{noreply, State}.

handle_cast({in, V}, State=#state{tab=Tab, rear=KR}) ->
	ets:insert(Tab, {KR, V}),
	{noreply, State#state{rear=KR+1}};
handle_cast({in_r, V}, State=#state{tab=Tab, front=KF0}) ->
	KF1=KF0-1,
	ets:insert(Tab, {KF1, V}),
	{noreply, State#state{front=KF1}};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
