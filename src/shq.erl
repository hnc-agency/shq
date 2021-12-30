-module(shq).

-behavior(gen_server).

-export([start/0, start/1]).
-export([start_link/0, start_link/1]).
-export([start_monitor/0, start_monitor/1]).
-export([stop/1, stop/3]).
-export([in/2, in_r/2]).
-export([out/1, out_r/1]).
-export([size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {tab, front=0, rear=0}).

start() ->
	gen_server:start(?MODULE, [], []).

start(ServerName) ->
	gen_server:start(ServerName, ?MODULE, [], []).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

start_link(ServerName) ->
	gen_server:start_link(ServerName, ?MODULE, [], []).

start_monitor() ->
	gen_server:start_monitor(?MODULE, [], []).

start_monitor(ServerName) ->
	gen_server:start_monitor(ServerName, ?MODULE, [], []).

stop(ServerRef) ->
	gen_server:stop(ServerRef).

stop(ServerRef, Reason, Timeout) ->
	gen_server:stop(ServerRef, Reason, Timeout).

in(ServerRef, Value) ->
	gen_server:cast(ServerRef, {in, Value}).

in_r(ServerRef, Value) ->
	gen_server:cast(ServerRef, {in_r, Value}).

out(ServerRef) ->
	gen_server:call(ServerRef, out, infinity).

out_r(ServerRef) ->
	gen_server:call(ServerRef, out_r, infinity).

size(ServerRef) ->
	gen_server:call(ServerRef, size, infinity).

init([]) ->
	Tab=ets:new(?MODULE, [public, set]),
	{ok, #state{tab=Tab}}.

handle_call(out, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(out_r, _From, State=#state{front=K, rear=K}) ->
	{reply, empty, State};
handle_call(out, _From, State=#state{tab=Tab, front=KF}) ->
	[{KF, V}]=ets:take(Tab, KF),
	{reply, {ok, V}, State#state{front=KF+1}};
handle_call(out_r, _From, State=#state{tab=Tab, rear=KR0}) ->
	KR1=KR0-1,
	[{KR1, V}]=ets:take(Tab, KR1),
	{reply, {ok, V}, State#state{rear=KR1}};
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
