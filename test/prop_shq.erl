-module(prop_shq).

-include_lib("proper/include/proper.hrl").

prop_in_peek_out() ->
	?FORALL(
		Items,
		non_empty(list(term())),
		do_in_peek_out(in, out, Items, Items)
	).

prop_in_r_peek_r_out_r() ->
	?FORALL(
		Items,
		non_empty(list(term())),
		do_in_peek_out(in_r, out_r, Items, Items)
	).

prop_in_peek_r_out_r() ->
	?FORALL(
		Items,
		non_empty(list(term())),
		do_in_peek_out(in, out_r, Items, lists:reverse(Items))
	).

prop_in_r_peek_out() ->
	?FORALL(
		Items,
		non_empty(list(term())),
		do_in_peek_out(in_r, out, Items, lists:reverse(Items))
	).

do_in_peek_out(InOp, OutOp, InItems, OutItems) ->
	PeekOp=get_peekop(OutOp),

	{ok, Pid}=shq:start_link(#{}),

	_=[ok=shq:InOp(Pid, I) || I <- InItems],

	Res=shq:size(Pid)=:=length(InItems)
	andalso lists:all(
		fun (Exp) ->
			{ok, Exp}=:=shq:PeekOp(Pid)
			andalso {ok, Exp}=:=shq:OutOp(Pid)
		end,
		OutItems
	)
	andalso empty=:=shq:PeekOp(Pid)
	andalso empty=:=shq:OutOp(Pid)
	andalso 0=:=shq:size(Pid),

	ok=shq:stop(Pid),

	Res.

prop_lim_in_out() ->
	?FORALL(
		Lim,
		non_neg_integer(),
		do_lim_in_out(Lim, in, out)
	).

prop_lim_in_r_out_r() ->
	?FORALL(
		Lim,
		non_neg_integer(),
		do_lim_in_out(Lim, in_r, out_r)
	).

prop_lim_in_out_r() ->
	?FORALL(
		Lim,
		non_neg_integer(),
		do_lim_in_out(Lim, in, out_r)
	).

prop_lim_in_r_out() ->
	?FORALL(
		Lim,
		non_neg_integer(),
		do_lim_in_out(Lim, in_r, out)
	).

do_lim_in_out(Lim, InOp, OutOp) ->
	{ok, Pid}=shq:start_link(#{max => Lim}),

	_=[ok=shq:InOp(Pid, I) || I <- lists:seq(1, Lim)],

	Res1=shq:size(Pid)=:=Lim
	andalso full=:=shq:InOp(Pid, test),

	_=shq:peek(Pid),
	Res2=full=:=shq:InOp(Pid, test),
	_=shq:peek_r(Pid),
	Res3=full=:=shq:InOp(Pid, test),

	Res4=case shq:OutOp(Pid) of
		empty ->
			Lim=:=0
			andalso full=:=shq:InOp(Pid, test);
		{ok, _} ->
			Lim>=0
			andalso ok=:=shq:InOp(Pid, test)
			andalso full=:=shq:InOp(Pid, test)
	end,

	ok=shq:stop(Pid),

	Res1 andalso Res2 andalso Res3 andalso Res4.

prop_ops() ->
	?FORALL(
		{Lim0, Init, Ops},
		{oneof([non_neg_integer(), infinity]), list(term()), list(oneof([{in, term()}, {in_r, term()}, out, out_r, peek, peek_r, size]))},
		begin
			Lim=if
				Lim0=:=infinity ->
					infinity;
				true ->
					max(Lim0, length(Init))
			end,

			{ok, Pid}=shq:start_link(#{max => Lim}),

			_=[ok=shq:in(Pid, I) || I <- Init],

			Rec=lists:foldl(
				fun
					({in, V}, Acc) when Lim=:=infinity; length(Acc)<Lim ->
						ok=shq:in(Pid, V),
						Acc++[V];
					({in, V}, Acc) ->
						full=shq:in(Pid, V),
						Acc;
					({in_r, V}, Acc) when Lim=:=inifinty; length(Acc)<Lim ->
						ok=shq:in_r(Pid, V),
						[V|Acc];
					({in_r, V}, Acc) ->
						full=shq:in_r(Pid, V),
						Acc;
					(peek, Acc=[]) ->
						empty=shq:peek(Pid),
						Acc;
					(peek, Acc=[V|_]) ->
						{ok, V}=shq:peek(Pid),
						Acc;
					(peek_r, Acc=[]) ->
						empty=shq:peek_r(Pid),
						Acc;
					(peek_r, Acc) ->
						V=lists:last(Acc),
						{ok, V}=shq:peek_r(Pid),
						Acc;
					(out, Acc=[]) ->
						empty=shq:out(Pid),
						Acc;
					(out, [V|Acc]) ->
						{ok, V}=shq:out(Pid),
						Acc;
					(out_r, Acc=[]) ->
						empty=shq:out_r(Pid),
						Acc;
					(out_r, Acc) ->
						V=lists:last(Acc),
						{ok, V}=shq:out_r(Pid),
						lists:droplast(Acc);
					(size, Acc) ->
						true=length(Acc)=:=shq:size(Pid),
						Acc
				end,
				Init,
				Ops
			),

			Res1=shq:size(Pid)=:=length(Rec),

			Res2=lists:all(
				fun
					(Exp) ->
						{ok, Exp}=:=shq:peek(Pid)
						andalso {ok, Exp}=:=shq:out(Pid)
				end,
				Rec
			),

			ok=shq:stop(Pid),

			Res1 andalso Res2
		end
	).

get_peekop(out) ->
	peek;
get_peekop(out_r) ->
	peek_r.
	