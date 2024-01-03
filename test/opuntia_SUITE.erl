-module(opuntia_SUITE).

-compile([export_all, nowarn_export_all]).

%% API
-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
     {group, throughput_throttle}
    ].

groups() ->
    [
     {throughput_throttle, [sequence],
      [
       simple_test_no_delay_is_needed,
       run_shaper_with_zero_does_not_shape,
       run_shaper_without_consuming_does_not_delay,
       run_basic_shaper_property,
       run_stateful_server
      ]},
     %% This group is purposefully left out because it is too slow to run on CI,
     %% and uses a `timer:tc/4` only available since OTP26
     %% Here for the record, can be enabled locally and checked
     {delays, [sequence],
     [
       run_with_delays
     ]}
    ].

%%%===================================================================
%%% Overall setup/teardown
%%%===================================================================

init_per_suite(Config) ->
    application:ensure_all_started(telemetry),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_Groupname, _Config) ->
    ok.

init_per_testcase(run_stateful_server, Config) ->
    [{{pid, run_stateful_server}, spawn(fun keep_table/0)} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(run_stateful_server, Config) ->
    Pid = ?config({pid, run_stateful_server}, Config),
    Pid ! clean_table;
end_per_testcase(_TestCase, _Config) ->
    ok.

keep_table() ->
    ets:new(?MODULE, [named_table, set, public,
                      {write_concurrency, true}, {read_concurrency, true}]),
    receive Msg -> Msg end.

%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================

simple_test_no_delay_is_needed(_) ->
    Units = [second, millisecond, microsecond, nanosecond, native],
    [ simple_test_no_delay_is_needed_for_unit(Unit) || Unit <- Units ].

simple_test_no_delay_is_needed_for_unit(Unit) ->
    FoldFun = fun(N, ShIn) -> {ShOut, 0} = opuntia:update(ShIn, N), ShOut end,
    Config = #{bucket_size => 100000, rate => 10000, time_unit => Unit, start_full => true},
    Shaper = opuntia:new(Config),
    lists:foldl(FoldFun, Shaper, lists:duplicate(10000, 1)).

run_shaper_with_zero_does_not_shape(_) ->
    Prop = ?FORALL(TokensToSpend, tokens(),
              begin
                  {_, _, CalculatedDelay} = run_shaper(0, TokensToSpend),
                  Val = 0 =:= CalculatedDelay,
                  success_or_log_and_return(Val, "one shaper took ~p", [CalculatedDelay])
              end),
    run_prop(?FUNCTION_NAME, Prop, 1000, 2).

run_shaper_without_consuming_does_not_delay(_) ->
    Prop = ?FORALL(Shape, shape(),
              begin
                  {_, Delay} = opuntia:update(opuntia:new(Shape), 0),
                  Val = 0 =:= Delay,
                  success_or_log_and_return(Val, "shape of ~p was actually requested", [Delay])
              end),
    run_prop(?FUNCTION_NAME, Prop, 1000, 2).

run_with_delays(_) ->
    S = "ToConsume ~p, Shape ~p, TimeItTook ~p, CalculatedDelay ~p ms, in range [~p, ~p]ms, ~nLastShaper ~p,~nHistory ~p",
    Prop = ?FORALL(
              {TokensToSpend, Shape},
              {tokens(), shape()},
              begin
                  Shaper = opuntia:new(Shape),
                  {TimeItTookUs, {LastShaper, History, CalculatedDelay}} =
                    timer:tc(fun run_with_sleeps/2, [Shaper, TokensToSpend], native),
                  TimeItTookMs = opuntia:convert_time_unit(TimeItTookUs, native, millisecond),
                  {CannotBeFasterThan, CannotBeSlowerThan} = should_take_in_range(Shape, TokensToSpend),
                  AdjustCannotBeSlowerThan = CannotBeSlowerThan + 10,
                  Val = value_in_range(TimeItTookMs, CannotBeFasterThan, AdjustCannotBeSlowerThan),
                  P = [TokensToSpend, Shape, TimeItTookMs, CalculatedDelay, CannotBeFasterThan,
                       AdjustCannotBeSlowerThan, LastShaper, History],
                  success_or_log_and_return(Val andalso is_integer(CalculatedDelay), S, P)
              end),
    run_prop(?FUNCTION_NAME, Prop, 100, 1).

run_basic_shaper_property(_) ->
    S = "ToConsume ~p, Shape ~p, CalculatedDelay ~p ms, in range [~p, ~p]ms, ~nLastShaper ~p,~nHistory ~p",
    Prop = ?FORALL(
              {TokensToSpend, Shape},
              {tokens(), shape()},
              begin
                  {LastShaper, History, CalculatedDelay} = run_shaper(Shape, TokensToSpend),
                  {CannotBeFasterThan, CannotBeSlowerThan} = should_take_in_range(Shape, TokensToSpend),
                  Val = value_in_range(CalculatedDelay, CannotBeFasterThan, CannotBeSlowerThan),
                  P = [TokensToSpend, Shape, CalculatedDelay, CannotBeFasterThan,
                       CannotBeSlowerThan, LastShaper, History],
                  success_or_log_and_return(Val andalso is_integer(CalculatedDelay), S, P)
              end),
    run_prop(?FUNCTION_NAME, Prop, 10000, 12).

value_in_range(Val, Min, Max) ->
    (Min =< Val) andalso (Val =< Max).

%%%===================================================================
%% Server stateful property
%%%===================================================================

run_stateful_server(_) ->
    Prop =
        ?FORALL(Cmds, commands(?MODULE),
            begin
                Config =  #{max_delay => 99999, cleanup_interval => 1},
                {ok, Pid} = opuntia_srv:start_link(?MODULE, Config),
                {History, State, Res} = run_commands(?MODULE, Cmds, [{server, Pid}]),
                ?WHENFAIL(io:format("H: ~p~nS: ~p~n Res: ~p~n", [History, State, Res]), Res == ok)
            end),
    run_prop(?FUNCTION_NAME, Prop, 10000, 12).

command(_State) ->
    oneof([
           {call, ?MODULE, wait, [{var, server}, key(), tokens(), config()]},
           {call, ?MODULE, request_wait, [{var, server}, key(), tokens(), config()]},
           {call, ?MODULE, reset_shapers, [{var, server}]}
          ]).


initial_state() ->
    #{}.

precondition(_State, {call, ?MODULE, reset_shapers, [_Server]}) ->
    true;
precondition(State, {call, ?MODULE, Wait, [Server, Key, _Tokens, Config]})
  when Wait =:= wait; Wait =:= request_wait ->
    case maps:is_key(Key, State) of
        true -> %% We already know when this one started
            true;
        false -> %% Track start for this key
            Shape = get_shape_from_config(Config),
            Now = erlang:monotonic_time(millisecond),
            ets:insert(?MODULE, {{Server, Key}, Shape, Now}),
            true
    end.

postcondition(_State, {call, ?MODULE, reset_shapers, [_Server]}, Res) ->
    ok =:= Res;
postcondition(State, {call, ?MODULE, wait, [Server, Key, Tokens, _Config]}, Res) ->
    do_postcondition(State, Server, Key, Tokens, Res);
postcondition(State, {call, ?MODULE, request_wait, [Server, Key, Tokens, _Config]}, Res) ->
    {reply, Response} = gen_server:wait_response(Res, infinity),
    do_postcondition(State, Server, Key, Tokens, Response).

do_postcondition(State, Server, Key, Tokens, Res) ->
    [{_, Shape, Start}] = ets:lookup(?MODULE, {Server, Key}),
    Now = erlang:monotonic_time(millisecond),
    TokensNowConsumed = tokens_now_consumed(State, Key, Tokens),
    {MinimumExpectedMs, _} = should_take_in_range(Shape, TokensNowConsumed),
    Duration = Now - Start,
    S = "For shape ~p, consumed ~p, expected-min-time ~f and it took duration ~B~n",
    P = [{maps:get(rate, Shape), maps:get(time_unit, Shape)}, Tokens, floor(MinimumExpectedMs), Duration],
    Val = continue =:= Res andalso floor(MinimumExpectedMs) =< Duration,
    success_or_log_and_return(Val, S, P).

next_state(_State, _Result, {call, ?MODULE, reset_shapers, [_Server]}) ->
    #{};
next_state(State, _Result, {call, ?MODULE, Wait, [_Server, Key, Tokens, _Config]})
  when Wait =:= wait; Wait =:= request_wait ->
    TokensNowConsumed = tokens_now_consumed(State, Key, Tokens),
    State#{Key => TokensNowConsumed}.

tokens_now_consumed(State, Key, NewTokens) ->
    TokensConsumedSoFar = maps:get(Key, State, 0),
    TokensConsumedSoFar + NewTokens.

wait(Shaper, Key, Tokens, Config) ->
    opuntia_srv:wait(Shaper, Key, Tokens, Config).

request_wait(Shaper, Key, Tokens, Config) ->
    opuntia_srv:request_wait(Shaper, Key, Tokens, Config).

reset_shapers(Shaper) ->
    opuntia_srv:reset_shapers(Shaper).

get_shape_from_config(Config) when is_function(Config, 0) ->
    Config();
get_shape_from_config(Config) ->
    Config.

%% Limit the number of keys to only a hundred, to make tests smaller
key() ->
    elements([ integer_to_binary(N) || N <- lists:seq(1, 100) ]).

tokens() ->
    integer(1, 99999).

time_unit() ->
    oneof([second, millisecond, microsecond, nanosecond, native]).

config() ->
    union([shape_for_server(), function(0, shape_for_server())]).

shape_for_server() ->
    %% server is slower and proper struggles with bigger numbers, not critical
    ShapeGen = {integer(1, 999), integer(1, 999), time_unit(), boolean()},
    let_shape(ShapeGen).

shape() ->
    Int = integer(1, 99999),
    ShapeGen = {Int, Int, time_unit(), boolean()},
    let_shape(ShapeGen).

let_shape(ShapeGen) ->
    ?LET(Shape, ShapeGen,
         begin
             {M, N, TimeUnit, StartFull} = Shape,
             #{bucket_size => max(M, N),
               rate => min(M, N),
               time_unit => TimeUnit,
               start_full => StartFull}
         end).

%%%===================================================================
%% Helpers
%%%===================================================================

success_or_log_and_return(true, _S, _P) ->
    true;
success_or_log_and_return(false, S, P) ->
    ct:pal(S, P),
    false.

should_take_in_range(#{rate := Rate, time_unit := TimeUnit, start_full := false}, ToConsume) ->
    Expected = ToConsume / Rate,
    ExpectedMs = opuntia:convert_time_unit(Expected, TimeUnit, millisecond),
    {ExpectedMs, ceil(ExpectedMs + 1)};
should_take_in_range(#{bucket_size := MaximumTokens,
                       rate := Rate,
                       time_unit := TimeUnit,
                       start_full := true},
                     ToConsume) ->
    case ToConsume < MaximumTokens of
        true -> {0, 1};
        false ->
            ToThrottle = ToConsume - MaximumTokens,
            Expected = ToThrottle / Rate,
            ExpectedMs = opuntia:convert_time_unit(Expected, TimeUnit, millisecond),
            {ExpectedMs, ceil(ExpectedMs + 1)}
    end.

run_with_sleeps(Shaper, ToConsume) ->
    run_with_sleeps(Shaper, [], 0, ToConsume).

run_with_sleeps(Shaper, History, AccumulatedDelay, TokensLeft) when TokensLeft =< 0 ->
    {Shaper, lists:reverse(History), AccumulatedDelay};
run_with_sleeps(Shaper, History, AccumulatedDelay, TokensLeft) ->
    ConsumeNow = rand:uniform(TokensLeft),
    {NewShaper, DelayMs} = opuntia:update(Shaper, ConsumeNow),
    timer:sleep(DelayMs),
    NewEvent = #{consumed => ConsumeNow, proposed_delay => DelayMs, shaper => Shaper},
    NewHistory = [NewEvent | History],
    NewDelay = AccumulatedDelay + DelayMs,
    run_with_sleeps(NewShaper, NewHistory, NewDelay, TokensLeft - ConsumeNow).

run_shaper(Shape, ToConsume) ->
    Shaper = opuntia:create(Shape, 0),
    run_shaper(Shaper, [], 0, ToConsume).

run_shaper(Shaper, History, AccumulatedDelay, 0) ->
    {Shaper, lists:reverse(History), AccumulatedDelay};
run_shaper(Shaper, History, AccumulatedDelay, TokensLeft) ->
    %% Uniform distributes in [1, N], and we want [0, N], so we generate [1, N+1] and subtract 1
    ConsumeNow = rand:uniform(TokensLeft + 1) - 1,
    {NewShaper, DelayMs} = opuntia:calculate(Shaper, ConsumeNow, 0),
    NewEvent = #{consumed => ConsumeNow, proposed_delay => DelayMs, final_shaper => NewShaper},
    NewHistory = [NewEvent | History],
    NewDelay = AccumulatedDelay + DelayMs,
    NewToConsume = TokensLeft - ConsumeNow,
    case is_integer(DelayMs) andalso DelayMs >= 0 of
        true ->
            run_shaper(NewShaper, NewHistory, NewDelay, NewToConsume);
        false ->
            {NewShaper, NewHistory, bad_delay}
    end.

run_prop(PropName, Property, NumTests, WorkersPerScheduler) ->
    Opts = [quiet, noshrink, {start_size, 1}, {numtests, NumTests},
            {numworkers, WorkersPerScheduler * erlang:system_info(schedulers_online)}],
    Res = proper:quickcheck(proper:conjunction([{PropName, Property}]), Opts),
    ?assertEqual(true, Res).
