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
       run_shaper_with_zero_does_not_shape,
       run_shaper_without_consuming_does_not_delay,
       run_basic_shaper_property,
       run_stateful_server
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
    ct:pal("For shape ~p, requested ~p, expected ~p and duration ~p~n",
           [Shape, Tokens, MinimumExpectedMs, Duration]),
    continue =:= Res andalso MinimumExpectedMs =< Duration.

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

config() ->
    union([shape_for_server(), function(0, shape_for_server())]).

shape_for_server() ->
    ShapeGen = {integer(1, 9999), integer(1, 9999)},
    ?LET({M, N}, ShapeGen,
         begin
             #{bucket_size => max(M, N),
               rate => min(M, N),
               start_full => true}
         end).

shape() ->
    ShapeGen = {integer(1, 99999), integer(1, 99999), boolean()},
    ?LET(Shape, ShapeGen,
         begin
             {M, N, StartFull} = Shape,
             #{bucket_size => max(M, N),
               rate => min(M, N),
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

should_take_in_range(#{rate := Rate, start_full := false}, ToConsume) ->
    ExpectedMs = ToConsume / Rate,
    {ExpectedMs, ExpectedMs + 1};
should_take_in_range(#{bucket_size := MaximumTokens,
                       rate := Rate,
                       start_full := true},
                     ToConsume) ->
    case ToConsume < MaximumTokens of
        true -> {0, 0};
        false ->
            ToThrottle = ToConsume - MaximumTokens,
            ExpectedMs = ToThrottle / Rate,
            {ExpectedMs, ExpectedMs + 1}
    end.

run_shaper(Shape, ToConsume) ->
    Now = erlang:monotonic_time(),
    Shaper = opuntia:create(Shape, Now),
    run_shaper(Shaper, Now, [], 0, ToConsume).

run_shaper(Shaper, _, History, AccumulatedDelay, 0) ->
    {Shaper, lists:reverse(History), AccumulatedDelay};
run_shaper(Shaper, Now, History, AccumulatedDelay, TokensLeft) ->
    %% Uniform distributes in [1, N], and we want [0, N], so we generate [1, N+1] and subtract 1
    ConsumeNow = rand:uniform(TokensLeft + 1) - 1,
    {NewShaper, DelayMs} = opuntia:calculate(Shaper, ConsumeNow, Now),
    true = DelayMs >= 0,
    run_shaper(NewShaper, Now, [{ConsumeNow, DelayMs} | History], AccumulatedDelay + DelayMs, TokensLeft - ConsumeNow).

run_prop(PropName, Property, NumTests, WorkersPerScheduler) ->
    Opts = [quiet, noshrink, {start_size, 1}, {numtests, NumTests},
            {numworkers, WorkersPerScheduler * erlang:system_info(schedulers_online)}],
    Res = proper:quickcheck(proper:conjunction([{PropName, Property}]), Opts),
    ?assertEqual(true, Res).
