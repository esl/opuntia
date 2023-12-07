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

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
     {group, throughput_throttle}
    ].

groups() ->
    [
     {throughput_throttle, [parallel],
      [
       run_shaper_with_zero_does_not_shape,
       run_basic_shaper_property,
       run_stateful_server
      ]}
    ].

%%%===================================================================
%%% Overall setup/teardown
%%%===================================================================

init_per_suite(Config) ->
    ct:pal("Online schedulers ~p~n", [erlang:system_info(schedulers_online)]),
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
    Prop = ?FORALL(
              TokensToSpend,
              tokens(),
              begin
                  Shaper = opuntia:new(0),
                  {TimeUs, _LastShaper} = timer:tc(fun run_shaper/2, [Shaper, TokensToSpend]),
                  TimeMs = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
                  0 =< TimeMs
              end),
    run_prop(?FUNCTION_NAME, Prop, 100, 1).

run_basic_shaper_property(_) ->
    Prop = ?FORALL(
              {TokensToSpend, RatePerMs},
              {tokens(), rate()},
              begin
                  Shaper = opuntia:new(RatePerMs),
                  {TimeUs, _LastShaper} = timer:tc(fun run_shaper/2, [Shaper, TokensToSpend]),
                  TimeMs = erlang:convert_time_unit(TimeUs, microsecond, millisecond),
                  MinimumExpected = calculate_accepted_range(TokensToSpend, RatePerMs),
                  Val = (MinimumExpected =< TimeMs),
                  Val orelse ct:pal("to_spend ~p; rate is ~p; took time ~p; expected ~p; result is ~p~n",
                                    [TokensToSpend, RatePerMs, TimeMs, MinimumExpected, Val]),
                  Val
              end),
    run_prop(?FUNCTION_NAME, Prop, 100_000, 256).

%%%===================================================================
%% Server stateful property
%%%===================================================================

run_stateful_server(_) ->
    Prop =
        ?FORALL(Cmds, commands(?MODULE),
            begin
                Config =  #{max_delay => 99999, gc_interval => 1},
                {ok, Pid} = opuntia_srv:start_link(?MODULE, Config),
                {History, State, Res} = run_commands(?MODULE, Cmds, [{server, Pid}]),
                ?WHENFAIL(io:format("H: ~p~nS: ~p~n Res: ~p~n", [History, State, Res]), Res == ok)
            end),
    run_prop(?FUNCTION_NAME, Prop, 1_000, 1).

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
            Now = erlang:monotonic_time(millisecond),
            Rate = get_rate_from_config(Config),
            ets:insert(?MODULE, {{Server, Key}, Rate, Now}),
            true
    end.

postcondition(_State, {call, ?MODULE, reset_shapers, [_Server]}, Res) ->
    ok =:= Res;
postcondition(State, {call, ?MODULE, wait, [Server, Key, Tokens, _Config]}, Res) ->
    do_postcondition(State, wait, Server, Key, Tokens, Res);
postcondition(State, {call, ?MODULE, request_wait, [Server, Key, Tokens, _Config]}, Res) ->
    {reply, Response} = gen_server:wait_response(Res, infinity),
    do_postcondition(State, request_wait, Server, Key, Tokens, Response).

do_postcondition(State, Wait, Server, Key, Tokens, Res) ->
    Now = erlang:monotonic_time(millisecond),
    [{_, Rate, Start}] = ets:lookup(?MODULE, {Server, Key}),
    TokensNowConsumed = tokens_now_consumed(State, Key, Tokens),
    MinimumExpected = calculate_accepted_range(TokensNowConsumed, Rate),
    Duration = Now - Start,
    ct:pal("Res ~p, Value ~p, wait ~p~n", [Res, Duration, Wait]),
    continue =:= Res andalso MinimumExpected =< Duration.

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

get_rate_from_config(N) when is_integer(N), N >= 0 ->
    N;
get_rate_from_config(Config) when is_function(Config, 0) ->
    Config().

%% Limit the number of keys to only a hundred, to make tests smaller
key() ->
    elements([ integer_to_binary(N) || N <- lists:seq(1, 100) ]).

tokens() ->
    integer(1, 9999).

config() ->
    union([0, rate(), function(0, rate())]).

rate() ->
    integer(1, 9999).

%%%===================================================================
%% Helpers
%%%===================================================================
calculate_accepted_range(_, 0) ->
    0;
calculate_accepted_range(TokensToSpend, RatePerMs) when TokensToSpend =< RatePerMs ->
    0;
calculate_accepted_range(TokensToSpend, RatePerMs) ->
    ExactMillisecondsFloat = TokensToSpend / RatePerMs,
    floor(ExactMillisecondsFloat).

run_shaper(_Shaper, TokensLeft) when TokensLeft =< 0 ->
    ok;
run_shaper(Shaper, TokensLeft) ->
    TokensConsumed = rand:uniform(TokensLeft),
    {NewShaper, DelayMs} = opuntia:update(Shaper, TokensConsumed),
    timer:sleep(DelayMs),
    run_shaper(NewShaper, TokensLeft - TokensConsumed).

run_prop(PropName, Property, NumTests, WorkersPerScheduler) ->
    Opts = [quiet, noshrink, long_result, {start_size, 2}, {numtests, NumTests},
            {numworkers, WorkersPerScheduler * erlang:system_info(schedulers_online)}],
    case proper:quickcheck(proper:conjunction([{PropName, Property}]), Opts) of
        true -> ok;
        Res -> ct:fail(Res)
    end.
