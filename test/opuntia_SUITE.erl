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

all() ->
    [
     {group, throughput_throttle}
    ].

groups() ->
    [
     {throughput_throttle, [parallel],
      [
       run_shaper_with_zero_does_not_shape,
       run_basic_shaper_property
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

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================

run_shaper_with_zero_does_not_shape(_) ->
    Prop = ?FORALL(
              TokensToSpend,
              integer(1, 9999),
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
              {integer(1, 9999), integer(1, 9999)},
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
    run_prop(?FUNCTION_NAME, Prop).

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




run_prop(PropName, Property) ->
    run_prop(PropName, Property, 100_000).

run_prop(PropName, Property, NumTests) ->
    run_prop(PropName, Property, NumTests, 256).

run_prop(PropName, Property, NumTests, WorkersPerScheduler) ->
    Opts = [quiet, noshrink, long_result, {start_size, 2}, {numtests, NumTests},
            {numworkers, WorkersPerScheduler * erlang:system_info(schedulers_online)}],
    case proper:quickcheck(proper:conjunction([{PropName, Property}]), Opts) of
        true -> ok;
        Res -> ct:fail(Res)
    end.
