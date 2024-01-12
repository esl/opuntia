%% @doc `opuntia', traffic shapers for Erlang and Elixir
%%
%% This module implements the token-bucket traffic-shaping algorithm.
%%
%% The rate is given in `t:tokens()' per `t:time_unit()' and a bucket size.
%% Resolution is in native unit times as described by `erlang:monotonic_time/0'.
%%
%% The delay is always returned in milliseconds unit,
%% as this is the unit receives and timers use in the BEAM.
%% @end
-module(opuntia).

-export([new/1, update/2, peek/1]).

-ifdef(TEST).
-export([create/2, calculate/3, convert_time_unit/3]).
-else.
-compile({inline, [create/2, calculate/3, convert_time_unit/3, timediff_in_units/3,
                   unbounded_available_tokens/3, exactly_available_tokens/2, final_state/4]}).
-endif.

-include("opuntia.hrl").

-type delay() :: non_neg_integer().
%% Number of milliseconds that is advise to wait after a shaping update.

-type rate() :: non_neg_integer().
%% Number of tokens accepted per `t:time_unit()'.

-type time_unit() :: second | millisecond | microsecond | nanosecond | native.
%% Supported shaping time units.

-type bucket_size() :: non_neg_integer().
%% Maximum capacity of the bucket regardless of how much time passes.

-type tokens() :: non_neg_integer().
%% Unit element the shaper consumes, for example bytes or requests.

-type config() :: 0 | #{bucket_size := bucket_size(),
                        rate := rate(),
                        time_unit := time_unit(),
                        start_full := boolean()}.
-type shape() :: {bucket_size(), rate(), time_unit()}.
%% See `new/1' for more details.

-type shaper() :: none | #token_bucket_shaper{}.
%% Shaper type

-export_type([shaper/0, shape/0, tokens/0, bucket_size/0, rate/0, time_unit/0, delay/0]).

-define(POS_INTEGER(N), (is_integer(N) andalso N > 0)).
-define(TU(T), (second =:= T orelse
                millisecond =:= T orelse
                microsecond =:= T orelse
                nanosecond =:= T orelse
                native =:= T)).

%% @doc Creates a new shaper according to the configuration.
%%
%% If zero is given, no shaper in created and any update action will always return zero delay;
%% to configure a shaper it will need a config like
%% ```
%% #{bucket_size => MaximumTokens, rate => Rate, time_unit => TimeUnit, start_full => Boolean}
%% '''
%% where
%% <ul>
%%  <li>`TimeUnit' is the time unit of measurement as defined by `t:time_unit()'</li>
%%  <li>`Rate' is the number of tokens per `TimeUnit' the bucket will grow with.</li>
%%  <li>`MaximumTokens' is the maximum number of tokens the bucket can grow.</li>
%%  <li>`StartFull' indicates if the shaper starts with the bucket full, or empty if not.</li>
%% </ul>
%%
%% So, for example, if we configure a shaper with the following:
%% ```
%% #{bucket_size => 60000, rate => 10, time_unit => millisecond, start_full => true}
%% '''
%% it means that the bucket will
%% allow `10' tokens per `millisecond', up to 60000 tokens, regardless of how long it is left
%% unused to charge: it will never charge further than 60000 tokens.
-spec new(config()) -> shaper().
new(0) ->
    none;
new(Shape) ->
    create(Shape, erlang:monotonic_time()).

%% @doc Peek currently available tokens.
-spec peek(shaper()) -> non_neg_integer() | infinity.
peek(none) ->
    infinity;
peek(Shaper) ->
    {NewShaper, _} = calculate(Shaper, 0, erlang:monotonic_time()),
    NewShaper#token_bucket_shaper.available_tokens.

%% @doc Update shaper and return possible waiting time.
%%
%% This function takes the current shaper state, and the number of tokens that have been consumed,
%% and returns a tuple containing the new shaper state, and a possibly non-zero number of
%% unit times to wait if more tokens that the shaper allows were consumed.
-spec update(shaper(), tokens()) -> {shaper(), delay()}.
update(none, _TokensNowUsed) ->
    {none, 0};
update(Shaper, 0) ->
    {Shaper, 0};
update(Shaper, TokensNowUsed) ->
    calculate(Shaper, TokensNowUsed, erlang:monotonic_time()).

%% Helpers
-spec create(config(), integer()) -> shaper().
create(0, _) ->
    none;
create(#{bucket_size := MaximumTokens,
         rate := Rate,
         time_unit := TimeUnit,
         start_full := StartFull},
       NativeNow)
  when ?POS_INTEGER(MaximumTokens),
       ?POS_INTEGER(Rate),
       MaximumTokens >= Rate,
       ?TU(TimeUnit),
       is_boolean(StartFull) ->
    AvailableAtStart = case StartFull of
                           true -> MaximumTokens;
                           false -> 0
                       end,
    #token_bucket_shaper{shape = {MaximumTokens, Rate, TimeUnit},
                         available_tokens = AvailableAtStart,
                         last_update = NativeNow,
                         debt = 0.0}.

-spec calculate(shaper(), tokens(), integer()) -> {shaper(), delay()}.
calculate(none, _, _) ->
    {none, 0};
calculate(#token_bucket_shaper{shape = {MaximumTokens, Rate, TimeUnit},
                               available_tokens = LastAvailableTokens,
                               last_update = NativeLastUpdate,
                               debt = OverPenalisedInUnitsLastTime} = Shaper, TokensNowUsed, NativeNow) ->

    TimeDiffInUnits = timediff_in_units(TimeUnit, NativeLastUpdate, NativeNow),

    UnboundedTokens = unbounded_available_tokens(Rate, LastAvailableTokens, TimeDiffInUnits),

    ExactlyAvailableNow = exactly_available_tokens(MaximumTokens, UnboundedTokens),

    %% How many are available after using TokensNowUsed can't be smaller than zero
    TokensAvailable = max(0, ExactlyAvailableNow - TokensNowUsed),

    %% How many tokens I overused might be zero if I didn't overused any
    TokensOverused = max(0, TokensNowUsed - ExactlyAvailableNow),

    %% And then MaybeDelay will be zero if TokensOverused was zero
    OverUsedRateNow = TokensOverused / Rate,

    NewShaper = Shaper#token_bucket_shaper{available_tokens = TokensAvailable},
    Punish = OverUsedRateNow - OverPenalisedInUnitsLastTime,
    final_state(NewShaper, TimeUnit, Punish, NativeNow).

-spec timediff_in_units(time_unit(), integer(), integer()) -> float().
timediff_in_units(TimeUnit, NativeLastUpdate, NativeNow) ->
    %% Time difference between now and the last update, in native
    NativeTimeSinceLastUpdate = NativeNow - NativeLastUpdate,
    %% This is now a float and so will all below be, to preserve best rounding errors possible
    convert_time_unit(NativeTimeSinceLastUpdate, native, TimeUnit).

%% Unbounded growth is calculated, with float precision, in the configured time units
%%
%% Note that it can be negative, if earlier we have penalised giving a larger `last_update' and now we
%% update even before we have reach the point in time where the previous `last_update' was set
%%
%% If the growth was negative, that means that it has grown the debt instead
-spec unbounded_available_tokens(rate(), tokens(), float()) -> float().
unbounded_available_tokens(Rate, LastAvailableTokens, TimeDiffInUnits) ->
    %% How much we might have recovered since last time
    AvailableAtGrowthRate = Rate * TimeDiffInUnits,
    %% Unbounded growth at rate since the last update
    LastAvailableTokens + AvailableAtGrowthRate.

%% This is the real growth considering the maximum bucket size.
-spec exactly_available_tokens(bucket_size(), float()) -> float().
exactly_available_tokens(MaximumTokens, UnboundedTokens) ->
    %% Real recovery cannot grow higher than the actual rate in the window frame
    ExactlyAvailableNow0 = min(MaximumTokens, UnboundedTokens),
    %% But it can't be negative either which can happen if we were already in debt,
    %% but this is a debt we will pay when we calculate the final punishment in final_state
    max(+0.0, ExactlyAvailableNow0).

%% We penalise rounding up, the most important contract is that rate will never exceed that
%% requested, but the same way timeouts in Erlang promise not to arrive any time earlier but
%% don't promise at what time in the future they would arrive, nor we promise any upper bound
%% to the limits of the shaper delay.
%%
%% Two cases, either:
%%   Punish is positive: even after paying the old debt you incur a new debt again
%%   Punish is negative: I overpenalised you last time, you get off now but with a future bill
final_state(Shaper, TimeUnit, Punish, NativeNow) when Punish >= +0.0 ->
    DelayMs = convert_time_unit(Punish, TimeUnit, millisecond),
    RoundedDelayMs = ceil(DelayMs),
    Debt = RoundedDelayMs - DelayMs,
    DebtInUnits = convert_time_unit(Debt, millisecond, TimeUnit),
    DelayNative = convert_time_unit(RoundedDelayMs, TimeUnit, native),
    RoundedDelayNative = ceil(DelayNative),
    NewShaper = Shaper#token_bucket_shaper{last_update = NativeNow + RoundedDelayNative,
                                           debt = DebtInUnits},
    {NewShaper, RoundedDelayMs};
final_state(Shaper, TimeUnit, Punish, NativeNow) when Punish < +0.0 ->
    DebtInUnits = convert_time_unit(-Punish, millisecond, TimeUnit),
    NewShaper = Shaper#token_bucket_shaper{last_update = NativeNow,
                                           debt = DebtInUnits},
    {NewShaper, 0}.

%% Avoid rounding errors by using floats and float division,
%% erlang:convert_time_unit works only with integers
-spec convert_time_unit(number(), time_unit(), time_unit()) -> float().
convert_time_unit(Time, SameUnit, SameUnit) -> Time;
convert_time_unit(Time, FromUnit, ToUnit) ->
    time_unit_multiplier(ToUnit) * Time / time_unit_multiplier(FromUnit).

-compile({inline, [time_unit_multiplier/1]}).
-spec time_unit_multiplier(time_unit()) -> pos_integer().
time_unit_multiplier(native) ->
    erts_internal:time_unit();
time_unit_multiplier(nanosecond) ->
    1000*1000*1000;
time_unit_multiplier(microsecond) ->
    1000*1000;
time_unit_multiplier(millisecond) ->
    1000;
time_unit_multiplier(second) ->
    1.
