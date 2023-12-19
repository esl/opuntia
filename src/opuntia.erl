%% @doc `opuntia', traffic shapers for Erlang and Elixir
%%
%% This module implements the token-bucket traffic-shaping algorithm.
%%
%% The rate is given in tokens per millisecond and a bucket size.
%% Resolution is in native unit times as described by `erlang:monotonic_time/0'.
%%
%% The delay is always returned in milliseconds unit,
%% as this is the unit receives and timers use in the BEAM.
%% @end
-module(opuntia).

-export([new/1, update/2]).

-ifdef(TEST).
-export([create/2, calculate/3]).
-else.
-compile({inline, [create/2, calculate/3, convert_native_to_ms/1]}).
-endif.

-include("opuntia.hrl").

-type delay() :: non_neg_integer().
%% Number of milliseconds that is advise to wait after a shaping update.

-type rate() :: non_neg_integer().
%% Number of tokens accepted per millisecond.

-type bucket_size() :: non_neg_integer().
%% Maximum capacity of the bucket regardless of how much time passes.

-type tokens() :: non_neg_integer().
%% Unit element the shaper consumes, for example bytes or requests.

-type config() :: 0 | #{bucket_size := bucket_size(),
                        rate := rate(),
                        start_full := boolean()}.
-type shape() :: {bucket_size(), rate()}.
%% See `new/1' for more details.

-type shaper() :: none | #token_bucket_shaper{}.
%% Shaper type

-export_type([shaper/0, shape/0, tokens/0, bucket_size/0, rate/0, delay/0]).

-define(NON_NEG_INT(N), (is_integer(N) andalso N > 0)).

%% @doc Creates a new shaper according to the configuration.
%%
%% If zero is given, no shaper in created and any update action will always return zero delay;
%% to configure a shaper it will need a
%% ```
%% #{bucket_size => MaximumTokens, rate => Rate, start_full => Boolean},
%% '''
%% where
%% <ul>
%%  <li>`Rate' is the number of tokens per millisecond the bucket will grow with.</li>
%%  <li>`MaximumTokens' is the maximum number of tokens the bucket can grow.</li>
%%  <li>`StartFull' indicates if the shaper starts with the bucket full, or empty if not.</li>
%% </ul>
%%
%% So, for example, if we configure a shaper with the following:
%% ```
%% #{bucket_size => 60000, rate => 10, start_full => true}
%% '''
%% it means that the bucket will
%% allow `10' tokens per `millisecond', up to 60000 tokens, regardless of how long it is left
%% unused to charge: it will never charge further than 60000 tokens.
-spec new(config()) -> shaper().
new(0) ->
    none;
new(Shape) ->
    create(Shape, erlang:monotonic_time()).

%% @doc Update shaper and return possible waiting time.
%%
%% This function takes the current shaper state, and the number of tokens that have been consumed,
%% and returns a tuple containing the new shaper state, and a possibly non-zero number of
%% unit times to wait if more tokens that the shaper allows were consumed.
-spec update(shaper(), tokens()) -> {shaper(), delay()}.
update(none, _TokensNowUsed) ->
    {none, 0};
update(Shaper, TokensNowUsed) ->
    calculate(Shaper, TokensNowUsed, erlang:monotonic_time()).

%% Helpers
-spec create(config(), integer()) -> shaper().
create(0, _) ->
    none;
create(#{bucket_size := MaximumTokens,
         rate := Rate,
         start_full := StartFull},
       NativeNow)
  when ?NON_NEG_INT(MaximumTokens),
       ?NON_NEG_INT(Rate),
       MaximumTokens >= Rate,
       is_boolean(StartFull) ->
    AvailableAtStart = case StartFull of
                           true -> MaximumTokens;
                           false -> 0
                       end,
    #token_bucket_shaper{shape = {MaximumTokens, Rate},
                         available_tokens = AvailableAtStart,
                         last_update = NativeNow,
                         debt = 0.0}.

-spec calculate(shaper(), tokens(), integer()) -> {shaper(), delay()}.
calculate(none, _, _) ->
    {none, 0};
calculate(Shaper, 0, _) ->
    {Shaper, 0};
calculate(#token_bucket_shaper{shape = {MaximumTokens, Rate},
                               available_tokens = LastAvailableTokens,
                               last_update = NativeLastUpdate,
                               debt = LastDebt} = Shaper, TokensNowUsed, NativeNow) ->
    NativeTimeSinceLastUpdate = NativeNow - NativeLastUpdate,

    %% This is now a float and so will all below be, to preserve best rounding errors possible
    TimeSinceLastUpdate = convert_native_to_ms(NativeTimeSinceLastUpdate) + LastDebt,

    %% How much we might have recovered since last time
    AvailableAtGrowthRate = Rate * TimeSinceLastUpdate,
    UnboundedTokenGrowth = LastAvailableTokens + AvailableAtGrowthRate,

    %% Real recovery cannot grow higher than the actual rate in the window frame
    ExactlyAvailableNow = min(MaximumTokens, UnboundedTokenGrowth),

    %% How many are available after using TokensNowUsed can't be smaller than zero
    TokensAvailable = max(0, ExactlyAvailableNow - TokensNowUsed),

    %% How many tokens I overused might be zero if I didn't overused any
    TokensOverused = max(0, TokensNowUsed - ExactlyAvailableNow),

    %% And then MaybeDelay will be zero if TokensOverused was zero
    MaybeDelayMs = TokensOverused / Rate,

    %% We penalise rounding up, the most important contract is that rate will never exceed that
    %% requested, but the same way timeouts in Erlang promise not to arrive any time earlier but
    %% don't promise at what time in the future they would arrive, nor we promise any upper bound
    %% to the limits of the shaper delay.
    RoundedDelayMs = ceil(MaybeDelayMs),

    NewShaper = Shaper#token_bucket_shaper{available_tokens = TokensAvailable,
                                           last_update = NativeNow + RoundedDelayMs,
                                           debt = RoundedDelayMs - MaybeDelayMs},
    {NewShaper, RoundedDelayMs}.

%% Avoid rounding errors by using floats and float division,
%% erlang:convert_native_to_ms works only with integers
-spec convert_native_to_ms(number()) -> float().
convert_native_to_ms(Time) ->
    time_unit_multiplier(millisecond) * Time / time_unit_multiplier(native).

-compile({inline, [time_unit_multiplier/1]}).
time_unit_multiplier(native) ->
    erts_internal:time_unit();
time_unit_multiplier(millisecond) ->
    1000.
