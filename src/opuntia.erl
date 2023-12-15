%% @doc `opuntia', traffic shapers for Erlang and Elixir
%%
%% This module implements the token-bucket traffic-shaping algorithm.
%%
%% The rate is given in tokens per time unit and a bucket size.
%%
%% The delay is always returned in milliseconds unit,
%% as this is the unit receives and timers use in the BEAM.
%% @end
-module(opuntia).

-export([new/1, update/2]).

-include("opuntia.hrl").

-type delay() :: non_neg_integer().
%% Number of milliseconds that is advise to wait after a shaping update.

-type rate() :: non_neg_integer().
%% Number of tokens accepted per `time_unit'.

-type time_unit() :: second | millisecond | microsecond.
%% Supported shaping time units: `second', `millisecond', `microsecond'.

-type bucket_size() :: non_neg_integer().
%% Maximum capacity of the bucket regardless of how much time it passes.

-type tokens() :: non_neg_integer().
%% Unit element the shaper consumes, for example bytes or requests.

-type shape() :: 0 | {bucket_size(), rate(), time_unit()}.
%% Zero, or a rate-per-time-unit and a maximum number of tokens. See `new/1' for more details.

-type shaper() :: none | #token_bucket_shaper{}.
%% Shaper type

-export_type([shaper/0, shape/0, tokens/0, time_unit/0, delay/0]).

-define(NON_NEG_INT(N), (is_integer(N) andalso N > 0)).
-define(TU(T), (second =:= T orelse millisecond =:= T orelse microsecond =:= T)).

%% @doc Creates a new shaper according to the configuration.
%%
%% If zero is given, no shaper in created and any update action will always return zero delay;
%% to configure a shaper it will need a `{MaximumTokens, Rate, TimeUnit}', where
%% <ul>
%% <li>`TimeUnit' is the time unit of measurement, that is, `second', `millisecond', or `microsecond'.</li>
%% <li>`Rate' is the number of tokens per `TimeUnit' the bucket will grow with.</li>
%% <li>`MaximumTokens' is the maximum number of tokens the bucket can grow.</li>
%% </ul>
%%
%% So, for example, if we configure `{60000, 10, millisecond}', it means that the bucket will
%% allow `10' tokens per `millisecond', up to 60000 tokens so regardless of how long it is left
%% unused to charge, it will never charge further than 60000 tokens.
%%
%% Note that shapers start unloaded, that is, without available tokens.
-spec new(shape()) -> shaper().
new(0) ->
    none;
new({MaximumTokens, Rate, TimeUnit})
  when ?NON_NEG_INT(MaximumTokens), ?NON_NEG_INT(Rate), ?TU(TimeUnit) ->
    #token_bucket_shaper{shape = {MaximumTokens, Rate, TimeUnit},
                         available_tokens = 0,
                         last_update = erlang:monotonic_time(TimeUnit)}.

%% @doc Update shaper and return possible waiting time.
%%
%% This function takes the current shaper state, and the number of tokens that have been consumed,
%% and returns a tuple containing the new shaper state, and a possibly non-zero number of
%% unit times to wait if more tokens that the shaper allows were consumed.
-spec update(shaper(), tokens()) -> {shaper(), delay()}.
update(none, _TokensNowUsed) ->
    {none, 0};
update(#token_bucket_shaper{shape = {MaximumTokens, Rate, TimeUnit},
                            available_tokens = LastAvailableTokens,
                            last_update = LastUpdate} = Shaper, TokensNowUsed) ->

    %% Time since last shape update
    Now = erlang:monotonic_time(TimeUnit),
    TimeSinceLastUpdate = Now - LastUpdate,

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
    MaybeDelay = TokensOverused / Rate,

    %% We penalise rounding up, the most important contract is that rate will never exceed that
    %% requested, but the same way timeouts in Erlang promise not to arrive any time earlier but
    %% don't promise at what time in the future they would arrive, nor we promise any upper bound
    %% to the limits of the shaper delay.
    RoundedDelay = ceil(MaybeDelay),

    NewShaper = Shaper#token_bucket_shaper{available_tokens = TokensAvailable,
                                           last_update = Now + RoundedDelay + 1},
    {NewShaper, RoundedDelay}.
