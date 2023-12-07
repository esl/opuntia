%% @doc `opuntia', traffic shapers for Erlang and Elixir
%%
%% This module implements the token bucket traffic shaping algorithm.
%% The time unit of measurement is millisecond, as this is the unit receives and timers use in the BEAM.
%% @end
-module(opuntia).

-export([new/1, update/2]).

-include("opuntia.hrl").

-type timestamp() :: number().
-type tokens() :: non_neg_integer().
-type delay() :: non_neg_integer().
-type rate() :: non_neg_integer().
-type shaper() :: #token_bucket{} | none.

-export_type([tokens/0, timestamp/0, delay/0, rate/0, shaper/0]).

-spec new(rate()) -> shaper().
new(0) ->
    none;
new(MaxRatePerMs) ->
    #token_bucket{rate = MaxRatePerMs,
                  available_tokens = MaxRatePerMs,
                  last_update = erlang:monotonic_time(millisecond)}.

%% @doc Update shaper and return possible waiting time.
%%
%% This function takes the current shaper state, and the number of tokens that have been consumed,
%% and returns a tuple containing the new shaper state, and a possibly non-zero number of
%% unit times to wait if more tokens that the shaper allows were consumed.
-spec update(shaper(), tokens()) -> {shaper(), rate()}.
update(none, _Size) ->
    {none, 0};
update(#token_bucket{rate = MaxRatePerMs,
                     available_tokens = LastAvailableTokens,
                     last_update = LastUpdate} = Shaper, TokensNowUsed) ->
    Now = erlang:monotonic_time(millisecond),
    % How much we might have recovered since last time
    TimeSinceLastUpdate = Now - LastUpdate,
    PossibleTokenGrowth = round(MaxRatePerMs * TimeSinceLastUpdate),
    % Available plus recovered cannot grow higher than the actual rate limit
    ExactlyAvailableNow = min(MaxRatePerMs, LastAvailableTokens + PossibleTokenGrowth),
    % Now check how many tokens are available by substracting how many where used,
    % and how many where overused
    TokensAvailable = max(0, ExactlyAvailableNow - TokensNowUsed),
    TokensOverused = max(0, TokensNowUsed - ExactlyAvailableNow),
    MaybeDelay = TokensOverused / MaxRatePerMs,
    RoundedDelay = floor(MaybeDelay) + 1,
    NewShaper = Shaper#token_bucket{available_tokens = TokensAvailable,
                                    last_update = Now + MaybeDelay},
    {NewShaper, RoundedDelay}.
