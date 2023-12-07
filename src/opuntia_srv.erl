%%%-------------------------------------------------------------------
%%% @doc Shared shapers.
%%%-------------------------------------------------------------------
-module(opuntia_srv).

-behaviour(gen_server).

-include("opuntia.hrl").

%% API Function Exports
-export([start_link/2, wait/4, reset_shapers/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% Record definitions
-record(opuntia_state, {
          name :: name(),
          max_delay :: opuntia:delay(), %% Maximum amount of time units to wait
          gc_ttl :: non_neg_integer(), %% How many seconds to store each shaper
          gc_time :: non_neg_integer(), %% How often to run the gc
          gc_ref :: undefined | reference(),
          shapers = #{} :: #{key() := opuntia:shaper()}
         }).
-type opuntia_state() :: #opuntia_state{}.
-type name() :: atom().
-type key() :: term().
-type seconds() :: non_neg_integer().
-type args() :: #{max_delay => opuntia:delay(),
                  gc_interval => seconds(),
                  ttl => seconds()}.
-type maybe_rate() :: fun(() -> opuntia:rate() | opuntia:rate()).

%% API Function Definitions
-spec start_link(name(), args()) -> ignore | {error, _} | {ok, pid()}.
start_link(Name, Args) ->
    gen_server:start_link(?MODULE, {Name, Args}, []).

%% @doc Shapes the caller from executing the action
-spec wait(gen_server:server_ref(), key(), opuntia:tokens(), maybe_rate()) ->
    continue | {error, max_delay_reached}.
wait(Shaper, Key, Tokens, Config) ->
    gen_server:call(Shaper, {wait, Key, Tokens, Config}, infinity).

%% @doc Ask server to forget its shapers
reset_shapers(ProcName) ->
    gen_server:call(ProcName, reset_shapers, infinity).

%% gen_server Function Definitions
-spec init({name(), args()}) -> {ok, opuntia_state()}.
init({Name, Args}) ->
    MaxDelay = maps:get(max_delay, Args, 3000),
    GCInt = timer:seconds(maps:get(gc_interval, Args, 30)),
    GCTTL = maps:get(ttl, Args, 120),
    State = #opuntia_state{name = Name, max_delay = MaxDelay, gc_ttl = GCTTL, gc_time = GCInt},
    {ok, schedule_cleanup(State)}.

handle_call({wait, Key, Tokens, Config}, From,
            #opuntia_state{name = Name, max_delay = MaxDelayMs} = State) ->
    Shaper = find_or_create_shaper(State, Key, Config),
    {UpdatedShaper, Delay} = opuntia:update(Shaper, Tokens),
    NewState = save_shaper(State, Key, UpdatedShaper),
    case Delay of
        0 ->
            Measurements = #{tokens => Tokens},
            Metadata = #{key => Key},
            telemetry:execute([opuntia, wait, continue, Name], Measurements, Metadata),
            {reply, continue, NewState};
        DelayTime when DelayTime =< MaxDelayMs ->
            Measurements = #{delay_time => DelayTime, tokens => Tokens},
            Metadata = #{key => Key},
            telemetry:execute([opuntia, wait, delay, Name], Measurements, Metadata),
            reply_after(DelayTime, From, continue),
            {noreply, NewState};
        _ ->
            Measurements = #{tokens => Tokens},
            Metadata = #{max_delay_time => MaxDelayMs, key => Key},
            telemetry:execute([opuntia, wait, max_delay_reached, Name], Measurements, Metadata),
            {reply, {error, max_delay_reached}, NewState}
    end;
handle_call(reset_shapers, _From, #opuntia_state{name = Name} = State) ->
    telemetry:execute([opuntia, reset_shapers, Name], #{}, #{}),
    {reply, ok, State#opuntia_state{shapers = #{}}};
handle_call(Msg, From, #opuntia_state{name = Name} = State) ->
    telemetry:execute([opuntia, unknown_request, Name], #{value => 1}, #{msg => Msg, from => From, type => call}),
    {reply, unknown_request, State}.

handle_cast(Msg, #opuntia_state{name = Name} = State) ->
    telemetry:execute([opuntia, unknown_request, Name], #{value => 1}, #{msg => Msg, type => cast}),
    {noreply, State}.

handle_info({timeout, TRef, cleanup}, #opuntia_state{gc_ref = TRef} = State) ->
    {noreply, schedule_cleanup(cleanup(State))};
handle_info(Info, #opuntia_state{name = Name} = State) ->
    telemetry:execute([opuntia, unknown_request, Name], #{value => 1}, #{msg => Info, type => info}),
    {noreply, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

find_or_create_shaper(#opuntia_state{shapers = Shapers}, Key, Config) ->
    case Shapers of
        #{Key := Shaper} -> Shaper;
        _ -> create_new_from_config(Config)
    end.

create_new_from_config(N) when is_integer(N), N >= 0 ->
    opuntia:new(N);
create_new_from_config(Config) when is_function(Config, 0) ->
    create_new_from_config(Config()).

save_shaper(#opuntia_state{shapers = Shapers} = State, Key, Shaper) ->
    State#opuntia_state{shapers = maps:put(Key, Shaper, Shapers)}.

cleanup(State = #opuntia_state{name = Name, shapers = Shapers, gc_ttl = TTL}) ->
    telemetry:execute([opuntia, cleanup, Name], #{}, #{}),
    TimestampThreshold = erlang:system_time(second) - TTL,
    Min = erlang:convert_time_unit(TimestampThreshold, second, millisecond),
    F = fun(_, #token_bucket{last_update = ATime}) -> ATime > Min;
           (_, none) -> false end,
    RemainingShapers = maps:filter(F, Shapers),
    State#opuntia_state{shapers = RemainingShapers}.

schedule_cleanup(#opuntia_state{gc_time = 0} = State) ->
    State;
schedule_cleanup(#opuntia_state{gc_time = GCInt} = State) ->
    TRef = erlang:start_timer(GCInt, self(), cleanup),
    State#opuntia_state{gc_ref = TRef}.

%% @doc It is a small hack
%% This function calls this in more efficient way:
%% timer:apply_after(DelayMs, gen_server, reply, [From, Reply]).
-spec reply_after(opuntia:rate(), {atom() | pid(), _}, continue) -> reference().
reply_after(DelayMs, {Pid, Tag}, Reply) ->
    erlang:send_after(DelayMs, Pid, {Tag, Reply}).
