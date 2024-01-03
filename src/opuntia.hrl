-ifndef(OPUNTIA).
-define(OPUNTIA, true).

-record(token_bucket_shaper, {
          shape :: opuntia:shape(),
          available_tokens :: opuntia:tokens(),
          last_update :: integer(),
          debt :: float() %% Always in the range [0.0, 1.0]
}).

-endif.
