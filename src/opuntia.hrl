-ifndef(OPUNTIA).
-define(OPUNTIA, true).

-record(token_bucket, {
          rate :: opuntia:rate(),
          available_tokens :: opuntia:tokens(),
          last_update :: opuntia:timestamp()
}).

-endif.
