-ifndef(OPUNTIA).
-define(OPUNTIA, true).

-record(token_bucket_shaper, {
          shape :: opuntia:shape(),
          available_tokens :: opuntia:tokens(),
          last_update :: number()
}).

-endif.
