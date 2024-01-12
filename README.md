# `opuntia`

[![Actions Status](https://github.com/esl/opuntia/actions/workflows/ci.yml/badge.svg)](https://github.com/esl/opuntia/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/esl/opuntia/branch/main/graph/badge.svg)](https://codecov.io/gh/esl/opuntia)
[![Hex](http://img.shields.io/hexpm/v/opuntia.svg)](https://hex.pm/packages/opuntia)

`opuntia` is a basic set of tools for traffic shaping for erlang and elixir

It implements the [token bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket).

There are two ways to use it, checking availability a priori or accepting a penalisation.

After creating a bucket
```erl
Bucket = opuntia:new(#{bucket_size => 10, rate => 1, time_unit => millisecond, start_full => true}),
```
you can either consume all tokens queued and see the suggested delay, considering that this might
allow you to consume at once much more than the bucket size:
```erl
{NewShaper, Delay} = opuntia:update(Shaper, 50),
timer:sleep(Delay), %% Will suggest to sleep 40ms
```
or you can first how many tokens are available for you to consume before doing so:
```erl
Allowed = opuntia:peek(Shaper),
consume_tokens(Allowed),
{NewShaper, 0} = opuntia:update(Shaper), %% Will suggest no delay if you were diligent and consume less that adviced
```
