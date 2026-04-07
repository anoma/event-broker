defmodule EventbrokerTest.SubscribeTest do
  use ExUnit.Case, async: true

  use ExExample.ExUnit, for: Examples.EEventBroker.Subscribe
end
