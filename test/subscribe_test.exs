defmodule EventbrokerTest.SubscribeTest do
  use ExUnit.Case, async: true

  use EventBroker.TestHelper.GenerateExampleTests,
    for: Examples.EEventBroker.Subscribe
end
