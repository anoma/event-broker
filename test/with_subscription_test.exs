defmodule EventbrokerTest.WithSub do
  use ExUnit.Case, async: true

  use EventBroker.TestHelper.GenerateExampleTests,
    for: Examples.EEventBroker.WithSub
end
