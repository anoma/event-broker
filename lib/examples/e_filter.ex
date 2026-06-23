defmodule Examples.EEventBroker.Filter do
  @moduledoc """
  I define examples on how to use the deffilter macro to create filters for the event broker.
  """
  alias EventBroker.Event

  use EventBroker.DefFilter

  # @doc """
  # I am a filter that subscribes to all messages.
  # I don't filter on any patterns, so all messages are valid.
  # """
  deffilter AcceptAll do
    _ -> true
  end

  # @doc """
  # I am a filter that rejects all messages.
  # """
  deffilter RejectAll do
    _ -> false
  end

  # @doc """
  # I am a filter that accept evnets that have a body which is a map and has a key
  # `:level` with value `:error`.
  # """
  deffilter Error do
    %Event{body: %{level: :error}} -> true
    _ -> false
  end

  deffilter ManyFields,
    params_module: module(),
    params_foo: term(),
    params_bar: term() do
    %EventBroker.Event{
      source_module: event_module,
      body: {event_foo, event_bar}
    } ->
      params_module == event_module && params_foo == event_foo &&
        params_bar == event_bar
  end

  deffilter SourceModule, module: module() do
    %EventBroker.Event{source_module: ^module} -> true
    _ -> false
  end
end
