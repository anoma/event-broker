defmodule Examples.EEventBroker.Subscribe do
  @moduledoc """
  I define examples on how to susbcribe to topics in the event broker.
  """

  alias Examples.EEventBroker.EFilter
  alias EventBroker.Event

  use ExExample
  import ExUnit.Assertions

  # A list of default filters to use in the examples below.
  @filters [%EFilter.AcceptAll{}, %EFilter.Error{}]

  @doc """
  I subscribe using the `Trivial` filter and assert that I receive any events
  sent on the message broker. I also verify that subscribe and unsubscribe
  commands are written to the command log.
  """
  @spec subscribe_to_filter(struct()) :: {:received, any()}
  @spec subscribe_to_filter() :: {:received, any()}
  example subscribe_to_filter(filter \\ %EFilter.AcceptAll{}) do
    {:atomic, t} =
      :mnesia.transaction(fn -> EventBroker.Log.system_time() end)

    EventBroker.subscribe_me([filter], :sub_id)

    {:atomic, commands} =
      :mnesia.transaction(fn -> EventBroker.Log.commands_since(t) end)

    assert Enum.any?(commands, fn {:command, _, _, cmd, body} ->
             cmd == :subscribe and body == {:sub_id, [filter]}
           end)

    registry = :sys.get_state(EventBroker.Registry)
    mailbox_pid = registry.registered_pids[:sub_id]
    assert is_pid(mailbox_pid) and Process.alive?(mailbox_pid)
    assert Map.has_key?(registry.registered_pids, [filter])
    assert [[filter]] == registry.registered_filter_specs[:sub_id]

    event = %Event{source_module: nil, body: %{message: "everything matches"}}

    EventBroker.event(event)
    assert_receive ^event

    EventBroker.unsubscribe_me([filter], :sub_id)

    {:atomic, commands} =
      :mnesia.transaction(fn -> EventBroker.Log.commands_since(t) end)

    assert Enum.any?(commands, fn {:command, _, _, cmd, body} ->
             cmd == :unsubscribe and body == {:sub_id, [filter]}
           end)

    registry = :sys.get_state(EventBroker.Registry)
    refute Map.has_key?(registry.registered_pids, :sub_id)
    assert registry.registered_filter_specs[:sub_id] == nil

    {:received, event}
  end

  @doc """
  I subscribe to multiple filters and assert that I receive the events that are
  allowed by those filters.
  """

  @spec subscribe_to_multiple_filters([struct()]) :: {:received, any()}
  @spec subscribe_to_multiple_filters() :: {:received, any()}
  example subscribe_to_multiple_filters(filters \\ @filters) do
    # subscribe to the trivial filter (i.e., all messages)
    EventBroker.subscribe_me(filters)

    # confirm this process is subscribed
    assert_subscription(filters)

    # create an event that matches the filter
    event = %Event{source_module: nil, body: %{level: :error}}

    # send the event
    :ok = EventBroker.event(event)

    # assert that this process receives the event
    assert_receive ^event

    EventBroker.unsubscribe_me(filters)

    # confirm this process is unsubscribed
    refute_subscription(filters)

    {:received, event}
  end

  @doc """
  I unsubscribe a process from a filter and verify that it is unsubscribed.
  """
  @spec unsubscribe_from_filter(struct()) :: {:received, any()}
  @spec unsubscribe_from_filter() :: {:received, any()}
  example unsubscribe_from_filter(filter \\ %EFilter.AcceptAll{}) do
    # subscribe to the trivial filter (i.e., all messages)
    EventBroker.subscribe_me([filter])

    # create an event that matches the filter
    event = %Event{source_module: nil, body: %{message: "everything matches"}}

    # send the event
    EventBroker.event(event)

    # assert that this process receives the event
    assert_receive ^event

    EventBroker.unsubscribe_me([filter])

    {:received, event}
  end

  @doc """
  I subscribe a process to events, and check whether the registry cleans up its
  subscriptions when it goes offline.
  """
  @spec unsubscribe_on_down() :: any()
  example unsubscribe_on_down do
    # any filter will do
    filter = %EFilter.AcceptAll{}

    this = self()

    # create a process that will subscribe
    subscriber =
      spawn(fn ->
        EventBroker.subscribe_me([filter])

        # confirm this process is subscribed
        assert_subscription(filter)

        # message the broker we're done
        send(this, :done)

        # wait for a message to terminate
        receive do
          :terminate ->
            :ok
        end
      end)

    # monitor the subscriber
    Process.monitor(subscriber)

    # wait for the subscriber to subscribe
    assert_receive :done

    # check that the process has been subscribed
    assert [[filter]] ==
             EventBroker.subscriptions(subscriber)

    # stop the subscriber
    send(subscriber, :terminate)

    # wait for its down message
    assert_receive {:DOWN, _, _, ^subscriber, _}

    # assert that its no longer subscribed
    assert [] == EventBroker.subscriptions(subscriber)
  end

  @doc """
  I demonstrate that a durable subscriber survives going offline.

  I subscribe a process under the atom ID `:reconnect_id`, then kill it.
  Events sent while the subscriber is offline are buffered by the mailbox.
  When the same atom ID subscribes again from a new process, the mailbox
  drains the buffered events to it.
  """
  @spec mailbox_reconnect() :: {:received, [Event.t()]}
  example mailbox_reconnect do
    filter = %EFilter.AcceptAll{}
    this = self()

    # First subscriber — subscribes under a durable atom ID then waits.
    first =
      spawn(fn ->
        EventBroker.subscribe_me([filter], :reconnect_id)
        send(this, :subscribed)

        receive do
          :terminate -> :ok
        end
      end)

    assert_receive :subscribed

    mailbox_pid =
      :sys.get_state(EventBroker.Registry).registered_pids[:reconnect_id]

    # Kill the first subscriber.
    Process.monitor(first)
    send(first, :terminate)
    assert_receive {:DOWN, _, _, ^first, _}

    # Sync with the mailbox so we know it has processed the :DOWN and
    # transitioned to :buffering before we send events.
    assert {:buffering, _} = :sys.get_state(mailbox_pid)

    e1 = %Event{source_module: nil, body: 1}
    e2 = %Event{source_module: nil, body: 2}
    EventBroker.event(e1)
    EventBroker.event(e2)

    # Reconnect: calling subscribe_me with the same atom ID connects self()
    # to the existing mailbox, which drains the buffered events to us.
    EventBroker.subscribe_me([filter], :reconnect_id)

    assert_receive ^e1
    assert_receive ^e2

    EventBroker.unsubscribe_me([filter], :reconnect_id)

    {:received, [e1, e2]}
  end

  ############################################################
  #                       Helpers                            #
  ############################################################
  @doc """
  I assert that the current process is subscribed to the given filter.
  """
  @spec assert_subscription([struct()] | struct()) :: [struct()] | struct()
  def assert_subscription(filter) when is_struct(filter) do
    # fetch the current subscriptions
    current_subscriptions = EventBroker.my_subscriptions()

    # check that we're currently not subscribed to this filter
    assert [filter] in current_subscriptions

    filter
  end

  def assert_subscription(filters) when is_list(filters) do
    # fetch the current subscriptions
    current_subscriptions = EventBroker.my_subscriptions()

    # check that we're currently not subscribed to this filter
    assert filters in current_subscriptions

    filters
  end

  @doc """
  Given a list of filters, I make sure that the current process is not subscribed to them.
  """
  @spec refute_subscription([struct()] | struct()) :: [struct()] | struct()
  def refute_subscription(filters) when is_list(filters) do
    # fetch the current subscriptions
    current_subscriptions = EventBroker.my_subscriptions()

    # check that we're currently not subscribed to this filter
    assert filters not in current_subscriptions

    filters
  end

  @spec refute_subscription(struct()) :: struct()
  def refute_subscription(filter) when is_struct(filter) do
    # fetch the current subscriptions
    current_subscriptions = EventBroker.my_subscriptions()

    # check that we're currently not subscribed to this filter
    assert [filter] not in current_subscriptions

    filter
  end
end
