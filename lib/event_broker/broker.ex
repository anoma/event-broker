defmodule EventBroker.Broker do
  @moduledoc """
  I am a Broker module.

  I specify the behavior of the server acting as a central broker of the
  PubSub service. My functionality is minimal. I wait for messages and
  relay them to my subscribers.
  """

  use GenServer
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    I am the type of the Event Broker.

    ### Fields

    - `:name` - The name of the broker
    - `:subscribers` - The set of pids showcasing subscribers.
                       Default: Map.Set.new()
    """

    field(:name, atom(), enforce: true)
    field(:subscribers, MapSet.t(pid()), default: MapSet.new())
  end

  @spec start_link(list()) :: GenServer.on_start()
  def start_link(args \\ []) do
    name = args[:broker_name] || __MODULE__
    GenServer.start_link(__MODULE__, name, name: name)
  end

  @impl true
  def init(name) do
    EventBroker.Log.register_broker(name)
    {:ok, %EventBroker.Broker{name: name}}
  end

  ############################################################
  #                    Genserver Behavior                    #
  ############################################################

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {:reply, :ok,
     %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(:wakeup, state) do
    {:atomic, events} =
      :mnesia.transaction(fn ->
        broker_time = EventBroker.Log.broker_time(state.name)
        system_time = EventBroker.Log.system_time(state.name)
        events = EventBroker.Log.commands_since(broker_time, state.name, :event)
        EventBroker.Log.write_broker_time(state.name, system_time)
        events
      end)

    # Broke 'er? I hardly know 'er!
    
    for {_table_name, _system_time, _tx_id, :event, event} <- events,
        pid <- state.subscribers do
      send(pid, event)
    end

    {:noreply, state}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_info, state) do
    {:noreply, state}
  end
end
