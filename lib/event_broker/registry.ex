defmodule EventBroker.Registry do
  @moduledoc """
  I am the Registry for the PubSub system.

  I am the central registry of all the topic subscriptions and filters. I am
  responsible for spawning filter agents, (un)subscribing to them, and keeping
  track of relations between them.

  ## Subscriptions

  When a process with pid `p` subscribes to a filter spec `[f, g]`, a  "filter
  agent" will be spawned for both `f` and `g`.

  A filter agent is a process that receives events, and only forwards the events
  that match its filter.

  If a filter spec list is subscribed to for which any prefix already has an
  existing filter agent, these are not spawned. The existing filter agents will
  be used instead.

  ## Registered Filters

  When a process subscribes to a filter spec list,
  """

  alias __MODULE__

  use GenServer
  use TypedStruct

  @typedoc """
  I am the type of the subscriber pids, matching a subscriber or filter id to its pid
  """
  @type registered_pids :: %{EventBroker.id() => pid()}

  @typedoc """
  I am the type of the registered subscribers. I map an id to their subscribed
  their filter specs.
  """
  @type registered_filter_specs :: %{
          EventBroker.id() => [EventBroker.filter_spec_list()]
        }

  typedstruct enforce: true do
    @typedoc """
    I am the type of the Registry.

    My main functionality is to keep track of all spawned filter actors.

    ### Fields

    - `:supervisor` - The name of the dynamic supervisor launched on start.
    - `:registered_filter_specs` - The map whose keys are a ID mapped onto their filter specs.
    - `:registered_pids` - The map whos keps are a subscriber ID mapped onto the PID
    Default: %{}
    """

    field(:supervisor, atom())
    field(:registered_filter_specs, registered_filter_specs, default: %{})
    field(:registered_pids, registered_pids, default: %{})
  end

  @spec start_link(list()) :: GenServer.on_start()
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args[:registry_name])
  end

  @impl true
  def init(args) do
    {:ok,
     %Registry{
       supervisor: args[:dyn_sup_name],
       registered_pids: %{[] => Process.whereis(EventBroker.Broker)},
       registered_filter_specs: %{}
     }}
  end

  ############################################################
  #                    Genserver Behavior                    #
  ############################################################

  @impl true
  def handle_call(
        {:subscribe, pid, filter_spec_list, id_subscriber},
        _from,
        state
      ) do
    # a filter is valid if it a module that exports a `&filter/2` function.
    invalid_filters =
      filter_spec_list
      |> Enum.flat_map(fn spec ->
        struct = spec.__struct__

        Code.ensure_loaded(struct)

        if Kernel.function_exported?(struct, :filter, 2) do
          []
        else
          [struct]
        end
      end)

    if Enum.empty?(invalid_filters) do
      # create a new filter agent for all non-existing filter specs. e.g., if
      # the filter spec list is [a b c], create an agent for a, b, and c. if a,
      # b already existed, only create c.
      new_state =
        if Map.has_key?(state.registered_pids, filter_spec_list) do
          state
        else
          {_, new_registered_pids} =
            iterate_sub(
              state.registered_pids,
              filter_spec_list,
              state.supervisor
            )

          %{state | registered_pids: new_registered_pids}
        end

      # keep a mapping between subscribing ids and which filterlists they subscribed to
      # this is used to unsubscribe them when they go down.
      registered_filter_specs =
        Map.update(
          new_state.registered_filter_specs,
          id_subscriber,
          [filter_spec_list],
          &[filter_spec_list | &1]
        )

      registered_pids = Map.put(new_state.registered_pids, id_subscriber, pid)

      new_state =
        new_state
        |> Map.put(:registered_filter_specs, registered_filter_specs)
        |> Map.put(:registered_pids, registered_pids)

      # monitor the subscribing process to get notified when it goes down.
      Process.monitor(pid)

      # subscribe the subscribing process to messages of the last filter in the
      # filter spec list its filter agent.
      GenServer.call(
        Map.get(new_state.registered_pids, filter_spec_list),
        {:subscribe, pid}
      )

      {:reply, :ok, new_state}
    else
      # if there was an invalid filter in the filter spec list, the subscription cannot be made.
      {:reply,
       "#{inspect(invalid_filters)} do not export filtering functions", state}
    end
  end

  def handle_call(
        {:unsubscribe, pid, filter_spec_list, id_subscriber},
        _from,
        state
      ) do
    new_registered_pids =
      do_unsubscribe(pid, filter_spec_list, state.registered_pids)

    state =
      state
      |> Map.put(:registered_pids, new_registered_pids)
      |> Map.update!(:registered_filter_specs, fn ss ->
        Map.update!(
          ss,
          id_subscriber,
          &Enum.reject(&1, fn f -> f == filter_spec_list end)
        )
      end)

    {:reply, :ok, state}
  end

  def handle_call({:subscriptions, id}, _from, state) do
    {:reply, Map.get(state.registered_filter_specs, id, []), state}
  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case Enum.find(state.registered_pids, fn {_, v} -> v == pid end) do
      nil ->
        {:noreply, state}

      {id, _} ->
        filter_spec_list_list = Map.get(state.registered_filter_specs, id, [])

        state =
          Enum.reduce(
            filter_spec_list_list,
            state,
            fn filter_spec_list, state ->
              new_registered_pids =
                do_unsubscribe(pid, filter_spec_list, state.registered_pids)

              %{state | registered_pids: new_registered_pids}
            end
          )
          |> Map.update!(:registered_pids, &Map.delete(&1, id))

        state =
          if is_pid(id) do
            Map.update!(state, :registered_filter_specs, &Map.delete(&1, id))
          else
            state
          end

        {:noreply, state}
    end
  end

  ############################################################
  #                          Helpers                         #
  ############################################################

  @spec iterate_sub(
          registered_pids(),
          EventBroker.filter_spec_list(),
          atom()
        ) ::
          {EventBroker.filter_spec_list(), registered_pids()}
  defp iterate_sub(registered, filter_spec_list, supervisor) do
    existing_prefix =
      registered
      |> Map.keys()
      |> Enum.filter(fn p ->
        is_list(p) and List.starts_with?(filter_spec_list, p)
      end)
      |> Enum.sort(&(length(&1) > length(&2)))
      |> hd()

    remaining_to_spawn = filter_spec_list -- existing_prefix

    # ["a", "b"] -> ["a", "b", "c", "d"]
    # {["a", "b"], state} iterating over "c"
    # {["a", "b", "c"], state + ["a", "b", "c"] iterating over "d"

    for f <- remaining_to_spawn, reduce: {existing_prefix, registered} do
      {parent_spec_list, old_state} ->
        parent_pid = Map.get(old_state, parent_spec_list)
        new_spec_list = parent_spec_list ++ [f]

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            supervisor,
            {EventBroker.FilterAgent, f}
          )

        GenServer.call(parent_pid, {:subscribe, new_pid})
        new_state = Map.put(old_state, new_spec_list, new_pid)
        {new_spec_list, new_state}
    end
  end

  @spec do_unsubscribe(
          pid(),
          EventBroker.filter_spec_list(),
          registered_pids()
        ) ::
          registered_pids()
  defp do_unsubscribe(pid, filter_spec_list, registered_pids) do
    filter_pid = Map.get(registered_pids, filter_spec_list)

    case filter_pid do
      nil ->
        registered_pids

      filter_pid ->
        case GenServer.call(filter_pid, {:unsubscribe, pid}) do
          :ok ->
            registered_pids

          :reap ->
            new_registered_pids =
              Map.delete(registered_pids, filter_spec_list)

            parent_spec_list =
              Enum.take(filter_spec_list, length(filter_spec_list) - 1)

            do_unsubscribe(
              filter_pid,
              parent_spec_list,
              new_registered_pids
            )
        end
    end
  end
end
