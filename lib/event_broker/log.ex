defmodule EventBroker.Log do
  @moduledoc """
  I am the write-ahead log for the event broker. I manage per-broker command
  logs stored in Mnesia and maintain a registry of broker name to table pair
  mappings. System time here refers to a monotonic counter per broker.
  """

  use Agent

  defp start_mnesia() do
    :ok = Application.put_env(:mnesia, :dir, ~c".mnesiastore/")

    case :mnesia.create_schema([node()]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
    end

    case :mnesia.start() do
      :ok ->
        :ok

      {:error, {:already_started, :mnesia}} ->
        :ok

      {:error, :failed_to_create_schema, _error} ->
        {:error, :failed_to_create_schema}
    end
  end

  defp create_table(name, opts) do
    case :mnesia.create_table(name, [{:disc_copies, [node()]} | opts]) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, _}} ->
        :ok

      {:aborted, {:bad_type, _, :disc_copies, :nonode@nohost}} ->
        case :mnesia.create_table(name, [{:ram_copies, [node()]} | opts]) do
          {:atomic, :ok} -> :ok
          {:aborted, {:already_exists, _}} -> :ok
        end
    end
  end

  defp create_broker_tables(broker) do
    command_table = :"command_#{broker}"
    meta_table = :"meta_#{broker}"

    # The command log 
    create_table(command_table,
      attributes: [:key, :tx_id, :command, :body],
      type: :ordered_set
    )

    # The meta table for cursors
    create_table(meta_table, attributes: [:key, :value], type: :set)

    :mnesia.wait_for_tables([command_table, meta_table], 5_000)

    {command_table, meta_table}
  end

  @spec start_link(list()) :: Agent.on_start()
  def start_link(_args) do
    start_mnesia()
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  I register a broker, creating its Mnesia table pair if needed, and
  initialise its cursor to 0 if not already set.
  """
  @spec register_broker(atom()) :: :ok
  def register_broker(broker) do
    {command_table, meta_table} = create_broker_tables(broker)
    Agent.update(__MODULE__, &Map.put(&1, broker, {command_table, meta_table}))
    init_broker_times(broker)
  end

  defp init_broker_times(broker) do
    :mnesia.transaction(fn ->
      if system_time(broker) == :absent do
        write_system_time(broker, 0)
      end

      if broker_time(broker) == :absent do
        write_broker_time(broker, 0)
      end
    end)
  end

  defp write_system_time(broker, i) do
    {_command_table, meta_table} = tables(broker)
    :mnesia.write({meta_table, :system_time, i})
  end

  @doc """
  I retrieve the current command and meta tables for a broker
  """
  @spec tables(atom()) :: {atom(), atom()}
  def tables(broker) do
    Agent.get(__MODULE__, &Map.fetch!(&1, broker))
  end

  @doc """
  I retrieve the current maximum time of the command log, where the next command will be written
  """
  @spec system_time(atom()) :: non_neg_integer() | :absent
  def system_time(broker) do
    {_command_table, meta_table} = tables(broker)

    case :mnesia.read(meta_table, :system_time) do
      [{_, :system_time, t}] -> t
      [] -> :absent
    end
  end

  @doc """
  I retrieve the current fanout cursor for the broker
  """
  @spec broker_time(atom()) :: non_neg_integer() | :absent
  def broker_time(broker) do
    {_command_table, meta_table} = tables(broker)

    case :mnesia.read(meta_table, :broker_time) do
      [{_, :broker_time, t}] -> t
      [] -> :absent
    end
  end

  @doc """
  I read all commands for broker since time t. By default I list all commands. I must be called within a transaction.
  """
  @spec commands_since(non_neg_integer(), atom(), atom()) :: [EventBroker.Event.t()]
  def commands_since(t, broker, command \\ :"$3") do
    {command_table, _meta_table} = tables(broker)

    :mnesia.select(command_table, [
      {{command_table, :"$1", :"$2", command, :"$4"}, [{:>=, :"$1", t}], [:"$_"]}
    ])
  end

  @doc """
  I write a command at the current system time. I must be called inside a
  transaction.
  """
  @spec write_command(atom(), non_neg_integer(), atom(), any()) :: :ok
  def write_command(broker, tx_id, command, body) do
    {command_table, _meta_table} = tables(broker)
    {t1, _t2} = inc_system_time(broker)
    :mnesia.write({command_table, t1, tx_id, command, body})
  end

  @doc """
  I increase the monotonic system time of the log
  """
  def inc_system_time(broker) do
    {_command_table, meta_table} = tables(broker)

    t = system_time(broker)
    :mnesia.write({meta_table, :system_time, t + 1})
    {t, t + 1}
  end

  @doc """
  I update the fanout cursor for the broker to i. I must be called inside a
  transaction.
  """
  @spec write_broker_time(atom(), non_neg_integer()) :: :ok
  def write_broker_time(broker, i) do
    {_command_table, meta_table} = tables(broker)
    :mnesia.write({meta_table, :broker_time, i})
  end
end
