defmodule EventBroker.Log do
  @moduledoc """
  I am the write-ahead log for the event broker. I manage a command log stored
  in Mnesia across two tables: `:command` for the log entries and `:meta` for
  cursors. System time here refers to a global monotonic counter.
  """

  def start_mnesia() do
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

  def create_tables() do
    command_table = :command
    meta_table = :meta

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

  def init_times do
    :mnesia.transaction(fn ->
      if system_time() == :absent do
        write_system_time(0)
      end

      if broker_time() == :absent do
        write_broker_time(0)
      end
    end)
  end

  defp write_system_time(i) do
    :mnesia.write({:meta, :system_time, i})
  end

  @doc """
  I retrieve the current system time of the command log. The next command will be written at this value, then time is incremented.
  """
  @spec system_time() :: non_neg_integer() | :absent
  def system_time() do
    case :mnesia.read(:meta, :system_time) do
      [{_, :system_time, t}] -> t
      [] -> :absent
    end
  end

  @doc """
  I retrieve the current fanout cursor for the broker
  """
  @spec broker_time() :: non_neg_integer() | :absent
  def broker_time() do
    case :mnesia.read(:meta, :broker_time) do
      [{_, :broker_time, t}] -> t
      [] -> :absent
    end
  end

  @doc """
  I read all commands since time t, optionally filtered by command type. By default I list all commands. I must be called within a transaction.
  """
  @spec commands_since(non_neg_integer(), atom()) :: [EventBroker.Event.t()]
  def commands_since(t, command \\ :"$3") do
    :mnesia.select(:command, [
      {{:command, :"$1", :"$2", command, :"$4"}, [{:>=, :"$1", t}], [:"$_"]}
    ])
  end

  @doc """
  I write a command at the current system time. I must be called inside a
  transaction.
  """
  @spec write_command(non_neg_integer(), atom(), any()) :: :ok
  def write_command(tx_id, command, body) do
    {t1, _t2} = inc_system_time()
    :mnesia.write({:command, t1, tx_id, command, body})
  end

  @doc """
  I increase the monotonic system time of the log
  """
  def inc_system_time() do
    t = system_time()
    :mnesia.write({:meta, :system_time, t + 1})
    {t, t + 1}
  end

  @doc """
  I update the fanout cursor for the broker to i. I must be called inside a
  transaction.
  """
  @spec write_broker_time(non_neg_integer()) :: :ok
  def write_broker_time(i) do
    :mnesia.write({:meta, :broker_time, i})
  end

  @doc """
  I replay all commands in the log in order, dispatching each to the
  registry. Subscribe and unsubscribe commands reconstruct the filter agent
  tree and subscription state. Events are skipped — the broker's fanout
  cursor handles those separately.
  """
  @spec replay(atom()) :: :ok
  def replay(registry \\ EventBroker.Registry) do
    {:atomic, commands} =
      :mnesia.transaction(fn -> commands_since(0) end)

    for {:command, _, _, command, body} <- commands do
      case command do
        :subscribe ->
          {id, filter_spec_list} = body
          GenServer.call(registry, {:subscribe, id, filter_spec_list})

        :unsubscribe ->
          {id, filter_spec_list} = body
          GenServer.call(registry, {:unsubscribe, id, filter_spec_list})

        _ ->
          :ok
      end
    end

    :ok
  end
end
