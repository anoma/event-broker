defmodule EventBroker.Mailbox do
  @moduledoc """
  I am the mailbox for a durable subscriber.

  I mediate event delivery between the final filter agent and a
  durable (atom-id) subscriber. I buffer events in memory when the
  subscriber is offline and drain them on reconnect before resuming
  live forwarding.

  ### States

  - `:buffering` — no live subscriber; events queued in memory
  - `:draining` — subscriber just reconnected; replaying queued events
  - `:live` — subscriber connected; events forwarded directly
  """

  @behaviour :gen_statem

  use TypedStruct

  typedstruct enforce: true do
    field(:id, atom())
    field(:subscriber_pid, pid() | nil, default: nil)
    field(:queue, :queue.queue(), default: :queue.new())
  end

  @spec start_link({atom(), EventBroker.filter_spec_list()}) ::
          :gen_statem.start_ret()
  def start_link({id, _filter_spec_list}) do
    :gen_statem.start_link(__MODULE__, id, [])
  end

  def child_spec({id, filter_spec_list}) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [{id, filter_spec_list}]},
      restart: :transient
    }
  end

  @doc "I connect a live subscriber pid, draining any queued events first."
  @spec connect(pid(), pid()) :: :ok
  def connect(mailbox, subscriber_pid) do
    :gen_statem.call(mailbox, {:connect, subscriber_pid})
  end

  @impl :gen_statem
  def callback_mode, do: :state_functions

  @impl :gen_statem
  def init(id) do
    {:ok, :buffering, %__MODULE__{id: id}}
  end

  ############################################################
  #                       :buffering                         #
  ############################################################

  def buffering({:call, from}, {:connect, pid}, data) do
    Process.monitor(pid)
    new_data = %{data | subscriber_pid: pid}

    if :queue.is_empty(data.queue) do
      {:next_state, :live, new_data, [{:reply, from, :ok}]}
    else
      {:next_state, :draining, new_data,
       [{:reply, from, :ok}, {:next_event, :internal, :drain}]}
    end
  end

  def buffering(:info, %EventBroker.Event{} = event, data) do
    {:keep_state, %{data | queue: :queue.in(event, data.queue)}}
  end

  def buffering(:info, _msg, _data), do: :keep_state_and_data

  ############################################################
  #                         :live                            #
  ############################################################

  def live(:info, %EventBroker.Event{} = event, data) do
    send(data.subscriber_pid, event)
    :keep_state_and_data
  end

  def live(:info, {:DOWN, _ref, :process, pid, _reason}, data)
      when pid == data.subscriber_pid do
    {:next_state, :buffering, %{data | subscriber_pid: nil}}
  end

  def live(:info, _msg, _data), do: :keep_state_and_data

  ############################################################
  #                       :draining                          #
  ############################################################

  def draining(:internal, :drain, data) do
    case :queue.out(data.queue) do
      {:empty, _} ->
        {:next_state, :live, data}

      {{:value, event}, rest} ->
        send(data.subscriber_pid, event)

        {:keep_state, %{data | queue: rest},
         [{:next_event, :internal, :drain}]}
    end
  end

  def draining(:info, %EventBroker.Event{} = event, data) do
    {:keep_state, %{data | queue: :queue.in(event, data.queue)}}
  end

  def draining(:info, {:DOWN, _ref, :process, pid, _reason}, data)
      when pid == data.subscriber_pid do
    {:next_state, :buffering, %{data | subscriber_pid: nil}}
  end

  def draining(:info, _msg, _data), do: :keep_state_and_data
end
