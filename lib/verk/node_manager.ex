defmodule Verk.NodeManager do
  @moduledoc """
  NodeManager keeps track of the nodes that are working on the queues
  """

  use GenServer
  require Logger

  @verk_nodes_key "verk_nodes"

  @doc false
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc false
  def init(_) do
    node_id    = Application.fetch_env!(:verk, :node_id)
    frequency  = Confex.get_env(:verk, :heartbeat, 3_000)

    Logger.info "Node Manager started for node #{node_id}. Heartbeat will run every #{frequency} milliseconds"

    Redix.pipeline!(Verk.Redis, [["SADD", @verk_nodes_key, node_id],
                                ["PSETEX", "verk:node:#{node_id}", 2 * frequency, "alive"]])
    heartbeat!(node_id, frequency)
    {:ok, {node_id, frequency}}
  end

  @doc false
  def handle_info(:heartbeat, state = {node_id, frequency}) do
    Logger.info("❤️")

    verk_nodes = Redix.command!(Verk.Redis, ["SMEMBERS", @verk_nodes_key])

    Logger.info("Nodes that are alive: #{Enum.join(verk_nodes, ",")}")

    Enum.each(verk_nodes, fn verk_node ->
      if Redix.command!(Verk.Redis, ["PTTL", "verk:node:#{verk_node}"]) < 0 do
        Logger.info("Node #{verk_node} is dead. Time to clean up!")

        queues = Redix.command!(Verk.Redis, ["SMEMBERS", "verk:node:#{verk_node}:queues"])
        for queue <- queues do
          enqueue_inprogress(verk_node, queue)
        end
        Redix.command!(Verk.Redis, ["DEL", "verk:node:#{verk_node}:queues"])
        Redix.command!(Verk.Redis, ["SREM", @verk_nodes_key, verk_node])
      else
        Logger.info("Node #{verk_node} is alive")
      end
    end)

    heartbeat!(node_id, frequency)
    {:noreply, state}
  end

  defp heartbeat!(node_id, frequency) do
    Redix.command!(Verk.Redis, ["PSETEX", "verk:node:#{node_id}", 2 * frequency, "alive"])
    Process.send_after(self(), :heartbeat, frequency)
  end


  @lpop_rpush_src_dest_script_sha Verk.Scripts.sha("lpop_rpush_src_dest")
  @max_enqueue_inprogress 1000
  def enqueue_inprogress(node_id, queue) do
    in_progress_key = "inprogress:#{queue}:#{node_id}"
    case Redix.command(Verk.Redis, ["EVALSHA", @lpop_rpush_src_dest_script_sha, 2,
                                     in_progress_key, "queue:#{queue}", @max_enqueue_inprogress]) do
      {:ok, [0, m]} ->
        Logger.info("Added #{m} jobs.")
        Logger.info("No more jobs to be added to the queue #{queue} from inprogress list.")
        :ok
      {:ok, [n, m]} ->
        Logger.info("Added #{m} jobs.")
        Logger.info("#{n} jobs still to be added to the queue #{queue} from inprogress list.")
        enqueue_inprogress(node_id, queue)
      {:error, reason} ->
        Logger.error("Failed to add jobs back to queue #{queue} from inprogress. Error: #{inspect reason}")
        throw :error
    end
  end
end
