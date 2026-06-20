# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Conventions

@.claude/skills/general-conventions/SKILL.md
@.claude/skills/elixir-conventions/SKILL.md
@.claude/skills/git-conventions/SKILL.md

Note: this repo's `.formatter.exs` uses `line_length: 78` (not 98). Run `mix format` before finalizing — it is the source of truth for formatting in this project.

## Common Commands

```bash
mix deps.get          # fetch dependencies
mix compile           # compile the project (also: make build)
mix test              # run the full test suite (also: make test)
mix test test/event_broker_test.exs            # run a single test file
mix test test/event_broker_test.exs:5          # run a single test at line 5
mix format            # auto-format (line length 78, see .formatter.exs)
mix credo             # lint (config: .credo.exs)
mix dialyzer          # type-check via dialyxir
mix docs              # generate ExDoc HTML
make release          # clean release build (clears _build/deps first)
```

Run a single example function from the REPL or a one-shot:

```bash
iex -S mix                                          # interactive shell with the app started
timeout 60 mix run -e 'Examples.EEventBroker.message_works_trivial()'
```

The application starts automatically (it is registered as `mod: {EventBroker, []}` in `mix.exs`), so the broker, registry, and dynamic supervisor are already running inside `iex -S mix`.

Toolchain pin: Elixir 1.18.4 / OTP 27 (`.tool-versions`).

## Architecture

EventBroker is a PubSub system whose central abstraction is a **filter chain**. A subscription is a *list* of filter specs (`filter_spec_list`), and the system spawns one `FilterAgent` per prefix of that list, sharing prefixes across subscribers.

### Process tree (`lib/event_broker/supervisor.ex`)

```
EventBroker.Supervisor (one_for_all)
├── EventBroker.Broker            — root sink, fans events out to top-level subscribers
├── EventBroker.Registry          — owns the filter-chain map and subscriber bookkeeping
└── DynamicSupervisor             — parents all FilterAgent processes (max_restarts: 0)
```

The names of all three children are configurable via `start_link/1` opts (`:name`, `:broker_name`, `:registry_name`), so multiple isolated broker instances can coexist — most public API functions take an optional `broker`/`registry` argument that defaults to the module-name atom.

### Filter chains and prefix sharing

`EventBroker.Registry` (`lib/event_broker/registry.ex`) keeps two maps:
- `registered_pids :: %{id => pid}` — maps a subscriber id to its pid. For atom ids this is the mailbox pid; for filter spec list keys, the filter agent pid; for pid ids, the subscriber pid. The key `[]` always maps to the root `Broker` PID.
- `registered_filter_specs :: %{id => [filter_spec_list]}` — maps a subscriber id to the list of filter spec lists it is subscribed to.

When a process subscribes to `[a, b, c]`:

1. Registry finds the **longest existing prefix** of that list (e.g. `[a]`).
2. It spawns only the *missing* tail of FilterAgents (`[a,b]`, `[a,b,c]`), each subscribed to the previous link.
3. For **pid subscribers** (ephemeral): the pid is subscribed directly to the final filter agent and `Process.monitor`'d by the registry.
4. For **atom subscribers** (durable): a `Mailbox` process is started (or reused) and subscribed to the final filter agent. The registry monitors the mailbox; the mailbox monitors the subscriber pid.

This means `[spec1, spec2]` and `[spec2]` produce **distinct** filter agents — identity is the whole chain, not the individual spec. Unsubscription walks back up the chain and reaps any agent whose subscriber set becomes empty (`FilterAgent` returns `:reap` on its last unsubscribe and `:stop`s `:normal`).

### Events and filters

- `EventBroker.Event` (`lib/event_broker/event.ex`) is the only message type the broker forwards. Anything else is silently dropped by `handle_info/2`. Use the `new_with_body/1` macro to build events with `source_module: __MODULE__` filled in.
- A filter is any module that implements the `EventBroker.Filter` behaviour (`filter(event, struct) :: bool()`). The struct value carries the per-instance parameters.
- The `deffilter` macro (`lib/event_broker/def_filter.ex`, used in `lib/event_broker/filters.ex`) generates the struct, the behaviour impl, and a `filter/2` whose body is a `case` over the event — typed parameter fields become both struct fields and locally-bound variables in the case body. Read `EventBroker.Filters` for canonical examples (`Trivial`, `LessTrivial`, `SourceModule`, `ManyFields`).
- `EventBroker.WithSubscription.with_subscription/2` is a macro that subscribes for the duration of a block and unsubscribes after, *only* for filters not already subscribed to — useful for scoped one-shot listeners.
- `EventBroker.Mailbox` (`lib/event_broker/mailbox.ex`) is a `gen_statem` that mediates delivery for durable (atom-id) subscribers. It has three states: `:buffering` (subscriber offline, events queued in memory), `:draining` (subscriber just reconnected, replaying queue), `:live` (forwarding directly). The mailbox is started under the DynamicSupervisor and survives subscriber process death.

### Startup and replay

On application start, `EventBroker.Log.replay/1` is called after the supervisor tree is up. It replays all `:subscribe` and `:unsubscribe` commands from the log to reconstruct the filter agent tree and durable subscription state. `:event` commands are skipped during replay — the broker's fanout cursor handles those separately.

### Public surface

`lib/event_broker.ex` is the only module callers should reach for:
- `event/1` — write an event to the log and trigger fanout
- `transaction/1` — wrap multiple operations in a single Mnesia transaction; fanout fires on commit
- `subscribe/3` + `subscribe_me/2` — `(pid, filter_spec_list, id_subscriber)`; `_me` defaults `pid` and `id_subscriber` to `self()`
- `unsubscribe/3` + `unsubscribe_me/2` — mirror of subscribe
- `subscriptions/1` + `my_subscriptions/0` — query current subscriptions for an id

## Examples and tests

This project follows the example-driven pattern from the elixir-conventions skill, built on the [`ex_example`](https://hex.pm/packages/ex_example) framework:

- Runnable examples live in `lib/examples/` as `Examples.E*` modules (e.g. `Examples.EEventBroker` in `lib/examples/e_event_broker.ex`). Each module starts with `use ExExample` + `import ExUnit.Assertions`. Behaviour-demonstrating functions are defined via the `example` macro (which permits parameters with `\\` defaults — `example check_self_sub(list \\ []) do …`); pure data builders and benchmarks stay as plain `def`s. Examples are real `@spec`'d functions you can call from IEx; they double as the interactive documentation for the system's behavior.
- Test wrappers in `test/` are two-line modules: `use ExUnit.Case` + `use ExExample.ExUnit, for: Examples.E…`, which auto-generates one ExUnit test per `example` in the target module. To exercise a behavior, add or modify an example rather than writing assertions inline in a test.
- `Examples.EEventBroker` carries `def rerun?(_), do: true` — its examples intentionally leave singleton-registry state mutated for the next example to observe, so ExExample's caching is disabled for that module. The other example modules use the default cache. The `EventbrokerTest.EventBroker` test wrapper runs `async: false` because `kill_subscriber` reads global registry state that concurrent test modules could pollute under `async: true`.
