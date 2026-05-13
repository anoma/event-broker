import Config

config :logger,
  level: :error

config :event_broker, mnesia_storage: :ram_copies
