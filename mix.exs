defmodule EventBroker.MixProject do
  use Mix.Project

  def project do
    [
      app: :event_broker,
      version: "1.1.1",
      build_path: "_build",
      config_path: "config/config.exs",
      package: package(),
      deps_path: "deps",
      lockfile: "mix.lock",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {EventBroker, []},
      extra_applications: [:ex_unit, :mix, :logger, :mnesia]
    ]
  end

  def package do
    [
      maintainers: ["Mariari", " Raymond E. Pasco"],
      name: :event_broker,
      description: "A PubSub event broker with filtered subscriptions",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/anoma/event-broker"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_example, "~> 0.1.1"},
      {:jason, "~> 1.4"},
      {:typed_struct, "~> 0.3.0"},
      # non-runtime dependencies below
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.31", only: [:dev], runtime: false}
    ]
  end
end
