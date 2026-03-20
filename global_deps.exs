# keep me alphabetized; non-runtime dependencies at the bottom.
[
  {:typed_struct, "~> 0.3.0"},
  # non-runtime dependencies below
  {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
  {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
  {:ex_doc, "~> 0.31", only: [:dev], runtime: false}
]
