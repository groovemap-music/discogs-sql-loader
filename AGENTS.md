# Repository instructions

- Keep catalog-event and persistence contracts pinned to immutable producer commits.
- Never restore monorepo-relative imports or write generated bindings into another repository.
- Default tests and checks must not connect to RabbitMQ or PostgreSQL.
- Do not log credentials, AMQP URLs, PostgreSQL connection strings, or secret-file contents.
- Cross-repository CI authentication must use a narrowly installed GitHub App, never a PAT.
- Run `just check` before proposing a change; run `just image` for container changes.
- Publishing, tagging, pushing images, and releasing require separate approval.
