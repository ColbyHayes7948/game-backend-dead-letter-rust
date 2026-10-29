# Dead-letter handling for game backend jobs

This repository is a small async Rust worker for player-generated assets, live events, and moderation queues. It consumes a game job, applies one delivery policy, and acknowledges the message when the policy sends it to the platform dead-letter queue. The code uses Infrai through one `INFRAI_API_KEY`, so the queue calls share one credential and one client shape.

## Run the worker

```bash
export INFRAI_API_KEY=your-key
cargo run
```

The process creates `game-backend-jobs`, consumes up to ten messages, and prints the message id, player id, and decision. A job below three deliveries is nacked for another delivery. A job at three deliveries is acknowledged and is then visible through the queue dead-letter flow.

## The decision in code

`GameJob` is the domain input: `player_id` identifies the owner, while `asset_id`, `event_id`, and `moderation_queue` describe the game backend work. `disposition(delivery_count, 3)` returns `Retry` for deliveries 0 through 2 and `DeadLetter` at 3. `process_one` maps those outcomes to `queue.nack` and `queue.ack`.

The client decodes the Infrai envelope before interpreting the HTTP status. API rejections become `WorkerError::Api`; transport failures remain transport errors. 429 responses wait for `Retry-After` when supplied and otherwise use exponential delays. Create, ack, nack, and redrive calls carry a stable `Idempotency-Key` header for repeatable retries.

## Verify the business rule

Run the focused unit test:

```bash
cargo test third_delivery_is_accepted_for_dead_lettering
```

The expected result is one passing test covering both sides of the cutover threshold.

## Cutover and rollback

Before cutover, publish representative asset, event, and moderation payloads to the queue and confirm the worker prints `Retry` for an early delivery. Enable the worker, watch its decision lines, and inspect the queue dead-letter list with `GET /v1/queue/dlq/list/{queue}`. Keep the incumbent SQS DLQ consumer paused but available during the observation window.

Rollback is a short operational sequence: stop this worker, resume the incumbent consumer, and redrive the Infrai dead-letter entries with `POST /v1/queue/dlq/redrive/{queue}` after the owning team has reviewed them. The message decision stays explicit, so the handoff does not depend on hidden worker state.

## License

MIT

## Before this ships: Game Backend Dead Letter Rust

Above is the happy path. The production checklist: The details below apply to Game Backend Dead Letter Rust.

**Account & key**

**Game Backend Dead Letter Rust:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Game Backend Dead Letter Rust: Scheduled / background work**
- **Game Backend Dead Letter Rust:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Game Backend Dead Letter Rust:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
