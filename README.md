# ucu-m2-fp-wikistream

A functional-reactive integration and analytics service for **Wikipedia / Wikimedia Recent Changes**.

## What is implemented

- Wikimedia integration through the official Event Stream endpoint:
  - `https://stream.wikimedia.org/v2/stream/recentchange`
- Real-time streaming API for:
  - all recent changes,
  - one or many tracked users.
- User analytics API:
  - contribution growth series (`X=time`, `Y=total contributions`) with configurable granularity,
  - top topics the user contributed to,
  - contribution types (`typo_editing` vs `content_addition`) as absolute counts.
- Aggregated analytics:
  - most active user for `year | month | day`,
  - top 10 topics with most typo editings,
  - optional common mistake words mined from typo-related comments.

## FRP approach and architecture

This project treats Wikimedia changes as an **event stream** and uses a reactive pipeline:

1. Ingest SSE events from Wikimedia.
2. Map payloads to immutable `ChangeEvent` values.
3. Push events through a reactive stream bus.
4. Fold events into analytics projections (`InMemoryAnalyticsStore`).
5. Expose projections via query endpoints and publish raw events via SSE.

Detailed architecture and sequence diagram are in [`docs/architecture.md`](docs/architecture.md).

## API endpoints

- `GET /stream/recent-changes`
- `GET /stream/users?users=UserA,UserB`
- `POST /users/track`
- `GET /users/{user}/stats?granularity=hour|day|month|year`
- `GET /analytics/active-user?period=day|month|year`
- `GET /analytics/top-typo-topics`
- `GET /analytics/common-mistakes`

### Track users payload

```json
{
  "users": ["UserA", "UserB"]
}
```

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
uvicorn app.main:app --reload
```

Then open `http://127.0.0.1:8000/docs`.

## Testing

```bash
pytest
```
