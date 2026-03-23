---
name: python-frp
description: Expert guidance for Python Functional Reactive Programming (FRP) and aiostream. Use when building or refactoring event-driven systems using Monads, immutable state snapshots, and declarative reactive pipelines.
---

# Python Functional Reactive Programming (FRP)

This skill provides procedural knowledge for building robust, type-safe, and side-effect-free reactive systems in Python using `aiostream` and functional primitives.

## 🏗 Architectural Principles

### 1. Functional Reactive Pipelines
All data flow must be declarative. Use the pipe `|` operator to chain transformations.

- **Sources**: Created with `@operator`. Always use `async with pipeline.stream() as streamer` for resource management.
- **Transformations**: Created with `@pipable_operator`. Use `aiostream.streamcontext(source)` to wrap the input stream.
- **Rate Limiting**: Always include `aiostream.pipe.delay(0.1)` when interacting with external SSE or WebSocket endpoints.

### 2. Monad-Driven Safety
Encapsulate complexity and side-effects using the standard Monad pattern:
- **Maybe**: For optional values and safe parsing. Map/FlatMap to transform without null checks.
- **Either**: For domain-specific error handling. `Left` for errors, `Right` for success.
- **IO**: For all side-effects (logging, DB, UI). Delay execution until the "end of the world" (`.run()`).

### 3. Immutable State Management
Never mutate global or local state variables.
- Use `aiostream.pipe.accumulate` to evolve state.
- The `accumulator` function should return a new instance of the state object.
- Pattern: `lambda state, event: state.update(event)` where `update` returns a `NewState`.

## 🛠 Common Patterns

### Custom Pipable Operator
```python
@pipable_operator
async def my_op(source: AsyncIterator[T], arg: Any) -> AsyncIterator[U]:
    async with aiostream.streamcontext(source) as streamer:
        async for item in streamer:
            # Transformation logic
            yield transformed_item
```

### Pure Functional Accumulation
```python
pipeline = (
    source_stream
    | parse_op.pipe()
    | aiostream.pipe.accumulate(
        lambda store, event: store.update(event),
        initializer=InitialStore()
    )
)
```

## 🧪 Best Practices
- **Purity**: Keep logic in operators pure. Move side-effects to the very end of the pipeline or wrap them in `IO`.
- **Typing**: Use `TypeVar` (T, U, TKey) for generic operators to maintain type safety.
- **Testing**: Test operators in isolation by using `aiostream.stream.iterate` as a source.
