# Wikipedia Real-Time Stream Analyzer

A functional reactive programming implementation for analyzing Wikipedia/Wikimedia real-time change streams using **aiostream** library (following professor's pattern from lesson14.py).

## Overview

This project demonstrates functional reactive programming principles in Python by building a real-time Wikipedia change stream analyzer. It uses the **aiostream** library with `@operator` and `@pipable_operator` decorators to create composable functional pipelines, following the exact pattern taught in class.

## Installation

### Prerequisites

- Python 3.10 or higher
- pip

### Setup

```bash
# Install dependencies
pip install -r requirements.txt
```

**Dependencies:**
- `aiohttp>=3.9.0` - For HTTP streaming (Server-Sent Events)
- `aiostream>=0.5.2` - For functional reactive operators (professor's pattern)
- `matplotlib>=3.7.0` - For data visualization
- `pytest>=7.4.0` - For testing

**Note:** WikiMedia EventStream API does NOT require an API key - it's freely accessible!

---

## Quick Start

```bash
# Run the demo
python main.py

# Select a demo mode:
# Mode 3 - Statistical queries with visualizations (recommended)
# Mode 5 - AIOSTREAM pipeline demo (shows professor's pattern)

# Run tests
pytest test_functional.py -v
```

---

## Implementation Approach: Why This Design?

### 1. Why We Selected This Specific Approach (Professor's Pattern)

#### Following Professor's lesson14.py Pattern

Our implementation closely follows the functional reactive programming pattern demonstrated by the professor in `lesson14.py`. Here's why this approach was chosen:

**A. AIOStream Library (`aiostream>=0.5.2`)**

The professor's lesson used `aiostream` for functional reactive programming. We adopted this for several key reasons:

1. **Composable Operators**: The `@operator` and `@pipable_operator` decorators create pure, composable functional units that can be chained using the `|` operator - exactly as shown in class.

2. **Declarative Pipelines**: Instead of imperative loops, we declare data transformation pipelines:
   ```python
   pipeline = (
       fetch_wikipedia_changes()        # @operator - stream source
       | aiostream.pipe.delay(0.1)      # Rate limiting
       | deduplicator.pipe(...)          # @pipable_operator - transformation
       | aiostream.pipe.take(50)         # Limit
   )
   ```

3. **Prevents Rate Limiting Issues**: The professor experienced getting banned from GitHub due to excessive requests. We learned from this and implemented:
   - `aiostream.pipe.delay(0.1)` - 100ms delay between events
   - Event count limits
   - Deduplication to prevent reprocessing
   - Real-time stream (not bulk historical queries)

**B. Functional Programming Patterns**

Following the professor's teaching, we implemented three core monads:

1. **Maybe Monad**: For handling optional/missing values in event parsing
   - Eliminates null checks
   - Composable with `map()` and `flat_map()`
   - Gracefully handles malformed data from WikiMedia stream

2. **Either Monad**: For type-safe error handling in queries
   - Returns `Left(error)` or `Right(result)`
   - No hidden exceptions
   - Explicit error types in function signatures

3. **IO Monad**: For encapsulating side effects
   - Separates pure logic from side effects
   - Delays execution until `.run()` is called
   - Makes side effects explicit and testable

**C. Why WikiMedia EventStream API?**

1. **No API Key Required**: Unlike other APIs (GitHub, Twitter), WikiMedia is freely accessible
2. **Real-Time SSE Protocol**: Server-Sent Events provide push-based streaming (no polling)
3. **Rich Event Data**: Complete metadata for each Wikipedia edit
4. **Reliable Infrastructure**: Production-grade service from Wikimedia Foundation
5. **Educational-Friendly**: Designed for research and educational use

**D. Why In-Memory Storage?**

1. **Simplicity**: Focuses on functional programming patterns, not database complexity
2. **Performance**: Fast queries for demonstration purposes
3. **Extensibility**: Easy to replace with database later (same interface)
4. **Educational Focus**: Keeps attention on FP concepts, not infrastructure

### 2. Data Flow Sequence Diagram Explanation

The data flow follows a functional reactive pipeline pattern:

```
┌─────────────────────────────────────────────────────────────────┐
│                     WikiMedia EventStream API                    │
│                    (Server-Sent Events - SSE)                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Push: JSON events via SSE
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: @operator fetch_wikipedia_changes()                     │
│         - Opens SSE connection                                   │
│         - Yields raw event dictionaries                          │
│         - Async generator (reactive stream source)               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Emits: Dict[str, Any] (raw events)
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: aiostream.pipe.delay(0.1)                               │
│         - Rate limiting (100ms between events)                   │
│         - Prevents overwhelming the system                       │
│         - Learned from professor's GitHub ban experience         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Throttled: Same events, rate-limited
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: @pipable_operator deduplicator()                        │
│         - Stateful transformation                                │
│         - Tracks seen event IDs                                  │
│         - Filters out duplicates                                 │
│         - Uses streamcontext for state management                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Filtered: Unique events only
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 4: parse_event() -> Maybe[ParsedEvent]                     │
│         - MAYBE MONAD: Handles missing/malformed data            │
│         - Returns Some(event) if valid                           │
│         - Returns Nothing() if invalid                           │
│         - No exceptions thrown                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Safe: Maybe[ParsedEvent]
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: Maybe.map(extract_user_info)                            │
│         - FUNCTOR: Transform value inside Maybe context          │
│         - Only executes if Some (not Nothing)                    │
│         - Chains transformations safely                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Transformed: Maybe[UserInfo]
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 6: store.update_user_statistics() -> IO[None]              │
│         - IO MONAD: Wraps side effect                            │
│         - Doesn't execute immediately                            │
│         - Returns IO action                                      │
│         - Separates pure logic from side effects                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Delayed: IO[None] (not executed)
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 7: IO.run()                                                 │
│         - Executes the side effect NOW                           │
│         - Updates in-memory statistics                           │
│         - Thread-safe with locks                                 │
│         - Functional updates (returns new instances)             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Updated: Statistics in store
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ STEP 8: Query Functions -> Either[Error, Result]                │
│         - get_most_active_users()                                │
│         - get_top_typo_topics()                                  │
│         - get_user_contribution_series()                         │
│         - EITHER MONAD: Returns Left(error) or Right(result)     │
│         - No exceptions, explicit error handling                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points About This Flow:**

1. **Reactive**: Events flow through the pipeline as they arrive (push-based)
2. **Composable**: Each step is a pure function or operator
3. **Type-Safe**: Monads make errors explicit (Maybe, Either)
4. **Side-Effect Isolation**: IO monad separates pure logic from mutations
5. **Non-Blocking**: Async/await throughout for concurrent processing

### 3. Why This Specific Implementation Approach Was Selected

#### A. AIOStream Operators vs Plain Async Generators

**Decision:** Use `@operator` and `@pipable_operator` decorators

**Reasoning:**
1. **Professor's Pattern**: Matches the lesson14.py example exactly
2. **Better Composition**: Pipelines with `|` operator are more declarative than nested generators
3. **Reusability**: Each operator is a standalone, testable unit
4. **Standard Pattern**: Common in functional reactive programming (RxJS, Reactor, etc.)
5. **Built-in Utilities**: `aiostream.pipe.delay()`, `take()`, etc. are battle-tested

**Example Comparison:**

Traditional approach (nested generators):
```python
async def process():
    async for event in stream():
        if not is_duplicate(event):
            await asyncio.sleep(0.1)
            if count < limit:
                yield event
```

Professor's approach (pipeline composition):
```python
pipeline = (
    stream()
    | deduplicator.pipe(...)
    | aiostream.pipe.delay(0.1)
    | aiostream.pipe.take(limit)
)
```

The pipeline is more declarative, composable, and testable.

#### B. Monads for Error Handling

**Decision:** Implement Maybe, Either, IO monads from scratch

**Reasoning:**
1. **Educational Value**: Understanding monads by implementing them
2. **No External Dependencies**: Full control over implementation
3. **Type Safety**: Make errors explicit in function signatures
4. **Composability**: Chain operations with `map()` and `flat_map()`
5. **No Exceptions**: Functional approach to error handling

**Why Not Try/Except?**
- Exceptions are for exceptional cases, not control flow
- Hidden side effects (can throw anywhere)
- Not composable
- Breaks referential transparency

**Why Monads?**
- Explicit error handling in type signatures
- Composable with operators
- Predictable control flow
- Pure functional approach

#### C. WikiMedia EventStream (SSE) vs REST API

**Decision:** Use EventStream (Server-Sent Events) not Recent Changes REST API

**Reasoning:**
1. **Real-Time**: Push-based streaming, no polling needed
2. **Efficient**: One connection, events pushed as they occur
3. **Matches Professor's Pattern**: Similar to GitHub events stream in lesson14.py
4. **No Rate Limits**: Designed for streaming use cases
5. **Reactive**: Perfect for functional reactive programming

**Why Not REST API?**
- Would require polling (inefficient)
- Rate limits would be an issue
- Not truly "real-time"
- More complex state management

#### D. In-Memory Storage vs Database

**Decision:** Use in-memory data structures with functional updates

**Reasoning:**
1. **Simplicity**: Focus on FP patterns, not database operations
2. **Fast**: No I/O overhead for queries
3. **Immutable Updates**: Return new instances instead of mutating
4. **Functional Style**: Fits with FP principles
5. **Extensible**: Easy to swap with DB later (same interface)

**Functional Update Pattern:**
```python
def add_contribution(self, event):
    # Don't mutate self, return new instance
    new_stats = UserStatistics()
    new_stats.data = self.data.copy()
    new_stats.count = self.count + 1
    return new_stats
```

#### E. Test Strategy

**Decision:** Unit tests for monads and core logic, manual verification for streams

**Reasoning:**
1. **Testable**: Pure functions are easy to test
2. **Monads**: Comprehensive tests for Maybe, Either, IO
3. **Integration**: Manual verification with real Wikipedia stream
4. **Coverage**: Focus on business logic, not infrastructure

---

## Project Structure

```
├── functional_utils.py      # FP primitives (Maybe, Either, IO monads)
├── wiki_stream.py           # @operator and @pipable_operator for streaming
├── event_processor.py       # Event parsing with Maybe monad
├── user_statistics.py       # In-memory statistics with functional updates
├── query_api.py             # Query interface with Either monad
├── main.py                  # Demo application with visualizations
├── test_functional.py       # Unit tests (32 tests)
├── requirements.txt         # Dependencies
└── README.md                # This file
```

---

## Usage Examples

### Example 1: Basic Pipeline (Professor's Pattern)

```python
from wiki_stream import create_basic_pipeline

async def demo():
    # Create functional pipeline
    pipeline = create_basic_pipeline(limit=50)
    
    # Execute pipeline
    async with pipeline.stream() as streamer:
        async for event in streamer:
            print(event)

asyncio.run(demo())
```

### Example 2: Statistical Analysis

```python
from query_api import get_most_active_users, get_top_typo_topics
from user_statistics import TimePeriod

# Get most active users (last day)
result = get_most_active_users(period=TimePeriod.DAY, limit=10)

if result.is_right():  # Either monad
    for username, count in result.get_right():
        print(f"{username}: {count} contributions")

# Get top typo topics
result = get_top_typo_topics(limit=10)

if result.is_right():
    for topic, count in result.get_right():
        print(f"{topic}: {count} typo edits")
```

### Example 3: User Tracking

```python
from wiki_stream import create_user_tracking_pipeline

async def track_user():
    pipeline = create_user_tracking_pipeline("Username", limit=10)
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            print(f"User edited: {event['title']}")

asyncio.run(track_user())
```

---

## Functional Programming Patterns Used

### 1. Monads

**Maybe Monad** - Handles optional values:
```python
maybe_event = parse_event(raw_event)  # Returns Maybe[ParsedEvent]
maybe_user = maybe_event.map(extract_user_info)  # Functor
result = maybe_user.get_or_else(default_user)
```

**Either Monad** - Type-safe error handling:
```python
result = get_user_stats(username)  # Returns Either[str, Stats]
if result.is_right():
    stats = result.get_right()  # Success
else:
    error = result.get_left()   # Failure
```

**IO Monad** - Encapsulates side effects:
```python
io_action = store.update_statistics(event)  # Returns IO[None]
io_action.run()  # Execute side effect
```

### 2. Functors

Map operations over wrapped values:
```python
Some(5).map(lambda x: x * 2)  # Some(10)
Right(5).map(lambda x: x * 2)  # Right(10)
```

### 3. Applicatives

Combine independent computations:
```python
Applicative.lift2_maybe(add, Some(5), Some(3))  # Some(8)
```

### 4. Stream Operators (Professor's Pattern)

```python
@operator
async def fetch_wikipedia_changes():
    # Stream source
    yield events

@pipable_operator
async def deduplicator(source, key_extractor):
    # Transformation
    async with aiostream.streamcontext(source) as streamer:
        async for item in streamer:
            yield filtered_item

# Compose with |
pipeline = source() | transform1.pipe() | transform2.pipe()
```

---

## Demo Modes

### Mode 1: Basic Streaming
Shows first 50 Wikipedia edits in real-time.

### Mode 2: User Tracking
Monitor specific user's contributions.

### Mode 3: Statistical Queries ⭐
Collects 100 events, runs statistical queries, and **creates visualizations**:
- Line chart: Cumulative contributions over time
- Bar chart: Total contributions by user
- Saved to `visualizations/` directory

### Mode 4: Full Demo
Continuous streaming with live statistics.

### Mode 5: AIOSTREAM Pipeline Demo
Demonstrates professor's pattern with `@operator` and `@pipable_operator` decorators.

---

## Testing

```bash
# Run all tests
pytest test_functional.py -v

# Run specific test class
pytest test_functional.py::TestMaybeMonad -v
pytest test_functional.py::TestEitherMonad -v
pytest test_functional.py::TestIOMonad -v
```

**Test Coverage:**
- 32 unit tests
- Monad behavior (Maybe, Either, IO)
- Applicative operations
- Event parsing and processing
- Function composition

---

## Requirements Met

✅ **1. Real-Time Stream**: Get all recent changes as reactive stream  
✅ **2. User Tracking**: Track activity of specific users  
✅ **3.1 Contribution Series**: Time series with configurable granularity  
✅ **3.2 Top Topics**: Topics user contributed to most  
✅ **3.3 Contribution Types**: Typo editing vs content addition  
✅ **3.4 Most Active Users**: During YEAR|MONTH|DAY  
✅ **3.5 Top Typo Topics**: Top 10 topics with most typo edits  
✅ **Visualizations**: Line charts showing contribution growth  

---

## Key Takeaways

1. **Following Professor's Pattern**: Implementation uses `aiostream` library with `@operator` and `@pipable_operator` decorators, exactly as demonstrated in lesson14.py

2. **Functional Reactive Programming**: Async generators + monads + operators = composable, type-safe reactive streams

3. **Learning from Experience**: Rate limiting and deduplication prevent the ban issues the professor experienced with GitHub

4. **Monads for Clarity**: Maybe, Either, and IO monads make errors explicit and side effects isolated

5. **WikiMedia is Educational-Friendly**: No API key, no rate limits, perfect for learning FRP

6. **Visualization**: Real-time data analysis with professional charts for presentations

---

## License

Educational project for Functional Programming course.
