# GEMINI Instructions

This document provides foundational mandates and guidelines for Gemini CLI when working on the Wikipedia Real-Time Stream Analyzer project. These instructions take precedence over general defaults.

## 📖 Project Overview
This is a Functional Reactive Programming (FRP) implementation for analyzing Wikipedia/Wikimedia real-time change streams. It follows a specific "Professor's Pattern" using the `aiostream` library and custom Monad implementations for type safety and side-effect isolation.

## 🏗 Architectural Mandates

### 1. Functional Reactive Programming (FRP)
- **Library**: Use `aiostream>=0.5.2`.
- **Operators**: All streaming logic MUST use `@operator` and `@pipable_operator` decorators from `aiostream`.
- **Composition**: Chain operators using the pipe `|` operator for declarative pipelines.
- **Rate Limiting**: Always include `aiostream.pipe.delay(0.1)` (100ms) in streaming pipelines to prevent overwhelming the system and respect the Wikimedia SSE endpoint.

### 2. Monad-Driven Development
Located in `functional_utils.py`, these monads MUST be used for consistency:
- **Maybe Monad**: Use for handling optional values and parsing potentially malformed JSON from the stream.
- **Either Monad**: Use for type-safe error handling in query APIs and business logic (prefer `Left(error)`/`Right(result)` over raising exceptions).
- **IO Monad**: Use to encapsulate side-effects (e.g., printing to console, writing files, updating in-memory state). Side-effects should only be executed by calling `.run()`.

### 3. Immutability & State Management
- **Functional Updates**: In `user_statistics.py`, prefer returning new state instances rather than mutating existing objects.
- **Thread Safety**: Use `asyncio.Lock` when updating shared state within the `IO` monad execution.

## 🧪 Engineering Standards

- **Language**: Python 3.10+ (utilizing modern async/await syntax).
- **Testing**: `pytest` is the primary test runner. All new features or bug fixes MUST include corresponding tests in `test_functional.py`.
- **Code Style**: Adhere to existing patterns in the codebase. Ensure clear separation between pure functions and side-effect-heavy IO operations.
- **Documentation**: Maintain the detailed implementation notes in `README.md` for any major architectural changes.

## 🛠 Development Workflow

1. **Research & Reproduction**:
   - Before fixing a bug, reproduce it with a new test case in `test_functional.py`.
   - Study `functional_utils.py` before extending Monad capabilities.

2. **Execution**:
   - For UI/Visualization changes, verify the output in the `visualizations/` directory.
   - For streaming changes, use `aiostream.pipe.take(N)` to limit event counts during testing.

3. **Validation**:
   - Run `pytest test_functional.py -v` to ensure no regressions in Monad behavior or event processing.
   - Run `python main.py` and select Mode 5 to verify the `aiostream` pipeline integrity.

## 🚨 Safety & Security
- **Data Privacy**: Although the Wikipedia EventStream is public, avoid logging or storing personal identifying information (PII) beyond what is necessary for statistics.
- **Resource Management**: Always use `async with pipeline.stream() as streamer` to ensure proper resource cleanup of SSE connections.
