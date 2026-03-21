"""
Functional Programming Primitives

This module implements core FP concepts:
- Maybe/Option Monad: for handling optional values
- Either Monad: for error handling
- IO Monad: for side effects
- Functor, Applicative patterns
"""

from typing import TypeVar, Generic, Callable, Any, Optional
from abc import ABC, abstractmethod

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')


# ============================================================================
# MAYBE MONAD - Handles optional/missing values
# ============================================================================

class Maybe(Generic[A], ABC):
    """
    Maybe monad for handling optional values.
    
    MONAD: Implements bind (flat_map) for chaining computations that may fail
    FUNCTOR: Implements map for transforming wrapped values
    """
    
    @abstractmethod
    def map(self, f: Callable[[A], B]) -> 'Maybe[B]':
        """FUNCTOR: Transform the value inside Maybe"""
        pass
    
    @abstractmethod
    def flat_map(self, f: Callable[[A], 'Maybe[B]']) -> 'Maybe[B]':
        """MONAD: Chain computations that return Maybe"""
        pass
    
    @abstractmethod
    def get_or_else(self, default: A) -> A:
        """Extract value or return default"""
        pass
    
    @abstractmethod
    def is_some(self) -> bool:
        """Check if value exists"""
        pass


class Some(Maybe[A]):
    """Represents a present value"""
    
    def __init__(self, value: A):
        self._value = value
    
    def map(self, f: Callable[[A], B]) -> Maybe[B]:
        """FUNCTOR: Apply function to wrapped value"""
        try:
            return Some(f(self._value))
        except Exception:
            return Nothing()
    
    def flat_map(self, f: Callable[[A], Maybe[B]]) -> Maybe[B]:
        """MONAD: Chain Maybe-returning operations"""
        try:
            return f(self._value)
        except Exception:
            return Nothing()
    
    def get_or_else(self, default: A) -> A:
        return self._value
    
    def is_some(self) -> bool:
        return True
    
    def __repr__(self):
        return f"Some({self._value})"


class Nothing(Maybe[A]):
    """Represents absence of value"""
    
    def map(self, f: Callable[[A], B]) -> Maybe[B]:
        """FUNCTOR: Nothing maps to Nothing"""
        return Nothing()
    
    def flat_map(self, f: Callable[[A], Maybe[B]]) -> Maybe[B]:
        """MONAD: Nothing flat_maps to Nothing"""
        return Nothing()
    
    def get_or_else(self, default: A) -> A:
        return default
    
    def is_some(self) -> bool:
        return False
    
    def __repr__(self):
        return "Nothing()"


def maybe_from_optional(value: Optional[A]) -> Maybe[A]:
    """Convert Python Optional to Maybe"""
    return Some(value) if value is not None else Nothing()


# ============================================================================
# EITHER MONAD - Error handling with values
# ============================================================================

class Either(Generic[A, B], ABC):
    """
    Either monad for error handling.
    Left = error/failure, Right = success
    
    MONAD: Implements flat_map for chaining computations that may fail
    FUNCTOR: Implements map for transforming Right values
    """
    
    @abstractmethod
    def map(self, f: Callable[[B], C]) -> 'Either[A, C]':
        """FUNCTOR: Transform Right value"""
        pass
    
    @abstractmethod
    def flat_map(self, f: Callable[[B], 'Either[A, C]']) -> 'Either[A, C]':
        """MONAD: Chain Either-returning operations"""
        pass
    
    @abstractmethod
    def get_or_else(self, default: B) -> B:
        """Extract Right value or return default"""
        pass
    
    @abstractmethod
    def is_right(self) -> bool:
        """Check if this is Right"""
        pass
    
    @abstractmethod
    def is_left(self) -> bool:
        """Check if this is Left"""
        pass


class Left(Either[A, B]):
    """Represents an error/failure"""
    
    def __init__(self, value: A):
        self._value = value
    
    def map(self, f: Callable[[B], C]) -> Either[A, C]:
        """FUNCTOR: Left ignores transformation"""
        return Left(self._value)
    
    def flat_map(self, f: Callable[[B], Either[A, C]]) -> Either[A, C]:
        """MONAD: Left short-circuits"""
        return Left(self._value)
    
    def get_or_else(self, default: B) -> B:
        return default
    
    def is_right(self) -> bool:
        return False
    
    def is_left(self) -> bool:
        return True
    
    def get_left(self) -> A:
        return self._value
    
    def __repr__(self):
        return f"Left({self._value})"


class Right(Either[A, B]):
    """Represents a success value"""
    
    def __init__(self, value: B):
        self._value = value
    
    def map(self, f: Callable[[B], C]) -> Either[A, C]:
        """FUNCTOR: Transform the Right value"""
        try:
            return Right(f(self._value))
        except Exception as e:
            return Left(str(e))
    
    def flat_map(self, f: Callable[[B], Either[A, C]]) -> Either[A, C]:
        """MONAD: Chain Either-returning operations"""
        try:
            return f(self._value)
        except Exception as e:
            return Left(str(e))
    
    def get_or_else(self, default: B) -> B:
        return self._value
    
    def is_right(self) -> bool:
        return True
    
    def is_left(self) -> bool:
        return False
    
    def get_right(self) -> B:
        return self._value
    
    def __repr__(self):
        return f"Right({self._value})"


# ============================================================================
# IO MONAD - Encapsulates side effects
# ============================================================================

class IO(Generic[A]):
    """
    IO monad for encapsulating side effects.
    
    MONAD: Delays execution until run() is called
    FUNCTOR: Implements map for transforming result
    """
    
    def __init__(self, effect: Callable[[], A]):
        self._effect = effect
    
    def map(self, f: Callable[[A], B]) -> 'IO[B]':
        """FUNCTOR: Transform the result of IO operation"""
        def mapped_effect():
            return f(self._effect())
        return IO(mapped_effect)
    
    def flat_map(self, f: Callable[[A], 'IO[B]']) -> 'IO[B]':
        """MONAD: Chain IO operations"""
        def chained_effect():
            result = self._effect()
            return f(result).run()
        return IO(chained_effect)
    
    def run(self) -> A:
        """Execute the side effect"""
        return self._effect()
    
    def __repr__(self):
        return f"IO(<effect>)"


def io_pure(value: A) -> IO[A]:
    """Wrap a pure value in IO"""
    return IO(lambda: value)


# ============================================================================
# APPLICATIVE pattern - Combining independent computations
# ============================================================================

class Applicative:
    """
    APPLICATIVE: Apply wrapped functions to wrapped values
    Useful for combining multiple independent Maybe/Either values
    """
    
    @staticmethod
    def apply_maybe(maybe_f: Maybe[Callable[[A], B]], maybe_a: Maybe[A]) -> Maybe[B]:
        """Apply a Maybe function to a Maybe value"""
        if maybe_f.is_some() and maybe_a.is_some():
            f = maybe_f.get_or_else(lambda x: x)
            a = maybe_a.get_or_else(None)
            return Some(f(a))
        return Nothing()
    
    @staticmethod
    def lift2_maybe(f: Callable[[A, B], C], ma: Maybe[A], mb: Maybe[B]) -> Maybe[C]:
        """
        APPLICATIVE: Lift a binary function to work with Maybe values
        Useful for combining two Maybe values
        """
        if ma.is_some() and mb.is_some():
            return Some(f(ma.get_or_else(None), mb.get_or_else(None)))
        return Nothing()
    
    @staticmethod
    def sequence_maybe(maybes: list[Maybe[A]]) -> Maybe[list[A]]:
        """Convert list of Maybe to Maybe of list"""
        results = []
        for m in maybes:
            if m.is_some():
                results.append(m.get_or_else(None))
            else:
                return Nothing()
        return Some(results)


# ============================================================================
# Helper Functions - Composition and currying
# ============================================================================

def compose(*functions: Callable) -> Callable:
    """
    Compose functions right to left: compose(f, g, h)(x) = f(g(h(x)))
    Demonstrates function composition from FP
    """
    def composed(arg):
        result = arg
        for f in reversed(functions):
            result = f(result)
        return result
    return composed


def pipe(*functions: Callable) -> Callable:
    """
    Pipe functions left to right: pipe(f, g, h)(x) = h(g(f(x)))
    More intuitive than compose for some use cases
    """
    def piped(arg):
        result = arg
        for f in functions:
            result = f(result)
        return result
    return piped


def curry2(f: Callable[[A, B], C]) -> Callable[[A], Callable[[B], C]]:
    """Curry a 2-argument function"""
    def curried(a: A) -> Callable[[B], C]:
        def inner(b: B) -> C:
            return f(a, b)
        return inner
    return curried


def curry3(f: Callable[[A, B, C], Any]) -> Callable[[A], Callable[[B], Callable[[C], Any]]]:
    """Curry a 3-argument function"""
    def curried(a: A):
        def inner1(b: B):
            def inner2(c: C):
                return f(a, b, c)
            return inner2
        return inner1
    return curried
