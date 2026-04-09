"""
Functional Programming Primitives

This module implements core functional programming concepts:
- Maybe/Option Monad: For handling optional values safely without null checks.
- Either Monad: For type-safe error handling and branching logic.
- IO Monad: For encapsulating and delaying side effects to maintain purity.
- Functor & Applicative patterns: For transforming and combining wrapped values.
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
    An abstract base class for the Maybe monad, used for handling optional values.
    
    The Maybe monad provides a way to represent a value that might be missing, 
    allowing for safe chaining of operations without explicit null checks.
    """
    
    @abstractmethod
    def map(self, f: Callable[[A], B]) -> 'Maybe[B]':
        """
        FUNCTOR: Transforms the value inside the Maybe if it exists.
        
        Args:
            f: A transformation function.
            
        Returns:
            A new Maybe instance containing the transformed value or Nothing.
        """
        pass
    
    @abstractmethod
    def flat_map(self, f: Callable[[A], 'Maybe[B]']) -> 'Maybe[B]':
        """
        MONAD: Chains computations that return a Maybe instance.
        
        Args:
            f: A function that returns a Maybe instance.
            
        Returns:
            The result of applying f to the internal value, or Nothing.
        """
        pass
    
    @abstractmethod
    def get_or_else(self, default: A) -> A:
        """
        Extracts the value if it exists, otherwise returns the provided default.
        
        Args:
            default: The value to return if this is Nothing.
            
        Returns:
            The internal value or the default.
        """
        pass
    
    @abstractmethod
    def is_some(self) -> bool:
        """
        Checks if the instance contains a value.
        
        Returns:
            True if this is Some, False if this is Nothing.
        """
        pass


class Some(Maybe[A]):
    """Represents a present value in the Maybe monad."""
    
    def __init__(self, value: A):
        """
        Initialize with a value.
        
        Args:
            value: The value to wrap.
        """
        self._value = value
    
    def map(self, f: Callable[[A], B]) -> Maybe[B]:
        """Apply function to the wrapped value."""
        try:
            return Some(f(self._value))
        except Exception:
            return Nothing()
    
    def flat_map(self, f: Callable[[A], Maybe[B]]) -> Maybe[B]:
        """Chain a Maybe-returning operation."""
        try:
            return f(self._value)
        except Exception:
            return Nothing()
    
    def get_or_else(self, default: A) -> A:
        """Return the wrapped value."""
        return self._value
    
    def is_some(self) -> bool:
        """Confirm value presence."""
        return True
    
    def __repr__(self):
        return f"Some({self._value})"


class Nothing(Maybe[A]):
    """Represents the absence of a value in the Maybe monad."""
    
    def map(self, f: Callable[[A], B]) -> Maybe[B]:
        """Mapping over Nothing always results in Nothing."""
        return Nothing()
    
    def flat_map(self, f: Callable[[A], Maybe[B]]) -> Maybe[B]:
        """Flat mapping over Nothing always results in Nothing."""
        return Nothing()
    
    def get_or_else(self, default: A) -> A:
        """Return the default value since no value is present."""
        return default
    
    def is_some(self) -> bool:
        """Confirm value absence."""
        return False
    
    def __repr__(self):
        return "Nothing()"


def maybe_from_optional(value: Optional[A]) -> Maybe[A]:
    """
    Convenience function to convert a Python Optional (Value or None) to a Maybe instance.
    
    Args:
        value: The value to convert.
        
    Returns:
        Some(value) if value is not None, else Nothing().
    """
    return Some(value) if value is not None else Nothing()


# ============================================================================
# EITHER MONAD - Error handling with values
# ============================================================================

class Either(Generic[A, B], ABC):
    """
    An abstract base class for the Either monad, used for type-safe error handling.
    
    Conventionally, 'Left' carries an error or failure message/object, 
    while 'Right' carries the successful result.
    """
    
    @abstractmethod
    def map(self, f: Callable[[B], C]) -> 'Either[A, C]':
        """
        FUNCTOR: Transforms the Right value if it exists.
        
        Args:
            f: A transformation function.
            
        Returns:
            A new Either instance with the transformed success value or the original failure.
        """
        pass
    
    @abstractmethod
    def flat_map(self, f: Callable[[B], 'Either[A, C]']) -> 'Either[A, C]':
        """
        MONAD: Chains computations that return an Either instance.
        
        Args:
            f: A function that returns an Either instance.
            
        Returns:
            The result of applying f to the Right value, or the original Left.
        """
        pass
    
    @abstractmethod
    def get_or_else(self, default: B) -> B:
        """
        Extracts the success value or returns a default.
        
        Args:
            default: Value to return if this is Left.
            
        Returns:
            The Right value or the default.
        """
        pass
    
    @abstractmethod
    def is_right(self) -> bool:
        """Checks if this is a success instance."""
        pass
    
    @abstractmethod
    def is_left(self) -> bool:
        """Checks if this is a failure instance."""
        pass


class Left(Either[A, B]):
    """Represents a failure or error value in the Either monad."""
    
    def __init__(self, value: A):
        """
        Initialize with a failure value.
        
        Args:
            value: The error or failure information.
        """
        self._value = value
    
    def map(self, f: Callable[[B], C]) -> Either[A, C]:
        """Left ignores transformations and stays Left."""
        return Left(self._value)
    
    def flat_map(self, f: Callable[[B], Either[A, C]]) -> Either[A, C]:
        """Left short-circuits the chain."""
        return Left(self._value)
    
    def get_or_else(self, default: B) -> B:
        """Return the default value."""
        return default
    
    def is_right(self) -> bool:
        """Confirm this is not a success instance."""
        return False
    
    def is_left(self) -> bool:
        """Confirm this is a failure instance."""
        return True
    
    def get_left(self) -> A:
        """Extract the failure value."""
        return self._value
    
    def __repr__(self):
        return f"Left({self._value})"


class Right(Either[A, B]):
    """Represents a success value in the Either monad."""
    
    def __init__(self, value: B):
        """
        Initialize with a success value.
        
        Args:
            value: The success data.
        """
        self._value = value
    
    def map(self, f: Callable[[B], C]) -> Either[A, C]:
        """Apply the transformation to the success value."""
        try:
            return Right(f(self._value))
        except Exception as e:
            return Left(str(e))
    
    def flat_map(self, f: Callable[[B], Either[A, C]]) -> Either[A, C]:
        """Chain another operation returning an Either."""
        try:
            return f(self._value)
        except Exception as e:
            return Left(str(e))
    
    def get_or_else(self, default: B) -> B:
        """Return the success value."""
        return self._value
    
    def is_right(self) -> bool:
        """Confirm success."""
        return True
    
    def is_left(self) -> bool:
        """Confirm not failure."""
        return False
    
    def get_right(self) -> B:
        """Extract success value."""
        return self._value
    
    def __repr__(self):
        return f"Right({self._value})"


# ============================================================================
# IO MONAD - Encapsulates side effects
# ============================================================================

class IO(Generic[A]):
    """
    The IO monad encapsulates side effects by delaying their execution.
    
    This allows side-effecting code (like DB updates or console output) to be 
    treated as values until explicitly triggered by the .run() method.
    """
    
    def __init__(self, effect: Callable[[], A]):
        """
        Initialize with a side-effecting function.
        
        Args:
            effect: A zero-argument function that performs a side effect.
        """
        self._effect = effect
    
    def map(self, f: Callable[[A], B]) -> 'IO[B]':
        """
        FUNCTOR: Transforms the result of the IO operation when it eventually runs.
        
        Args:
            f: A transformation function.
            
        Returns:
            A new IO instance wrapping the transformation.
        """
        def mapped_effect():
            return f(self._effect())
        return IO(mapped_effect)
    
    def flat_map(self, f: Callable[[A], 'IO[B]']) -> 'IO[B]':
        """
        MONAD: Chains another IO operation based on the result of the first.
        
        Args:
            f: A function that takes the result of this IO and returns another IO.
            
        Returns:
            A new IO instance representing the chained effects.
        """
        def chained_effect():
            result = self._effect()
            return f(result).run()
        return IO(chained_effect)
    
    def run(self) -> A:
        """
        Executes the encapsulated side effect.
        
        Returns:
            The result of the effect.
        """
        return self._effect()
    
    def __repr__(self):
        return f"IO(<effect>)"


def io_pure(value: A) -> IO[A]:
    """
    Wraps a pure value in an IO monad.
    
    Args:
        value: The value to wrap.
        
    Returns:
        An IO instance that returns the value when run.
    """
    return IO(lambda: value)


# ============================================================================
# APPLICATIVE pattern - Combining independent computations
# ============================================================================

class Applicative:
    """
    A utility class for the Applicative pattern, allowing the combination 
    of independent wrapped computations.
    """
    
    @staticmethod
    def apply_maybe(maybe_f: Maybe[Callable[[A], B]], maybe_a: Maybe[A]) -> Maybe[B]:
        """
        Applies a function wrapped in Maybe to a value wrapped in Maybe.
        
        Args:
            maybe_f: Maybe[Function]
            maybe_a: Maybe[Value]
            
        Returns:
            Some(f(a)) if both exist, else Nothing().
        """
        if maybe_f.is_some() and maybe_a.is_some():
            f = maybe_f.get_or_else(lambda x: x)
            a = maybe_a.get_or_else(None)
            return Some(f(a))
        return Nothing()
    
    @staticmethod
    def lift2_maybe(f: Callable[[A, B], C], ma: Maybe[A], mb: Maybe[B]) -> Maybe[C]:
        """
        Lifts a binary function to operate on two Maybe values.
        
        Args:
            f: A binary function.
            ma: Maybe value A.
            mb: Maybe value B.
            
        Returns:
            Some(f(a, b)) if both exist, else Nothing().
        """
        if ma.is_some() and mb.is_some():
            return Some(f(ma.get_or_else(None), mb.get_or_else(None)))
        return Nothing()
    
    @staticmethod
    def sequence_maybe(maybes: list[Maybe[A]]) -> Maybe[list[A]]:
        """
        Converts a list of Maybe values into a Maybe of a list.
        If any element is Nothing, the result is Nothing.
        
        Args:
            maybes: A list of Maybe instances.
            
        Returns:
            Some([values]) or Nothing().
        """
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
    Composes multiple functions from right to left.
    Example: compose(f, g, h)(x) is equivalent to f(g(h(x))).
    
    Args:
        *functions: Functions to compose.
        
    Returns:
        A new function representing the composition.
    """
    def composed(arg):
        result = arg
        for f in reversed(functions):
            result = f(result)
        return result
    return composed


def pipe(*functions: Callable) -> Callable:
    """
    Pipes multiple functions from left to right.
    Example: pipe(f, g, h)(x) is equivalent to h(g(f(x))).
    
    Args:
        *functions: Functions to pipe.
        
    Returns:
        A new function representing the pipe.
    """
    def piped(arg):
        result = arg
        for f in functions:
            result = f(result)
        return result
    return piped


def curry2(f: Callable[[A, B], C]) -> Callable[[A], Callable[[B], C]]:
    """
    Curries a two-argument function into a chain of one-argument functions.
    
    Args:
        f: A function taking two arguments.
        
    Returns:
        A curried version of the function.
    """
    def curried(a: A) -> Callable[[B], C]:
        def inner(b: B) -> C:
            return f(a, b)
        return inner
    return curried


def curry3(f: Callable[[A, B, C], Any]) -> Callable[[A], Callable[[B], Callable[[C], Any]]]:
    """
    Curries a three-argument function into a chain of one-argument functions.
    
    Args:
        f: A function taking three arguments.
        
    Returns:
        A curried version of the function.
    """
    def curried(a: A):
        def inner1(b: B):
            def inner2(c: C):
                return f(a, b, c)
            return inner2
        return inner1
    return curried
