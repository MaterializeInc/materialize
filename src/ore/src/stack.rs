// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Stack management utilities.

use std::cell::RefCell;
use std::error::Error;
use std::fmt;

/// The red zone is the amount of stack space that must be available on the
/// current stack in order for [`maybe_grow`] to call the supplied closure
/// without allocating a new stack.
///
/// We use a much larger red zone in debug builds because several functions have
/// been observed to have 32KB+ stack frames when compiled without
/// optimizations. In particular, match statements on large enums are
/// problematic, because *each arm* of the match statement gets its own
/// dedicated stack space. For example, consider the following function:
///
/// ```ignore
/// fn big_stack(input: SomeEnum) {
///     match input {
///         SomeEnum::Variant1 => {
///             let a_local = SomeBigType::new();
///         }
///         SomeEnum::Variant2 => {
///             let b_local = SomeBigType::new();
///         }
///         // ...
///         SomeEnum::Variant10 => {
///             let z_local = SomeBigType::new();
///         }
///     }
/// }
/// ```
///
/// In debug builds, the compiler will generate a stack frame that contains
/// space for 10 separate copies of `SomeBigType`. This can quickly result in
/// massive stack frames for perfectly reasonable code.
pub const STACK_RED_ZONE: usize = {
    #[cfg(debug_assertions)]
    {
        256 << 10 // 256KiB
    }
    #[cfg(not(debug_assertions))]
    {
        32 << 10 // 32KiB
    }
};

/// The size of any freshly allocated stacks. It was chosen to match the default
/// stack size for threads in Rust.
///
/// The default stack size is larger in debug builds to correspond to the the
/// larger [`STACK_RED_ZONE`].
pub const STACK_SIZE: usize = {
    #[cfg(debug_assertions)]
    {
        16 << 20 // 16MiB
    }
    #[cfg(not(debug_assertions))]
    {
        2 << 20 // 2 MiB
    }
};

/// Grows the stack if necessary before invoking `f`.
///
/// This function is intended to be called at manually instrumented points in a
/// program where arbitrarily deep recursion is known to happen. This function
/// will check to see if it is within `STACK_RED_ZONE` bytes of the end of the
/// stack, and if so it will allocate a new stack of at least `STACK_SIZE`
/// bytes.
///
/// The closure `f` is guaranteed to run on a stack with at least
/// `STACK_RED_ZONE` bytes, and it will be run on the current stack if there's
/// space available.
///
/// It is generally better to use [`CheckedRecursion`] to enforce a limit on the
/// stack growth. Not all recursive code paths support returning errors,
/// however, in which case unconditionally growing the stack with this function
/// is still preferable to panicking.
#[inline(always)]
pub fn maybe_grow<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    stacker::maybe_grow(STACK_RED_ZONE, STACK_SIZE, f)
}

/// A trait for types which support bounded recursion to prevent stack overflow.
///
/// The rather odd design of this trait allows checked recursion to be added to
/// existing mutually recursive functions without threading an explicit `depth:
/// &mut usize` parameter through each function. As long as there is an
/// existing context structure, or if the mutually recursive functions are
/// methods on a context structure, the [`RecursionGuard`] can be embedded
/// inside this existing structure.
///
/// # Examples
///
/// Consider a simple expression evaluator:
///
/// ```
/// # use std::collections::HashMap;
///
/// enum Expr {
///     Var { name: String },
///     Add { left: Box<Expr>, right: Box<Expr> },
/// }
///
/// struct Evaluator {
///     vars: HashMap<String, i64>,
/// }
///
/// impl Evaluator {
///     fn eval(&mut self, expr: &Expr) -> i64 {
///         match expr {
///             Expr::Var { name } => self.vars[name],
///             Expr::Add { left, right } => self.eval(left) + self.eval(right),
///         }
///     }
/// }
/// ```
///
/// Calling `eval` could overflow the stack and crash with a sufficiently large
/// `expr`. This is the situation `CheckedRecursion` is designed to solve, like
/// so:
///
/// ```
/// # use std::collections::HashMap;
/// # enum Expr {
/// #     Var { name: String },
/// #     Add { left: Box<Expr>, right: Box<Expr> },
/// # }
/// use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
///
/// struct Evaluator {
///     vars: HashMap<String, i64>,
///     recursion_guard: RecursionGuard,
/// }
///
/// impl Evaluator {
///     fn eval(&mut self, expr: &Expr) -> Result<i64, RecursionLimitError> {
///         // ADDED: call to `self.checked_recur`.
///         self.checked_recur_mut(|e| match expr {
///             Expr::Var { name } => Ok(e.vars[name]),
///             Expr::Add { left, right } => Ok(e.eval(left)? + e.eval(right)?),
///         })
///     }
/// }
///
/// impl CheckedRecursion for Evaluator {
///     fn recursion_guard(&self) -> &RecursionGuard {
///         &self.recursion_guard
///     }
/// }
/// ```
pub trait CheckedRecursion {
    /// Extracts a reference to the recursion guard embedded within the type.
    fn recursion_guard(&self) -> &RecursionGuard;

    /// Checks whether it is safe to recur and calls `f` if so.
    ///
    /// If the recursion limit for the recursion guard returned by
    /// [`CheckedRecursion::recursion_guard`] has been reached, returns a
    /// `RecursionLimitError`. Otherwise, it will call `f`, possibly growing the
    /// stack if necessary.
    ///
    /// Calls to this function must be manually inserted at any point that
    /// mutual recursion occurs.
    #[inline(always)]
    fn checked_recur<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Self) -> Result<T, E>,
        E: From<RecursionLimitError>,
    {
        self.recursion_guard().descend()?;
        let out = maybe_grow(|| f(self));
        self.recursion_guard().ascend();
        out
    }

    /// Like [`CheckedRecursion::checked_recur`], but operates on a mutable
    /// reference to `Self`.
    #[inline(always)]
    fn checked_recur_mut<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<RecursionLimitError>,
    {
        self.recursion_guard().descend()?;
        let out = maybe_grow(|| f(self));
        self.recursion_guard().ascend();
        out
    }
}

/// Tracks recursion depth.
///
/// See the [`CheckedRecursion`] trait for usage instructions.
#[derive(Default, Debug, Clone)]
pub struct RecursionGuard {
    depth: RefCell<usize>,
    limit: usize,
}

impl RecursionGuard {
    /// Constructs a new recursion guard with the specified recursion
    /// limit.
    pub fn with_limit(limit: usize) -> RecursionGuard {
        RecursionGuard {
            depth: RefCell::new(0),
            limit,
        }
    }

    fn descend(&self) -> Result<(), RecursionLimitError> {
        let mut depth = self.depth.borrow_mut();
        if *depth < self.limit {
            *depth += 1;
            Ok(())
        } else {
            Err(RecursionLimitError { limit: self.limit })
        }
    }

    fn ascend(&self) {
        *self.depth.borrow_mut() -= 1;
    }
}

/// A [`RecursionGuard`]'s recursion limit was reached.
#[derive(Clone, Debug)]
pub struct RecursionLimitError {
    limit: usize,
    // todo: add backtrace (say, bottom 20 frames) once `std::backtrace` stabilizes in Rust 1.65
}

impl fmt::Display for RecursionLimitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "exceeded recursion limit of {}", self.limit)
    }
}

impl Error for RecursionLimitError {}
