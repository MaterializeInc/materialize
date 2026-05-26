-- Schema-level type definitions (ColType, ColSchema, Schema, EvalError)
-- reused by the indexed model.
import Mz.Schema

-- Indexed (GADT) model: Datum k, Expr sch k, Collection sch, etc.
import Mz.Indexed.Datum
import Mz.Indexed.PrimEval
import Mz.Indexed.Boolean
import Mz.Indexed.Laws
import Mz.Indexed.Strict
import Mz.Indexed.Variadic
import Mz.Indexed.Coalesce
import Mz.Indexed.Expr
import Mz.Indexed.Eval
import Mz.Indexed.Subst
import Mz.Indexed.MightError
import Mz.Indexed.OutputType
import Mz.Indexed.Schema
import Mz.Indexed.Collection
import Mz.Indexed.Equiv
import Mz.Indexed.EquivBounded
import Mz.Indexed.Legal
