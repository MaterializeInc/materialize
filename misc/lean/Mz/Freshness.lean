-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

/-!
# Freshness of a join of two ticking sources

A source with timestamp interval `i` assigns an event that arrives at wall-clock time `t` the
timestamp `ceilTo t i`, and its frontier at wall-clock time `w` is `floorTo w i`. Time is in
integer milliseconds and every tick is on time. A join of two sources is complete up to the
minimum of their frontiers.

An arrival at `t` counts as visible at `w` once both inputs' timestamps for it are below the
join's frontier. `visibleAt t` is the earliest such `w` (`visible_iff`). The worst-case delay
between arrival and visibility is `i + j - gcd i j`: `latency_le` bounds it and `latency_tight`
shows that some arrival reaches it. The bound is stated for the window convention of the
freshness histogram, which measures from the exclusive start of an assignment window, so a
single arrival's delay is one millisecond less.
-/

namespace Mz.Freshness

/-- The largest multiple of `i` at or below `w`, the frontier of a source ticking every `i`. -/
def floorTo (w i : Nat) : Nat := w / i * i

/-- The smallest multiple of `i` at or above `t`, the timestamp of an arrival at `t`. -/
def ceilTo (t i : Nat) : Nat := (t + i - 1) / i * i

theorem floorTo_le (w i : Nat) : floorTo w i ≤ w := Nat.div_mul_le_self w i

theorem dvd_ceilTo (t i : Nat) : i ∣ ceilTo t i := Nat.dvd_mul_left i _

/-- A multiple of `g` below a multiple `j` of `g` is at most `j - g`. -/
theorem add_le_of_dvd {g d j : Nat} (hd : g ∣ d) (hj : g ∣ j) (h : d < j) : d + g ≤ j := by
  obtain ⟨x, rfl⟩ := hd
  obtain ⟨y, rfl⟩ := hj
  have : x < y := Nat.lt_of_mul_lt_mul_left h
  have := Nat.mul_le_mul_left g (Nat.succ_le_of_lt this)
  rw [Nat.mul_succ] at this
  exact this

section Rounding

variable {i : Nat} (hi : 0 < i)
include hi

theorem mul_le_floorTo {k w : Nat} (h : k * i ≤ w) : k * i ≤ floorTo w i :=
  Nat.mul_le_mul_right i ((Nat.le_div_iff_mul_le hi).mpr h)

theorem le_ceilTo (t : Nat) : t ≤ ceilTo t i := by
  have h := Nat.div_add_mod (t + i - 1) i
  have := Nat.mod_lt (t + i - 1) hi
  unfold ceilTo
  rw [Nat.mul_comm] at h
  omega

theorem ceilTo_lt (t : Nat) : ceilTo t i < t + i := by
  have := Nat.div_mul_le_self (t + i - 1) i
  unfold ceilTo
  omega

theorem ceilTo_le_mul {t k : Nat} (h : t ≤ k * i) : ceilTo t i ≤ k * i := by
  have : (t + i - 1) / i < k + 1 := (Nat.div_lt_iff_lt_mul hi).mpr (by rw [Nat.succ_mul]; omega)
  exact Nat.mul_le_mul_right i (by omega)

theorem ceilTo_mul (k : Nat) : ceilTo (k * i) i = k * i :=
  Nat.le_antisymm (ceilTo_le_mul hi (Nat.le_refl _)) (le_ceilTo hi _)

theorem ceilTo_mono {s t : Nat} (h : s ≤ t) : ceilTo s i ≤ ceilTo t i :=
  ceilTo_le_mul hi (Nat.le_trans h (le_ceilTo hi t))

/-- Rounding up and rounding down are adjoint: `c` is at or below the frontier at `w` exactly
when the tick that covers `c` has happened by `w`. -/
theorem le_floorTo_iff (c w : Nat) : c ≤ floorTo w i ↔ ceilTo c i ≤ w := by
  constructor
  · intro h
    exact Nat.le_trans (ceilTo_le_mul hi h) (floorTo_le w i)
  · intro h
    exact Nat.le_trans (le_ceilTo hi c) (mul_le_floorTo hi h)

end Rounding

/-- The alignment term: from a tick of `i`, the next tick of `j` is at most `j - gcd i j` away,
because the two grids differ only by multiples of the gcd. -/
theorem alignment_le {i j : Nat} (hj : 0 < j) (k : Nat) :
    ceilTo (k * i) j + Nat.gcd i j ≤ k * i + j := by
  have hlt := ceilTo_lt hj (k * i)
  have hle := le_ceilTo hj (k * i)
  have hg : Nat.gcd i j ∣ ceilTo (k * i) j - k * i :=
    Nat.dvd_sub (Nat.dvd_trans (Nat.gcd_dvd_right i j) (dvd_ceilTo _ _))
      (Nat.dvd_mul_left_of_dvd (Nat.gcd_dvd_left i j) k)
  have := add_le_of_dvd hg (Nat.gcd_dvd_right i j) (by omega)
  omega

/-- Bézout's identity, which the core library does not provide. -/
theorem bezout (a b : Nat) : ∃ x y : Int, x * a + y * b = (Nat.gcd a b : Int) := by
  induction a, b using Nat.gcd.induction with
  | H0 n => exact ⟨0, 1, by simp⟩
  | H1 m n _ ih =>
    obtain ⟨x, y, h⟩ := ih
    refine ⟨y - x * (n / m : Nat), x, ?_⟩
    have hdm : (m : Int) * (n / m : Nat) + (n % m : Nat) = n := by
      exact_mod_cast Nat.div_add_mod n m
    rw [Nat.gcd_rec, ← h, ← hdm]
    grind

/-- Some tick of `i` lands exactly `gcd i j` after a tick of `j`, modulo `j`. -/
theorem exists_mul_mod (i : Nat) {j : Nat} (hj : 0 < j) :
    ∃ k, (k * i) % j = Nat.gcd i j % j := by
  obtain ⟨x, y, h⟩ := bezout i j
  have hj' : (j : Int) ≠ 0 := by omega
  refine ⟨(x % j).toNat, ?_⟩
  have hx : (((x % j).toNat : Nat) : Int) = x % j := Int.toNat_of_nonneg (Int.emod_nonneg x hj')
  apply Int.ofNat_inj.mp
  rw [Int.natCast_emod, Int.natCast_emod, Int.natCast_mul, hx, Int.mul_emod,
    Int.emod_emod_of_dvd _ (Int.dvd_refl _), ← Int.mul_emod, ← h]
  simp

section Join

variable {i j : Nat} (hi : 0 < i) (hj : 0 < j)

/-- The join's timestamp for an arrival at `t`: the later of the two inputs' timestamps. -/
def stamp (i j t : Nat) : Nat := max (ceilTo t i) (ceilTo t j)

/-- The join is complete up to the earlier of the two inputs' frontiers. -/
def frontier (i j w : Nat) : Nat := min (floorTo w i) (floorTo w j)

/-- An arrival at `t` is visible at `w` when both inputs' timestamps for it are complete. -/
def Visible (i j t w : Nat) : Prop := ceilTo t i ≤ frontier i j w ∧ ceilTo t j ≤ frontier i j w

/-- The wall-clock time at which an arrival at `t` becomes visible. -/
def visibleAt (i j t : Nat) : Nat := max (ceilTo (stamp i j t) i) (ceilTo (stamp i j t) j)

include hi hj

theorem visible_iff (t w : Nat) : Visible i j t w ↔ visibleAt i j t ≤ w := by
  have hli := le_floorTo_iff hi (stamp i j t) w
  have hlj := le_floorTo_iff hj (stamp i j t) w
  unfold Visible visibleAt frontier stamp
  unfold stamp at hli hlj
  constructor
  · intro ⟨h1, h2⟩
    exact Nat.max_le.mpr ⟨hli.mp (by omega), hlj.mp (by omega)⟩
  · intro h
    have h1 := hli.mpr (by omega)
    have h2 := hlj.mpr (by omega)
    omega

/-- One input's term of the latency: assignment up to `i`, then alignment up to `j - gcd`. -/
theorem cross_le (t : Nat) : ceilTo (ceilTo t i) j + Nat.gcd i j + 1 ≤ t + i + j := by
  have hc := ceilTo_lt hi t
  have := alignment_le (i := i) hj ((t + i - 1) / i)
  unfold ceilTo at hc ⊢
  unfold ceilTo at this
  omega

theorem latency_le (t : Nat) : visibleAt i j t + 1 + Nat.gcd i j ≤ t + i + j := by
  have hgi := Nat.le_of_dvd hi (Nat.gcd_dvd_left i j)
  have hgj := Nat.le_of_dvd hj (Nat.gcd_dvd_right i j)
  have ci := ceilTo_lt hi t
  have cj := ceilTo_lt hj t
  have xij := cross_le hi hj t
  have xji := cross_le hj hi t
  rw [Nat.gcd_comm] at xji
  have mi := ceilTo_mul hi ((t + i - 1) / i)
  have mj := ceilTo_mul hj ((t + j - 1) / j)
  unfold visibleAt stamp
  unfold ceilTo at mi mj
  -- The join's timestamp is one of the inputs' timestamps, and rounding it to its own grid is
  -- the identity.
  rcases Nat.le_total (ceilTo t i) (ceilTo t j) with h | h
  · rw [Nat.max_eq_right h]
    have : ceilTo (ceilTo t j) j = ceilTo t j := mj
    omega
  · rw [Nat.max_eq_left h]
    have : ceilTo (ceilTo t i) i = ceilTo t i := mi
    omega

theorem latency_tight : ∃ t, visibleAt i j t + 1 + Nat.gcd i j = t + i + j := by
  obtain ⟨k, hk⟩ := exists_mul_mod i hj
  -- Shift to a positive tick, so that the arrival just after the previous tick exists.
  let a := (k + j) * i
  have ha : a % j = Nat.gcd i j % j := by
    simp only [a, Nat.add_mul, Nat.add_mod, Nat.mul_comm j i, Nat.mul_mod_left, Nat.add_zero,
      Nat.mod_mod, hk]
  have hai : i ≤ a := by
    have : 1 * i ≤ (k + j) * i := Nat.mul_le_mul_right i (by omega)
    omega
  refine ⟨a - i + 1, Nat.le_antisymm (latency_le hi hj _) ?_⟩
  have hgj := Nat.le_of_dvd hj (Nat.gcd_dvd_right i j)
  -- The arrival just after the previous tick of `i` gets timestamp `a` on that input.
  have hci : ceilTo (a - i + 1) i = a := by
    apply Nat.le_antisymm (ceilTo_le_mul hi (by omega))
    have := le_ceilTo hi (a - i + 1)
    -- A multiple of `i` below `a` would be at most `a - i`.
    apply Nat.le_of_not_lt
    intro hlt
    have := add_le_of_dvd (dvd_ceilTo (a - i + 1) i) (Nat.dvd_mul_left i _) hlt
    omega
  -- From `a`, the next tick of `j` is exactly `j - gcd i j` away.
  have hcj : a + j ≤ ceilTo a j + Nat.gcd i j := by
    have hd := Nat.div_add_mod a j
    have := le_ceilTo hj a
    obtain ⟨q, hq⟩ : j ∣ ceilTo a j := dvd_ceilTo _ _
    rcases Nat.eq_or_lt_of_le hgj with he | hl
    · rw [he] at ha ⊢
      omega
    · rw [Nat.mod_eq_of_lt hl] at ha
      have hg0 : 0 < Nat.gcd i j := Nat.gcd_pos_of_pos_left j hi
      have : a / j < q := by
        have : j * (a / j) < j * q := by omega
        exact Nat.lt_of_mul_lt_mul_left this
      have : j * (a / j + 1) ≤ j * q := Nat.mul_le_mul_left j this
      rw [Nat.mul_succ] at this
      omega
  have hs : a ≤ stamp i j (a - i + 1) := by
    unfold stamp
    rw [hci]
    exact Nat.le_max_left _ _
  have := ceilTo_mono hj hs
  unfold visibleAt
  have := Nat.le_max_right (ceilTo (stamp i j (a - i + 1)) i) (ceilTo (stamp i j (a - i + 1)) j)
  omega

/-- The 7s x 11s join from the freshness spike: the worst case is 17s. -/
example : ∃ t, visibleAt 7000 11000 t + 1 = t + 17000 := by
  obtain ⟨t, ht⟩ := latency_tight (i := 7000) (j := 11000) (by decide) (by decide)
  exact ⟨t, by
    have : Nat.gcd 7000 11000 = 1000 := by decide
    omega⟩

end Join

end Mz.Freshness
