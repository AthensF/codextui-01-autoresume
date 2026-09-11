//! Scheduling policy for usage-limit waits.
//!
//! Known-reset waits use a small fixed grace plus deterministic jitter:
//! ```text
//! next_check_at = expected_reset_at + 2 seconds + deterministic_jitter(0..=3 seconds)
//! ```
//!
//! Unknown-reset waits use exponential backoff with deterministic jitter:
//! ```text
//! delay(n) = min(base * 2^(n-1), max_interval) * (1 + deterministic_jitter(-0.2..=0.2))
//! ceiling = first_probe_at + max_wait
//! stop automatic probing when probe_count >= max_availability_checks
//!                                     OR now >= ceiling
//! then: ManualWait
//! ```

use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;

/// Fixed grace applied after a known reset timestamp before revalidation.
pub(crate) const KNOWN_RESET_GRACE: Duration = Duration::from_secs(2);

/// Maximum deterministic jitter applied after a known reset timestamp.
pub(crate) const KNOWN_RESET_JITTER_MAX: Duration = Duration::from_secs(3);

/// Internal jitter policy for unknown-reset probing: ±20%.
pub(crate) const UNKNOWN_RESET_JITTER_FRACTION: f64 = 0.2;

/// Deterministic jitter derived from a stable seed (root thread/generation).
///
/// The same seed always produces the same jitter value in `[0, max]` for
/// known-reset waits and in `[-fraction, +fraction]` for unknown-reset
/// waits. This keeps tests deterministic and prevents all clients from
/// retrying in lockstep.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeterministicJitter {
    /// Stable seed, typically derived from the root thread ID and generation.
    seed: u64,
}

impl DeterministicJitter {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Returns a deterministic duration in `[0, max]`.
    pub fn bounded(self, max: Duration) -> Duration {
        if max.is_zero() {
            return Duration::ZERO;
        }
        let max_nanos = max.as_nanos() as u64;
        let value = splitmix(self.seed.wrapping_mul(0x9E3779B97F4A7C15)) % (max_nanos + 1);
        Duration::from_nanos(value)
    }

    /// Returns a deterministic multiplier in `[1 - fraction, 1 + fraction]`.
    pub fn fraction(self, fraction: f64) -> f64 {
        // Map the seed into [0, 1) then into [-fraction, +fraction].
        let bits = splitmix(self.seed.wrapping_mul(0xD1B54A32D192ED03));
        let unit = (bits as f64) / (u64::MAX as f64);
        1.0 + (unit * 2.0 - 1.0) * fraction
    }
}

/// SplitMix64 hash — a simple, stable, well-distributed hash function.
fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Effective usage-limit policy after config resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageLimitPolicy {
    /// Whether to queue submissions when an authoritative snapshot says usage
    /// is unavailable.
    pub queue_when_limited: bool,
    /// Whether to automatically revalidate after a reset deadline.
    pub auto_resume: bool,
    /// Maximum number of usage-window crossings that may be automatically
    /// resumed. `0` permits queue/suspend visibility and manual retry but no
    /// automatic crossing.
    pub max_auto_resumes: u32,
    /// Maximum number of availability probes for unknown-reset 429s before
    /// transitioning to manual wait.
    pub max_availability_checks: u32,
    /// Base delay for the first unknown-reset probe and the exponential base
    /// for subsequent probes.
    pub unknown_reset_base_delay: Duration,
    /// Maximum interval between unknown-reset probes.
    pub unknown_reset_max_interval: Duration,
    /// Wall-clock ceiling for unknown-reset probing, measured from the first
    /// probe.
    pub unknown_reset_max_wait: Duration,
}

impl UsageLimitPolicy {
    /// Computes the next check time for a known reset timestamp.
    ///
    /// Returns `reset_at + grace + deterministic_jitter(0..=3s)`.
    pub fn known_reset_next_check(
        &self,
        reset_at: DateTime<Utc>,
        jitter: DeterministicJitter,
    ) -> DateTime<Utc> {
        let jitter_dur = jitter.bounded(KNOWN_RESET_JITTER_MAX);
        reset_at
            + chrono::Duration::seconds(KNOWN_RESET_GRACE.as_secs() as i64)
            + chrono::Duration::nanoseconds(jitter_dur.as_nanos() as i64)
    }

    /// Computes the delay for the `n`-th unknown-reset probe (1-indexed).
    ///
    /// `delay(n) = min(base * 2^(n-1), max_interval) * (1 + jitter(-0.2..=0.2))`
    pub fn unknown_reset_probe_delay(
        &self,
        probe_number: u32,
        jitter: DeterministicJitter,
    ) -> Duration {
        let base = self.unknown_reset_base_delay;
        let raw = if probe_number <= 1 {
            base
        } else {
            // base * 2^(n-1), saturating at max_interval
            let exponent = probe_number - 1;
            let scaled = saturating_duration_mul_pow2(base, exponent);
            if scaled > self.unknown_reset_max_interval {
                self.unknown_reset_max_interval
            } else {
                scaled
            }
        };
        let multiplier = jitter.fraction(UNKNOWN_RESET_JITTER_FRACTION);
        scale_duration(raw, multiplier)
    }

    /// Returns the wall-clock ceiling for unknown-reset probing, measured from
    /// the first probe time.
    pub fn unknown_reset_wait_ceiling(&self, first_probe_at: DateTime<Utc>) -> DateTime<Utc> {
        first_probe_at + chrono::Duration::seconds(self.unknown_reset_max_wait.as_secs() as i64)
    }
}

impl Default for UsageLimitPolicy {
    fn default() -> Self {
        Self {
            queue_when_limited: true,
            auto_resume: true,
            max_auto_resumes: 1,
            max_availability_checks: 6,
            unknown_reset_base_delay: Duration::from_secs(60),
            unknown_reset_max_interval: Duration::from_secs(1800),
            unknown_reset_max_wait: Duration::from_secs(7200),
        }
    }
}

/// Multiplies a duration by `2^exponent`, saturating at `Duration::MAX`.
fn saturating_duration_mul_pow2(dur: Duration, exponent: u32) -> Duration {
    let mut secs = dur.as_secs();
    let mut nanos = dur.subsec_nanos();
    for _ in 0..exponent {
        // Double nanos first, carrying into secs.
        nanos = nanos.saturating_mul(2);
        let carry = nanos / 1_000_000_000;
        nanos %= 1_000_000_000;
        secs = secs.saturating_mul(2).saturating_add(carry as u64);
    }
    Duration::new(secs, nanos)
}

/// Scales a duration by a float multiplier, clamping to non-negative.
fn scale_duration(dur: Duration, multiplier: f64) -> Duration {
    if multiplier <= 0.0 {
        return Duration::ZERO;
    }
    let nanos = dur.as_nanos() as f64 * multiplier;
    if !nanos.is_finite() || nanos >= u64::MAX as f64 {
        return Duration::MAX;
    }
    Duration::from_nanos(nanos as u64)
}

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;
