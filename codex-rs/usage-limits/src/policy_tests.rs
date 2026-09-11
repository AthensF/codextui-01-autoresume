use super::*;
use chrono::TimeZone;
use chrono::Utc;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[test]
fn deterministic_jitter_bounded_is_stable() {
    let jitter = DeterministicJitter::new(42);
    let d1 = jitter.bounded(Duration::from_secs(3));
    let d2 = jitter.bounded(Duration::from_secs(3));
    assert_eq!(d1, d2, "same seed must produce same jitter");
}

#[test]
fn deterministic_jitter_bounded_is_within_range() {
    for seed in 0..100 {
        let jitter = DeterministicJitter::new(seed);
        let d = jitter.bounded(Duration::from_secs(3));
        assert!(
            d <= Duration::from_secs(3),
            "jitter {d:?} exceeds max for seed {seed}"
        );
    }
}

#[test]
fn deterministic_jitter_fraction_is_stable() {
    let jitter = DeterministicJitter::new(7);
    let f1 = jitter.fraction(0.2);
    let f2 = jitter.fraction(0.2);
    assert_eq!(f1, f2, "same seed must produce same fraction");
}

#[test]
fn deterministic_jitter_fraction_is_within_range() {
    for seed in 0..100 {
        let jitter = DeterministicJitter::new(seed);
        let f = jitter.fraction(0.2);
        assert!(
            (0.8..=1.2).contains(&f),
            "fraction {f} out of range for seed {seed}"
        );
    }
}

#[test]
fn known_reset_next_check_adds_grace_and_jitter() {
    let policy = UsageLimitPolicy::default();
    let jitter = DeterministicJitter::new(0);
    let reset_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let next = policy.known_reset_next_check(reset_at, jitter);
    // next should be at least reset_at + 2s grace, at most reset_at + 5s.
    let min = reset_at + chrono::Duration::seconds(2);
    let max = reset_at + chrono::Duration::seconds(5);
    assert!(next >= min, "next {next} should be >= {min}");
    assert!(next <= max, "next {next} should be <= {max}");
}

#[test]
fn unknown_reset_probe_delay_first_probe_is_base() {
    let policy = UsageLimitPolicy::default();
    let jitter = DeterministicJitter::new(0);
    let delay = policy.unknown_reset_probe_delay(1, jitter);
    // Base is 60s, with ±20% jitter: [48, 72]
    assert!(
        delay >= Duration::from_secs(48),
        "delay {delay:?} too small"
    );
    assert!(
        delay <= Duration::from_secs(72),
        "delay {delay:?} too large"
    );
}

#[test]
fn unknown_reset_probe_delay_grows_exponentially() {
    let policy = UsageLimitPolicy::default();
    let jitter = DeterministicJitter::new(100);
    let d1 = policy.unknown_reset_probe_delay(1, jitter);
    let d2 = policy.unknown_reset_probe_delay(2, jitter);
    let d3 = policy.unknown_reset_probe_delay(3, jitter);
    // Each should be roughly double the previous (within jitter).
    assert!(d2 > d1, "d2 {d2:?} should be > d1 {d1:?}");
    assert!(d3 > d2, "d3 {d3:?} should be > d2 {d2:?}");
}

#[test]
fn unknown_reset_probe_delay_caps_at_max_interval() {
    let policy = UsageLimitPolicy::default();
    let jitter = DeterministicJitter::new(0);
    // Probe 10 should be capped at 1800s (with jitter: [1440, 2160]).
    let delay = policy.unknown_reset_probe_delay(10, jitter);
    assert!(
        delay <= Duration::from_secs(2160),
        "delay {delay:?} should be <= 2160s (1800 * 1.2)"
    );
}

#[test]
fn unknown_reset_wait_ceiling_adds_max_wait() {
    let policy = UsageLimitPolicy::default();
    let first = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let ceiling = policy.unknown_reset_wait_ceiling(first);
    let expected = first + chrono::Duration::seconds(7200);
    assert_eq!(ceiling, expected);
}

#[test]
fn saturating_duration_mul_pow2_doubles() {
    let d = Duration::from_secs(60);
    assert_eq!(saturating_duration_mul_pow2(d, 0), Duration::from_secs(60));
    assert_eq!(saturating_duration_mul_pow2(d, 1), Duration::from_secs(120));
    assert_eq!(saturating_duration_mul_pow2(d, 2), Duration::from_secs(240));
}

#[test]
fn scale_duration_clamps_negative_to_zero() {
    assert_eq!(
        scale_duration(Duration::from_secs(10), -1.0),
        Duration::ZERO
    );
    assert_eq!(scale_duration(Duration::from_secs(10), 0.0), Duration::ZERO);
}

#[test]
fn scale_duration_scales_correctly() {
    let result = scale_duration(Duration::from_secs(60), 1.5);
    assert_eq!(result, Duration::from_secs(90));
}
