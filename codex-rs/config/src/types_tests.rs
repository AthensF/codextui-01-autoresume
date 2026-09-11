use super::*;
use pretty_assertions::assert_eq;

#[test]
fn deserialize_skill_config_with_name_selector() {
    let cfg: SkillConfig = toml::from_str(
        r#"
            name = "github:yeet"
            enabled = false
        "#,
    )
    .expect("should deserialize skill config with name selector");

    assert_eq!(cfg.name.as_deref(), Some("github:yeet"));
    assert_eq!(cfg.path, None);
    assert!(!cfg.enabled);
}

#[test]
fn deserialize_skill_config_with_path_selector() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let skill_path = tempdir.path().join("skills").join("demo").join("SKILL.md");
    let cfg: SkillConfig = toml::from_str(&format!(
        r#"
            path = {path:?}
            enabled = false
        "#,
        path = skill_path.display().to_string(),
    ))
    .expect("should deserialize skill config with path selector");

    assert_eq!(
        cfg,
        SkillConfig {
            path: Some(
                AbsolutePathBuf::from_absolute_path(&skill_path)
                    .expect("skill path should be absolute"),
            ),
            name: None,
            enabled: false,
        }
    );
}

#[test]
fn memories_config_clamps_count_limits_to_nonzero_values() {
    let config = MemoriesConfig::from(MemoriesToml {
        max_raw_memories_for_consolidation: Some(0),
        max_rollouts_per_startup: Some(0),
        ..Default::default()
    });

    assert_eq!(
        config,
        MemoriesConfig {
            max_raw_memories_for_consolidation: 1,
            max_rollouts_per_startup: 1,
            ..MemoriesConfig::default()
        }
    );
}

#[test]
fn memories_config_clamps_rate_limit_remaining_threshold() {
    let config = MemoriesConfig::from(MemoriesToml {
        min_rate_limit_remaining_percent: Some(101),
        ..Default::default()
    });
    assert_eq!(
        config,
        MemoriesConfig {
            min_rate_limit_remaining_percent: 100,
            ..MemoriesConfig::default()
        }
    );

    let config = MemoriesConfig::from(MemoriesToml {
        min_rate_limit_remaining_percent: Some(-1),
        ..Default::default()
    });
    assert_eq!(
        config,
        MemoriesConfig {
            min_rate_limit_remaining_percent: 0,
            ..MemoriesConfig::default()
        }
    );
}

#[test]
fn memories_version_selects_pipeline_without_changing_other_defaults() {
    for (source, version) in [
        ("", MemoryVersion::V1),
        ("version = \"v2\"", MemoryVersion::V2),
    ] {
        let parsed: MemoriesToml = toml::from_str(source).expect("parse memories config");
        assert_eq!(
            MemoriesConfig::from(parsed),
            MemoriesConfig {
                version,
                ..Default::default()
            }
        );
    }
    assert!(toml::from_str::<MemoriesToml>("version = \"v3\"").is_err());
}

#[test]
fn usage_limits_config_uses_defaults_when_unset() {
    let parsed: UsageLimitsToml = toml::from_str("").expect("parse empty usage limits config");
    assert_eq!(
        UsageLimitsConfig::from(parsed),
        UsageLimitsConfig::default()
    );
}

#[test]
fn usage_limits_config_applies_explicit_overrides() {
    let parsed: UsageLimitsToml = toml::from_str(
        r#"
            queue_when_limited = false
            auto_resume = false
            max_auto_resumes = 3
            max_availability_checks = 10
            unknown_reset_base_delay_seconds = 30
            unknown_reset_max_interval_seconds = 600
            unknown_reset_max_wait_seconds = 3600
        "#,
    )
    .expect("parse explicit usage limits config");
    let config = UsageLimitsConfig::from(parsed);
    assert!(!config.queue_when_limited);
    assert!(!config.auto_resume);
    assert_eq!(config.max_auto_resumes, 3);
    assert_eq!(config.max_availability_checks, 10);
    assert_eq!(config.unknown_reset_base_delay_seconds, 30);
    assert_eq!(config.unknown_reset_max_interval_seconds, 600);
    assert_eq!(config.unknown_reset_max_wait_seconds, 3600);
}

#[test]
fn usage_limits_config_clamps_timing_fields_to_minimum_one_second() {
    let parsed: UsageLimitsToml = toml::from_str(
        r#"
            unknown_reset_base_delay_seconds = 0
            unknown_reset_max_interval_seconds = 0
            unknown_reset_max_wait_seconds = 0
        "#,
    )
    .expect("parse zeroed usage limits config");
    let config = UsageLimitsConfig::from(parsed);
    assert_eq!(config.unknown_reset_base_delay_seconds, 1);
    assert_eq!(config.unknown_reset_max_interval_seconds, 1);
    assert_eq!(config.unknown_reset_max_wait_seconds, 1);
}

#[test]
fn usage_limits_config_supports_zero_probes_and_zero_resumes() {
    let parsed: UsageLimitsToml = toml::from_str(
        r#"
            max_auto_resumes = 0
            max_availability_checks = 0
        "#,
    )
    .expect("parse zero caps usage limits config");
    let config = UsageLimitsConfig::from(parsed);
    assert_eq!(config.max_auto_resumes, 0);
    assert_eq!(config.max_availability_checks, 0);
}
