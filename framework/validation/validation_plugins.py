# src/price_domain/defs/validation/validation_plugins.py

def validate_custom_threshold(df, column, threshold):
    bad = df[df[column] > threshold]

    if bad.empty:
        return True, {"threshold": threshold}

    return False, {
        "threshold": threshold,
        "failing_count": len(bad),
        "sample_rows": bad.head().to_markdown(),
    }