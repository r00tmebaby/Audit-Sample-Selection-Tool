# Minimal package initializer to avoid unused imports during static checks.
__version__ = "0.1.0"

# Optionally expose public API via lazy imports to avoid static unused warnings.


def __getattr__(name: str):
    if name == "clean_data":
        from .cleaner import clean_data

        return clean_data
    if name in {"generate_sample", "generate_sample_streaming"}:
        from .sampler import generate_sample, generate_sample_streaming

        return {
            "generate_sample": generate_sample,
            "generate_sample_streaming": generate_sample_streaming,
        }[name]
    if name == "generate_reports":
        from .reporter import generate_reports

        return generate_reports
    raise AttributeError(name)
