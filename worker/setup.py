"""Setup configuration for audit sampling tool."""

from pathlib import Path

from setuptools import find_packages, setup

# Read long description from README
readme_file = Path(__file__).parent / "README.md"
long_description = (
    readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""
)

setup(
    name="audit-sampling-tool",
    version="1.0.0",
    author="r00tmebaby",
    description="Production-ready audit sampling tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/r00tmebaby/audit-sampling-tool",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "pydantic>=2.0.0",
        "structlog>=24.1.0",
        "openpyxl>=3.1.0",
    ],
    entry_points={
        "console_scripts": [
            "audit-sample=main:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
