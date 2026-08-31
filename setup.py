from setuptools import setup, find_packages

setup(
    name="jobscout",
    version="1.0.0",
    author="Hermes Studio",
    description="Async structured data collector from HTTP, files, and RSS",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["aiohttp>=3.8"],
    entry_points={
        "console_scripts": ["jobscout=jobscout.cli:main"],
    },
)
