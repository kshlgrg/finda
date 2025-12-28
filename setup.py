from setuptools import setup, find_packages

setup(
    name="finda",
    version="0.1.0",
    description="A unified financial data fetching engine for Stocks, Crypto, and Forex.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Kushal Garg",
    author_email="your.email@example.com",
    url="https://github.com/kshlgrg/finda",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "pandas",
        "python-dotenv",
        "ccxt",
        "alpaca-py",
        "dukascopy-python",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
