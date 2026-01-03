# 🚀 Rust vs Python Data Pipeline Benchmark

**40× faster, 95% less memory: A real-world comparison of Python+Pandas vs Rust+DuckDB**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Rust 1.70+](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org/)

This repository contains the complete code and benchmarks from the article **"I Rewrote Our Python Data Pipeline in Rust + DuckDB: 40× Faster, 95% Less Memory"**.

## 📊 Performance Results

| Metric | Python + Pandas | Rust + DuckDB | Improvement |
|--------|----------------|---------------|-------------|
| **Processing Time** | 2h 47min | 4min 12sec | **40× faster** |
| **Peak Memory** | 11.8 GB | 580 MB | **95% less** |
| **Monthly Cost** | $847 | $280 | **67% savings** |
| **CPU Usage** | 95% (single core) | 85% (multi-core) | Better utilization |

## 🎯 What's Inside

- **Python Implementation**: Original Pandas-based pipeline
- **Rust Implementation**: Optimized Rust + DuckDB version
- **Benchmark Suite**: Reproducible performance tests
- **Sample Data Generator**: Create realistic test datasets
- **Docker Support**: Run everything in containers
- **Detailed Documentation**: Step-by-step guides

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Rust 1.70+ (optional, for Rust version)
- Docker (optional, for containerized runs)

### 1. Clone the Repository

```bash
git clone https://github.com/suryawanshiaditya159-coder/rust-python-pipeline-benchmark.git
cd rust-python-pipeline-benchmark
```

### 2. Generate Sample Data

```bash
# Generate 1GB of sample CSV files (adjust size as needed)
python scripts/generate_data.py --size 1GB --output data/
```

### 3. Run Python Version

```bash
cd python-pipeline
pip install -r requirements.txt
python pipeline.py
```

### 4. Run Rust Version

```bash
cd rust-pipeline
cargo build --release
cargo run --release
```

### 5. Run Benchmarks

```bash
python scripts/benchmark.py --runs 5
```

## 📁 Repository Structure

```
.
├── python-pipeline/          # Python + Pandas implementation
│   ├── pipeline.py          # Main pipeline script
│   ├── requirements.txt     # Python dependencies
│   └── README.md           # Python-specific docs
│
├── rust-pipeline/           # Rust + DuckDB implementation
│   ├── src/
│   │   └── main.rs         # Main pipeline code
│   ├── Cargo.toml          # Rust dependencies
│   └── README.md           # Rust-specific docs
│
├── scripts/
│   ├── generate_data.py    # Sample data generator
│   ├── benchmark.py        # Performance benchmarking
│   └── compare.py          # Output validation
│
├── data/                    # Generated CSV files (gitignored)
├── results/                 # Benchmark results
├── docker/                  # Docker configurations
│   ├── Dockerfile.python
│   └── Dockerfile.rust
│
└── docs/
    ├── METHODOLOGY.md      # Benchmark methodology
    ├── MIGRATION.md        # Migration guide
    └── FAQ.md              # Common questions
```

## 🔬 Running Your Own Benchmarks

### Small Dataset (Quick Test)

```bash
# Generate 100MB of data
python scripts/generate_data.py --size 100MB

# Run benchmark
python scripts/benchmark.py --dataset small
```

### Medium Dataset (Realistic Test)

```bash
# Generate 5GB of data
python scripts/generate_data.py --size 5GB

# Run benchmark
python scripts/benchmark.py --dataset medium --runs 3
```

### Large Dataset (Production Scale)

```bash
# Generate 50GB of data (requires ~60GB free space)
python scripts/generate_data.py --size 50GB

# Run benchmark
python scripts/benchmark.py --dataset large --runs 3
```

## 🐳 Docker Quick Start

Run everything in Docker without installing dependencies:

```bash
# Build images
docker-compose build

# Generate data
docker-compose run datagen

# Run Python pipeline
docker-compose run python-pipeline

# Run Rust pipeline
docker-compose run rust-pipeline

# Run benchmarks
docker-compose run benchmark
```

## 📈 Understanding the Results

The benchmark script generates:

- **Performance metrics**: Time, memory, CPU usage
- **Output validation**: Ensures both pipelines produce identical results
- **Cost analysis**: AWS EC2 cost projections
- **Visualization**: Charts comparing performance

Results are saved to `results/` directory with timestamps.

## 🛠️ Customization

### Modify Data Schema

Edit `scripts/generate_data.py` to change:
- Number of columns
- Data types
- File sizes
- Number of files

### Adjust Pipeline Logic

Both implementations support:
- Custom aggregations
- Different file formats (CSV, Parquet)
- Filtering logic
- Output formats

See individual README files in `python-pipeline/` and `rust-pipeline/` for details.

## 📚 Learn More

- **[Methodology](docs/METHODOLOGY.md)**: How we measured performance
- **[Migration Guide](docs/MIGRATION.md)**: Step-by-step migration process
- **[FAQ](docs/FAQ.md)**: Common questions and answers
- **[Article](https://medium.com/@suryawanshiaditya159)**: Full story behind this project

## 🤝 Contributing

Contributions welcome! Areas of interest:

- [ ] Add Polars implementation for comparison
- [ ] Add Spark implementation
- [ ] More realistic data scenarios
- [ ] Additional benchmark metrics
- [ ] Performance optimizations

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- **DuckDB Team**: For building an amazing analytical database
- **Rust Community**: For excellent tooling and libraries
- **Pandas Team**: For years of reliable data processing

## 📧 Contact

- **Author**: Aditya Suryawanshi
- **Email**: suryawanshiaditya159@gmail.com
- **Article**: [Read the full story](https://medium.com/@suryawanshiaditya159)

## ⭐ Star This Repo

If this helped you optimize your data pipelines, please star this repository and share it with your team!

---

**Note**: Performance results will vary based on your hardware, data characteristics, and specific use case. Always benchmark with your own data before making production decisions.