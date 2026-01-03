# Contributing to Rust vs Python Pipeline Benchmark

Thank you for your interest in contributing! This project welcomes contributions from the community.

## How to Contribute

### 1. Report Issues
- Found a bug? Open an issue with:
  - Clear description
  - Steps to reproduce
  - Expected vs actual behavior
  - System information (OS, Python/Rust versions)

### 2. Suggest Enhancements
- Have an idea? Open an issue with:
  - Use case description
  - Proposed solution
  - Potential impact

### 3. Submit Pull Requests

#### Setup Development Environment
```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/rust-python-pipeline-benchmark.git
cd rust-python-pipeline-benchmark

# Install Python dependencies
pip install -r python-pipeline/requirements.txt

# Build Rust project
cd rust-pipeline && cargo build
```

#### Making Changes
1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Test thoroughly
4. Commit with clear messages
5. Push and create a pull request

#### Code Style
- **Python**: Follow PEP 8
- **Rust**: Use `cargo fmt` and `cargo clippy`
- Add comments for complex logic
- Update documentation as needed

## Areas for Contribution

### High Priority
- [ ] Add Polars implementation for comparison
- [ ] Add Apache Spark implementation
- [ ] Docker Compose setup for easy testing
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] More comprehensive benchmarks

### Medium Priority
- [ ] Support for Parquet files
- [ ] Visualization of benchmark results
- [ ] Memory profiling tools
- [ ] Additional data quality scenarios

### Low Priority
- [ ] Web UI for running benchmarks
- [ ] Cloud deployment scripts (AWS, GCP, Azure)
- [ ] Jupyter notebooks with analysis

## Questions?

Open an issue or reach out to suryawanshiaditya159@gmail.com

Thank you for contributing! 🚀
