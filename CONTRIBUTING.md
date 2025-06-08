# Contributing to RedBase

Thank you for your interest in contributing to RedBase! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)

## Code of Conduct

We expect all contributors to follow our Code of Conduct. Please be respectful and considerate of others when contributing to the project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/yourusername/RedBase.git
   cd RedBase
   ```
3. Add the original repository as a remote:
   ```bash
   git remote add upstream https://github.com/originalowner/RedBase.git
   ```
4. Create a new branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

1. Make sure your branch is up to date with the main repository:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```
2. Make your changes
3. Run the tests to ensure your changes don't break existing functionality:
   ```bash
   cargo test
   ```
4. Commit your changes with a descriptive commit message:
   ```bash
   git commit -m "Add feature X"
   ```
5. Push your changes to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

## Pull Request Process

1. Create a pull request from your fork to the main repository
2. Fill out the pull request template with a description of your changes
3. Link any relevant issues
4. Wait for a maintainer to review your pull request
5. Address any feedback from the review
6. Once approved, your pull request will be merged

## Coding Standards

We follow Rust's standard coding conventions:

1. Use `rustfmt` to format your code:
   ```bash
   cargo fmt
   ```
2. Use `clippy` to catch common mistakes:
   ```bash
   cargo clippy
   ```
3. Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
4. Write clear, descriptive variable and function names
5. Add comments for complex logic
6. Use Rust's documentation comments (`///`) for public API

## Testing

All new features should include tests:

1. Unit tests for individual functions
2. Integration tests for API functionality
3. Make sure all existing tests pass before submitting a pull request

To run tests:
```bash
cargo test
```

## Documentation

Good documentation is essential for the project:

1. Update the README.md if you add new features or change existing ones
2. Add documentation comments to all public API functions
3. Update the usage guide in the docs directory
4. Include examples for new features

### Documentation Style

For documentation comments, follow these guidelines:

```rust
/// Brief description of the function
///
/// More detailed description if needed
///
/// # Arguments
///
/// * `arg1` - Description of arg1
/// * `arg2` - Description of arg2
///
/// # Returns
///
/// Description of the return value
///
/// # Examples
///
/// ```
/// let result = my_function(arg1, arg2);
/// assert_eq!(result, expected);
/// ```
fn my_function(arg1: Type1, arg2: Type2) -> ReturnType {
    // Implementation
}
```

## Project Structure

Understanding the project structure will help you contribute effectively:

- `src/api.rs` - Public API for the database
- `src/memstore.rs` - In-memory storage with WAL
- `src/storage.rs` - On-disk storage (SSTables)
- `src/filter.rs` - Filtering capabilities
- `src/aggregation.rs` - Aggregation functions
- `src/lib.rs` - Library exports
- `src/main.rs` - Example usage
- `tests/` - Integration tests

## Feature Requests and Bug Reports

If you have a feature request or found a bug:

1. Check the existing issues to see if it's already reported
2. If not, create a new issue with a clear description
3. For bugs, include steps to reproduce, expected behavior, and actual behavior
4. For feature requests, describe the feature and why it would be valuable

Thank you for contributing to RedBase!