# Contributing to Splitter

Thank you for your interest in contributing to Splitter! This document provides
guidelines and information for contributors.

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected
to uphold this code. Please report unacceptable behavior to the project maintainers.

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates.
When creating a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior vs. actual behavior
- Your environment (OS, Go version)
- Relevant code snippets or error messages
- If possible, a minimal reproduction case

### Suggesting Features

Feature requests are welcome! Please provide:

- A clear description of the feature
- The use case / problem it solves
- Example usage if applicable
- Any implementation ideas you have

### Pull Requests

1. **Fork and clone** the repository
2. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-description
   ```
3. **Make your changes** following the code style guidelines below
4. **Write tests** for new functionality
5. **Run the test suite** to ensure nothing is broken:
   ```bash
   GOEXPERIMENT=synctest go test -v ./...
   bazel test //...
   ```
6. **Commit your changes** with a clear commit message
7. **Push** to your fork and open a Pull Request

## Development Setup

### Prerequisites

- Go 1.24.12
- Bazel 9.0.0 or higher

### Building

```bash
# Build all modules
bazel build //...
GOEXPERIMENT=synctest go build -v ./...
```

### Running Tests

```bash
# Run all tests
bazel test //...
GOEXPERIMENT=synctest go test -v ./...
```

### Updating Generated Protobuf Files

```bash
./bin/update-go-protos.sh
```

## Code Style Guidelines

### Go

- Follow [Google Go coding conventions](https://google.github.io/styleguide/go/)

### Documentation

- All public types should have documentation
- Provide code examples for complex APIs

### Testing

- Write unit tests for all new functionality
- Use descriptive test names that explain what is being tested

## Commit Messages

Write clear, concise commit messages:

- Use the imperative mood ("Add feature" not "Added feature")
- Keep the first line under 72 characters
- Reference issues when applicable (`Fixes #123`)

## Review Process

1. A maintainer will review your PR
2. They may request changes or ask questions
3. Once approved, a maintainer will merge your PR
4. Your contribution will be included in the next release

## Questions?

If you have questions about contributing, feel free to:

- Start a discussion in the repository
- Open an issue with your question

Thank you for contributing!
