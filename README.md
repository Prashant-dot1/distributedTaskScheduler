# Rust Task Scheduler

A distributed task scheduling system implemented in Rust. The design of the system is as shown in the following diagram:

![System Design](./assets/design-img.png)

## Project Structure

- `scheduler_client`: Client library for interacting with the scheduler
- `scheduler_core`: Core scheduling logic and data structures
- `scheduler_service`: Scheduler service implementation
- `worker_service`: Worker service for executing tasks

## Getting Started

### Prerequisites

- Rust 1.70 or higher
- Cargo (Rust's package manager)
