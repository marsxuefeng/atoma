## README.md (English Version - Default)

**[中文](README_zh.md)**

# Atoma: Distributed Concurrency Primitives for MongoDB

Atoma is a robust Java-based project that provides a set of distributed concurrency control primitives, similar to those found in `java.util.concurrent`, but implemented to operate reliably across multiple application instances using MongoDB as the underlying persistent and coordination store.

This project is designed to help developers build fault-tolerant distributed systems by offering reliable mechanisms for resource synchronization and coordination.

## Features

* **Distributed Mutual Exclusion Locks (Mutex Locks)**: Ensure that only one instance or thread can access a shared resource at a time, even across different JVMs or machines. Supports reentrancy.
* **Distributed Read-Write Locks**: Allow multiple readers or a single writer to access a shared resource, optimizing for read-heavy workloads in a distributed setting.
* **Distributed Semaphores**: Control access to a limited pool of resources in a distributed environment.
* **Distributed Barriers**: Synchronize multiple participants at a common point, ensuring all have reached a certain stage before proceeding. Includes support for double barriers.
* **Distributed Count-Down Latches**: Enable one or more processes to wait until a set of operations being performed by other processes in a distributed system completes.
* **Lease Management**: A mechanism for managing the lifecycle and ownership of distributed concurrency primitives.
* **MongoDB-backed Implementation**: Leverages MongoDB's transactional capabilities and Change Streams for state persistence, coordination, and real-time updates across distributed nodes.
* **Resilience and Fault-Tolerance**: Designed with fault-tolerance in mind, utilizing libraries like Failsafe for robust distributed operations.

## How it Works

Atoma implements its distributed primitives by managing their state within MongoDB collections. It uses:

* **MongoDB Transactions**: To ensure atomicity and consistency of operations that modify the state of the distributed primitives.
* **MongoDB Change Streams**: To enable distributed participants to react to state changes in real-time, facilitating efficient coordination without constant polling.

## External Libraries Used

The core functionality of Atoma relies on the following key external libraries:

* **MongoDB Java Driver**: For interacting with MongoDB.
* **Google Guava**: A set of core Java libraries from Google, providing utility functions.
* **Failsafe**: A resilience and fault-tolerance library for handling failures in distributed operations.
* **Lettuce**: A high-performance Redis client (though MongoDB is primary for core primitives, Redis might be used for other aspects or alternative implementations).
* **SLF4J & Logback**: For flexible and robust logging.
* **AutoValue & AutoService**: Google libraries for boilerplate code generation and service loading.

## Documentation

Comprehensive documentation, including usage examples and API references, is available on the project's dedicated documentation website, built with Next.js and Fumadocs.

## Getting Started

*(This section would typically include setup instructions, e.g., how to add it as a dependency in Maven/Gradle, basic code examples. Since I cannot execute code or provide dynamic content, this is a placeholder.)*

## Contributing

We welcome contributions! Please see our contributing guidelines (if available in the repository) for more details.

## License

*(Specify the project's license here, e.g., MIT, Apache 2.0)*

## Contact

For questions or feedback, please open an issue on the GitHub repository.