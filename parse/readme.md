# parse Module

The `parse` module project provides functionality for interpreting SQL-like statements, enabling structured interactions with the underlying Bitcask-based database. It converts input commands into executable operations while ensuring correctness and validation.


## File Overview


### `scaner.go`
- **Lexical analysis**:
  - Breaks down input strings into smaller, meaningful tokens.
  - Helps the parser understand and process SQL-like commands.
  - Handles:
    - Keywords (`CREATE`, `INSERT`, `SELECT`, etc.).
    - Identifiers (e.g., table names, column names).
    - Operators (`=`, `<`, `>`, etc.).
    - Literals (e.g., strings, numbers).
  - Provides utilities for error reporting during tokenization.

### `parser.go`
- **Core parsing logic**:
  - Converts SQL-like statements into structured commands.
  - Supports key operations such as:
    - `CREATE TABLE`
    - `INSERT`
    - `SELECT`
    - `DELETE`
  - Validates syntax and command arguments for consistency.


## Features

1. **SQL Parsing**  
   - Converts SQL-like input strings into structured commands.
   - Ensures commands are correctly formatted and valid.

2. **Command Execution**  
   - Supports operations to manage tables (`CREATE TABLE`) and manipulate data (`INSERT`, `SELECT`, `DELETE`).

3. **Error Handling**  
   - Provides clear and meaningful errors for invalid syntax or unsupported operations.


## Notes

- The `parse` module is integral to the functionality of the `bitcask_go` database.
- It works in conjunction with the Bitcask storage layer to facilitate SQL-like interactions with key-value data.
- This module is intended for internal use and is not a full-featured SQL parser.
