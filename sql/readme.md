# `sql` Module - Mini Bitcask

The `sql` folder within the `mini_bitcask` project provides a lightweight SQL-like abstraction layer built on top of the Bitcask key-value store. It simulates fundamental database functionalities such as table creation, data manipulation, and query operations.

---

## Features

1. **Table Schema Management**:
   - Define table schemas with support for multiple data types, including:
     - `INTEGER`
     - `STRING`
     - `FLOAT`
     - `BOOLEAN`
     - `DATE`
     - `TIMESTAMP`
   - Schema validation ensures consistency across operations.

2. **Core SQL-like Operations**:
   - **Create Table**: Define a structured schema for each table.
   - **Insert**: Add rows while maintaining schema integrity.
   - **Select**: Query data with support for conditions and specific column selection.
   - **Update**: Modify existing rows based on primary keys.
   - **Delete**: Remove rows and clean up related indexes.

3. **Indexing for Fast Queries**:
   - Automatic primary key indexing.
   - Optional indexing on specific columns for optimized lookups.

4. **Persistence**:
   - Built on top of the Bitcask engine to ensure data durability.

5. **Extensibility**:
   - Easily extendable to support additional SQL-like features or integrate with distributed systems.


## Future Enhancements

- **Joins**: Support for basic table joins between multiple schemas.
- **Transactions**: Implementation of atomic, consistent, isolated, and durable (ACID) transactions.
- **Query Planning**: Addition of a query planner to optimize execution of complex queries.

