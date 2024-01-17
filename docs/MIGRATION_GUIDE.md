# Migration Guide

## Migration Guide from v0.4.21 to v1.0.0

This guide details the necessary changes for migrating from version `0.4.21` to `1.0.0` of the library. Major updates have been introduced, resulting in breaking changes.

### Major Changes

#### Event Store Refactoring

- Change of Event Store Constructor:
  The `NewEventStore` function has been renamed to `NewEventStoreOnDynamoDB`. When migrating, replace all instances of `NewEventStore` with `NewEventStoreOnDynamoDB`.

  **Before:**
  ```go
  eventStore, err := NewEventStore(dynamodbClient, "journal", "snapshot", ...)
  ```

  **After:**
  ```go
  eventStore, err := NewEventStoreOnDynamoDB(dynamodbClient, "journal", "snapshot", ...)
  ```

- Interface Implementation:
  The `EventStore` has been changed to an interface, affecting how it is instantiated and interacted with in the code. Ensure your implementation aligns with the new interface structure.

#### New Event Store for Memory

- A new in-memory version of the Event Store, `EventStoreOnMemory`, has been introduced. This can be used for testing or scenarios where a DynamoDB-based store is not required.

  **Usage Example:**
  ```go
  eventStore := NewEventStoreOnMemory()
  ```

### Minor Changes

- Dependencies in `go.mod` have been updated. Ensure that your project's dependencies are compatible with these changes.

### General Advice

- Review any custom implementations that interact with the `EventStore`. Ensure that they comply with the new interface.
- Thoroughly test all parts of your application that interact with the Event Store to ensure that they function as expected with these changes.

For further assistance or clarification, refer to the library's updated documentation or reach out to the support community.
