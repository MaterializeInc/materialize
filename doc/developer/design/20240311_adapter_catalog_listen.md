# Adapter Catalog Listen

## The Problem

Currently, in response to certain events (mostly DDL) the coordinator/adapter takes the following
actions in no particular order:

    - Update the durable catalog state.
    - Update the in-memory catalog state.
    - Append updates to builtin tables.
    - Send commands to controllers.

We would like to structure the coordinator/adapter in such a way that it can listen to changes made
by other coordinators/adapters. In order to facilitate this in-memory catalog state updates,
builtin table updates, and controller commands should be created in response to a change to the
durable catalog state.

## Success Criteria

All `sequence_X` methods that correspond to DDL should be structured in the following way:

1. Keep doing the same thing before the catalog transaction.
2. Perform catalog the catalog transaction which only updates the durable catalog state.
3. Retrieve all changes to the durable catalog.
4. Update in-memory catalog state based on the changes.
5. Generate builtin table updates and controller commands from the changes.

`Catalog::open` should be structured in a way that all updates to in-memory state, builtin table
updates, and controller commands are generated directly from a list of updates from the durable
catalog.

## Out of Scope

- A catalog subscribe API.
- Multi-subscriber catalog.

## Solution Proposal

### Data Structures

```Rust
struct StateUpdate {
    kind: StateUpdateKind,
    diff: Diff,
}

enum StateUpdateKind {
    AuditLog(mz_audit_log::VersionedEvent),
    Cluster(mz_catalog::durable::objects::Cluster),
    ClusterReplica(mz_catalog::durable::objects::ClusterReplica),
    Comment(mz_catalog::durable::objects::Comment),
    Database(mz_catalog::durable::objects::Database),
    DefaultPrivilege(mz_catalog::durable::objects::DefaultPrivilege),
    IntrospectionSourceIndex(mz_catalog::durable::objects::IntrospectionSourceIndex),
    Item(mz_catalog::durable::objects::Item),
    Role(mz_catalog::durable::objects::Role),
    Schema(mz_catalog::durable::objects::Schema),
    StorageUsage(mz_audit_log::VersionedStorageUsage),
    SystemConfiguration(mz_catalog::durable::objects::SystemConfiguration),
    SystemObjectMapping(mz_catalog::durable::objects::SystemObjectMapping),
    SystemPrivilege(MzAclItem),
}

/// Enum of all commands that the Coordinator can send to the controller.
enum ControllerCommand {
    // ...
}
```

### Durable Catalog API

```Rust
trait ReadOnlyDurableCatalogState {
    // ...

    /// Fetches and returns all updates to the durable catalog state that have not yet been
    /// consumed.
    async fn sync(&mut self) -> Vec<StateUpdate>;

    // ...
}
```

### In-memory Catalog API

```Rust
impl Catalog {
    /// Fetches and applies all updates to the durable catalog state that have not yet been
    /// consumed.
    async fn sync(&mut self) -> (Vec<BuiltinTableUpdate>, Vec<ControllerCommand>) {
        let updates = self.storage().await.sync().await;
        self.apply_updates(updates);
    }

    /// Update in-memory catalog state and generate builtin table updates and controller commands. 
    fn apply_updates(&mut self, updates: Vec<StateUpdate>) -> (Vec<BuiltinTableUpdate>, Vec<ControllerCommand>) {
        let mut builtin_table_updates = Vec::new();
        let mut controller_commands = Vec::new();
        for StateUpdate { kind, diff } in updates {
            match (kind, diff) {
                // ...
            }
        }
        (builtin_table_updates, controller_commands)
    }
}
```

### Coordinator API

```Rust
impl Coordinator {
    /// Appends updates to builtin tables.
    async fn apply_builtin_table_updates(builtin_table_updates: Vec<BuiltinTableUpdate>) {
        // ...
    }

    /// Sends commands to the controller.
    async fn apply_controller_commands(controller_commands: Vec<ControllerCommand>) {
        // ...
    }
}
```

### Coordinator Bootstrap Psuedo Code

Note: This incorrectly simplifies the current structure of the code and combines some logic from
`mz_adapter::coord::serve`, `Catalog::open`, and `Coordinator::bootstrap` for ease of reading.

```Rust
impl Coordinator {
    async fn serve(&mut self) {
        // ... 
        let durable_catalog_state = self.open_durable_catalog_state().await;
        let catalog = Catalog::new(durable_catalog_state);
        // Get retractions of current builtin table contents.  
        let mut builtin_table_updates = self.get_retractions_for_all_builtin_table_contents();
        let (new_builtin_table_updates, controller_commands) = catalog.sync().await;
        builtin_table_updates.extend(new_builtin_table_updates);
        self.apply_builtin_table_updates(builtin_table_updates).await;
        self.apply_controller_commands(controller_commands).await;
        // ...
    }
}
```

### Coordinator Sequence Psuedo Code

```Rust
impl Coordinator {
    pub(crate) async fn sequence_plan(
        &mut self,
        mut ctx: ExecuteContext,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) -> LocalBoxFuture<'_, ()> {
        // ...
        // IMPORTANT: This may trigger a catalog transaction which only updates the durable catalog.
        self.sequence_X(plan);
        // ...
        let (builtin_table_updates, controller_commands) = self.catalog.sync().await;
        self.apply_builtin_table_updates(builtin_table_updates).await;
        self.apply_controller_commands(controller_commands).await;
    }
}
```

### Fencing

Fencing will not change as part of this work. That means that a new writer will fence out all
existing readers and writers.

## Open questions

- Is this a good incremental step towards multi-subscriber or should we go right to
  multi-subscriber?
- Should `StateUpdate`s contain timestamps? I.e., while consuming updates from the durable catalog,
  should the in-memory catalog care at all about timestamps?  
