use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use std::{collections::HashMap, sync::Arc};
use taskchampion as tc;
use taskchampion::Uuid;

/// Covert a strong from Python into a Rust Uuid.
pub(crate) fn uuid2tc(s: impl AsRef<str>) -> PyResult<Uuid> {
    Uuid::parse_str(s.as_ref()).map_err(|_| PyValueError::new_err("Invalid UUID"))
}

/// Convert an anyhow::Error into a Python RuntimeError.
pub(crate) fn into_runtime_error(err: taskchampion::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

macro_rules! wrap{
    (
        $(
            $(#[$meta:meta])*
            $vis:vis async fn $name:ident(&mut self
                $(, $arg:ident : $typ:ty)* $(,)?
            ) $(-> $ret:ty)? ;
        )*
    ) => {
        $(
            wrap_inner! {
                $(#[$meta])*
                $vis async fn $name(&mut self $(, $arg: $typ)*) $(-> $ret)?;
            }
        )*
    };
}

macro_rules! wrap_inner {
    (
        $(#[$meta:meta])*
        $vis:vis async fn $name:ident(&mut self $(, $arg:ident : $typ:ty)*) $(-> $ret:ty)?;
    ) => {
        $(#[$meta])*
        $vis async fn $name(&mut self $(, $arg: $typ)*) $(-> $ret)? {
        match self {
            ReplicaWrapper::Sqlite(r) => r.$name($($arg),*).await,
            ReplicaWrapper::InMemory(r) => r.$name($($arg),*).await,
        }
        }
    };
}

/// Wrapper around tc::Replica to static-dispatch to implementations of
/// that type for both in-memory and on-disk storage.
pub(crate) enum ReplicaWrapper {
    Sqlite(tc::Replica<tc::storage::sqlite::SqliteStorage>),
    InMemory(tc::Replica<tc::storage::inmemory::InMemoryStorage>),
}

impl ReplicaWrapper {
    wrap! {

    pub(crate) async fn create_task(
        &mut self,
        uuid: Uuid,
        ops: &mut tc::Operations,
    ) -> Result<tc::Task, tc::Error>;

    pub(crate) async fn all_tasks(&mut self) -> Result<HashMap<Uuid, tc::Task>, tc::Error>;

    pub(crate) async fn all_task_data(&mut self) -> Result<HashMap<Uuid, tc::TaskData>, tc::Error>;

    pub(crate) async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>, tc::Error>;

    pub(crate) async fn working_set(&mut self) -> Result<tc::WorkingSet, tc::Error>;

    pub(crate) async fn dependency_map(
        &mut self,
        force: bool,
    ) -> Result<Arc<tc::DependencyMap>, tc::Error>;

    pub(crate) async fn get_task(&mut self, uuid: Uuid) -> Result<Option<tc::Task>, tc::Error>;

    pub(crate) async fn get_task_data(
        &mut self,
        uuid: Uuid,
    ) -> Result<Option<tc::TaskData>, tc::Error>;

    pub(crate) async fn commit_operations(
        &mut self,
        operations: tc::Operations,
    ) -> Result<(), tc::Error>;

    pub(crate) async fn sync(
        &mut self,
        server: &mut Box<dyn tc::Server>,
        avoid_snapshots: bool,
    ) -> Result<(), tc::Error>;

    pub(crate) async fn rebuild_working_set(&mut self, renumber: bool) -> Result<(), tc::Error>;

    pub(crate) async fn num_local_operations(&mut self) -> Result<usize, tc::Error>;

    pub(crate) async fn num_undo_points(&mut self) -> Result<usize, tc::Error>;

    pub(crate) async fn get_undo_operations(&mut self) -> Result<tc::Operations, tc::Error>;

    pub(crate) async fn commit_reversed_operations(
        &mut self,
        operations: tc::Operations,
    ) -> Result<bool, tc::Error>;

    pub(crate) async fn expire_tasks(&mut self) -> Result<(), tc::Error>;

    }
}
