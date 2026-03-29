use crate::task::TaskData;
use crate::util::{into_runtime_error, uuid2tc, ReplicaWrapper};
use crate::{AccessMode, DependencyMap, Operations, Task, WorkingSet};
use pyo3::prelude::*;
use std::collections::HashMap;
use taskchampion::storage::inmemory::InMemoryStorage;
use taskchampion::{storage::sqlite::SqliteStorage, Replica as TCReplica, ServerConfig};
use tokio::runtime::Runtime;

#[pyclass(unsendable)]
/// A replica represents an instance of a user's task data, providing an easy interface
/// for querying and modifying that data.
///
/// A replica can only be used in the thread in which it was created. Use from any other
/// thread will panic.
pub struct Replica {
    tc: ReplicaWrapper,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl Replica {
    #[staticmethod]
    /// Create a Replica with on-disk storage.
    ///
    /// This is equivalent to created a `StorgeConfig::OnDisk` with the given parameters and
    /// passing that to `Replica::new`.
    ///
    /// Raises `RuntimeError` if the database does not exist, and `create_if_missing` is false
    #[pyo3(signature=(path, create_if_missing, access_mode=AccessMode::ReadWrite))]
    pub fn new_on_disk(
        path: String,
        create_if_missing: bool,
        access_mode: AccessMode,
    ) -> PyResult<Replica> {
        let rt = Runtime::new()?;
        let tc = rt.block_on(async {
            let s = SqliteStorage::new(path, access_mode.into(), create_if_missing)
                .await
                .map_err(into_runtime_error)?;
            Ok::<_, PyErr>(ReplicaWrapper::Sqlite(TCReplica::new(s)))
        })?;
        Ok(Replica { tc, rt })
    }

    #[staticmethod]
    /// Create a Replica with in-memory storage.
    pub fn new_in_memory() -> PyResult<Self> {
        let rt = Runtime::new()?;
        let tc = rt.block_on(async {
            let s = InMemoryStorage::new();
            Ok::<_, PyErr>(ReplicaWrapper::InMemory(TCReplica::new(s)))
        })?;
        Ok(Replica { tc, rt })
    }

    pub fn create_task(&mut self, uuid: String, ops: &mut Operations) -> PyResult<Task> {
        self.rt.block_on(async {
            let task = self
                .tc
                .create_task(uuid2tc(uuid)?, ops.as_mut())
                .await
                .map_err(into_runtime_error)?
                .into();
            Ok(task)
        })
    }

    pub fn all_tasks(&mut self) -> PyResult<HashMap<String, Task>> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .all_tasks()
                .await
                .map_err(into_runtime_error)?
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.into()))
                .collect())
        })
    }

    pub fn all_task_data(&mut self) -> PyResult<HashMap<String, TaskData>> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .all_task_data()
                .await
                .map_err(into_runtime_error)?
                .into_iter()
                .map(|(key, value)| (key.to_string(), TaskData::from(value)))
                .collect())
        })
    }

    pub fn all_task_uuids(&mut self) -> PyResult<Vec<String>> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .all_task_uuids()
                .await
                .map_err(into_runtime_error)?
                .iter()
                .map(|item| item.to_string())
                .collect())
        })
    }

    pub fn working_set(&mut self) -> PyResult<WorkingSet> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .working_set()
                .await
                .map_err(into_runtime_error)?
                .into())
        })
    }

    pub fn dependency_map(&mut self, force: bool) -> PyResult<DependencyMap> {
        self.rt.block_on(async {
            let dm = self
                .tc
                .dependency_map(force)
                .await
                .map_err(into_runtime_error)?;
            Ok(dm.into())
        })
    }

    pub fn get_task(&mut self, uuid: String) -> PyResult<Option<Task>> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .get_task(uuid2tc(uuid)?)
                .await
                .map_err(into_runtime_error)?
                .map(|t| t.into()))
        })
    }

    pub fn get_task_data(&mut self, uuid: String) -> PyResult<Option<TaskData>> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .get_task_data(uuid2tc(uuid)?)
                .await
                .map_err(into_runtime_error)?
                .map(TaskData::from))
        })
    }

    pub fn commit_operations(&mut self, ops: Operations) -> PyResult<()> {
        self.rt.block_on(async {
            self.tc
                .commit_operations(ops.into())
                .await
                .map_err(into_runtime_error)
        })
    }

    /// Sync with a server crated from `ServerConfig::Local`.
    fn sync_to_local(&mut self, server_dir: String, avoid_snapshots: bool) -> PyResult<()> {
        self.rt.block_on(async {
            let mut server = ServerConfig::Local {
                server_dir: server_dir.into(),
            }
            .into_server()
            .await
            .map_err(into_runtime_error)?;
            self.tc
                .sync(&mut server, avoid_snapshots)
                .await
                .map_err(into_runtime_error)
        })
    }

    /// Sync with a server created from `ServerConfig::Remote`.
    #[cfg(feature = "server-sync")]
    fn sync_to_remote(
        &mut self,
        url: String,
        client_id: String,
        encryption_secret: String,
        avoid_snapshots: bool,
    ) -> PyResult<()> {
        self.rt.block_on(async {
            let mut server = ServerConfig::Remote {
                url,
                client_id: uuid2tc(client_id)?,
                encryption_secret: encryption_secret.into(),
            }
            .into_server()
            .await
            .map_err(into_runtime_error)?;
            self.tc
                .sync(&mut server, avoid_snapshots)
                .await
                .map_err(into_runtime_error)
        })
    }

    /// Sync with a server created from `ServerConfig::Gcp`.
    #[cfg(feature = "server-gcp")]
    #[pyo3(signature=(bucket, credential_path, encryption_secret, avoid_snapshots))]
    fn sync_to_gcp(
        &mut self,
        bucket: String,
        credential_path: Option<String>,
        encryption_secret: String,
        avoid_snapshots: bool,
    ) -> PyResult<()> {
        self.rt.block_on(async {
            let mut server = ServerConfig::Gcp {
                bucket,
                credential_path,
                encryption_secret: encryption_secret.into(),
            }
            .into_server()
            .await
            .map_err(into_runtime_error)?;
            self.tc
                .sync(&mut server, avoid_snapshots)
                .await
                .map_err(into_runtime_error)
        })
    }

    pub fn rebuild_working_set(&mut self, renumber: bool) -> PyResult<()> {
        self.rt.block_on(async {
            self.tc
                .rebuild_working_set(renumber)
                .await
                .map_err(into_runtime_error)
        })
    }

    pub fn num_local_operations(&mut self) -> PyResult<usize> {
        self.rt.block_on(async {
            self.tc
                .num_local_operations()
                .await
                .map_err(into_runtime_error)
        })
    }

    pub fn num_undo_points(&mut self) -> PyResult<usize> {
        self.rt
            .block_on(async { self.tc.num_undo_points().await.map_err(into_runtime_error) })
    }

    pub fn get_undo_operations(&mut self) -> PyResult<Operations> {
        self.rt.block_on(async {
            Ok(self
                .tc
                .get_undo_operations()
                .await
                .map_err(into_runtime_error)?
                .into())
        })
    }

    pub fn commit_reversed_operations(&mut self, operations: Operations) -> PyResult<bool> {
        self.rt.block_on(async {
            self.tc
                .commit_reversed_operations(operations.into())
                .await
                .map_err(into_runtime_error)
        })
    }

    pub fn expire_tasks(&mut self) -> PyResult<()> {
        self.rt
            .block_on(async { self.tc.expire_tasks().await.map_err(into_runtime_error) })
    }
}
