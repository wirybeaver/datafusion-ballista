// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Iceberg SDK implementation of the provider build-gate interface.

use std::sync::Arc;

use ballista_core::iceberg::IcebergTableProviderCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference, exec_err, not_impl_err};
use datafusion::execution::TaskContext;
use iceberg_datafusion::metadata::IcebergMetadataTableProvider;
use iceberg_datafusion::{IcebergCatalogTableProvider, IcebergStaticTableProvider};
use serde::{Deserialize, Serialize};

use crate::catalog::PinnedTable;
use crate::wire::{self, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ProviderWire {
    table: TableWire,
    snapshot_id: Option<i64>,
}

/// Iceberg SDK provider adapter; framing and delegation live in ballista-core.
#[derive(Debug, Default)]
pub struct IcebergProviderAdapter;

/// Gate specialized with this demo's Iceberg SDK provider implementation.
pub type IcebergLogicalCodec =
    ballista_core::iceberg::IcebergLogicalCodec<IcebergProviderAdapter>;

impl IcebergTableProviderCodec for IcebergProviderAdapter {
    fn decode(
        &self,
        payload: &[u8],
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let ProviderWire { table, snapshot_id } = wire::decode(payload)?;
        Ok(Arc::new(PinnedTable {
            table: table.load()?,
            snapshot_id,
            schema,
        }))
    }

    fn try_encode(
        &self,
        table_ref: &TableReference,
        node: &dyn TableProvider,
    ) -> Result<Option<Vec<u8>>> {
        let payload = if let Some(provider) =
            node.downcast_ref::<IcebergStaticTableProvider>()
        {
            ProviderWire {
                table: TableWire::with_metadata(provider.table())?,
                snapshot_id: provider.snapshot_id(),
            }
        } else if let Some(pinned) = node.downcast_ref::<PinnedTable>() {
            ProviderWire {
                table: TableWire::with_metadata(&pinned.table)?,
                snapshot_id: pinned.snapshot_id,
            }
        } else if node.is::<IcebergCatalogTableProvider>() {
            return exec_err!(
                "Iceberg table {table_ref} is catalog-backed but wasn't pinned to a \
                 version: build the client's session with ballista_iceberg::register_iceberg"
            );
        } else if node.is::<IcebergMetadataTableProvider>() {
            return not_impl_err!(
                "Iceberg metadata table {table_ref} isn't supported on Ballista yet"
            );
        } else {
            return Ok(None);
        };
        wire::encode(&payload).map(Some)
    }
}
