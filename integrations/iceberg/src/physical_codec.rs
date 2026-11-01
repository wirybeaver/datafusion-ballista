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

//! Iceberg SDK implementation of the scan build-gate interface.

use std::sync::Arc;

use ballista_core::iceberg::IcebergScanCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::expr::Predicate;
use iceberg_datafusion::read::IcebergDataSource;
use serde::{Deserialize, Serialize};

use crate::wire::{self, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ScanWire {
    table: TableWire,
    snapshot_id: Option<i64>,
    /// The scan's output schema, already projected.
    schema: SchemaRef,
    predicate: Option<Predicate>,
    fetch: Option<usize>,
}

/// Iceberg SDK scan adapter; framing and delegation live in ballista-core.
#[derive(Debug, Default)]
pub struct IcebergScanAdapter;

/// Gate specialized with this demo's Iceberg SDK scan implementation.
pub type IcebergPhysicalCodec =
    ballista_core::iceberg::IcebergPhysicalCodec<IcebergScanAdapter>;

impl IcebergScanCodec for IcebergScanAdapter {
    fn decode(
        &self,
        payload: &[u8],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan: ScanWire = wire::decode(payload)?;
        let source = IcebergDataSource::builder(scan.table.load()?, scan.schema)
            .with_snapshot_id(scan.snapshot_id)
            .with_predicate(scan.predicate)
            .with_fetch(scan.fetch)
            .build()?;
        Ok(DataSourceExec::from_data_source(source))
    }

    fn try_encode(&self, node: &dyn ExecutionPlan) -> Result<Option<Vec<u8>>> {
        let source = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<IcebergDataSource>());
        let Some(source) = source else {
            return Ok(None);
        };
        wire::encode(&ScanWire {
            table: TableWire::new(source.table())?,
            snapshot_id: source.snapshot_id(),
            schema: source.schema(),
            predicate: source.predicate().cloned(),
            fetch: source.fetch(),
        })
        .map(Some)
    }
}
