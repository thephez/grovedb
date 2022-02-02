use neon::prelude::*;
use node_grove_common::GroveDbWrapper;

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("groveDbOpen", GroveDbWrapper::js_open)?;
    cx.export_function("groveDbInsert", GroveDbWrapper::js_insert)?;
    cx.export_function(
        "groveDbInsertIfNotExists",
        GroveDbWrapper::js_insert_if_not_exists,
    )?;
    cx.export_function("groveDbGet", GroveDbWrapper::js_get)?;
    cx.export_function("groveDbDelete", GroveDbWrapper::js_delete)?;
    cx.export_function("groveDbProof", GroveDbWrapper::js_proof)?;
    cx.export_function("groveDbClose", GroveDbWrapper::js_close)?;
    cx.export_function("groveDbFlush", GroveDbWrapper::js_flush)?;
    cx.export_function(
        "groveDbStartTransaction",
        GroveDbWrapper::js_start_transaction,
    )?;
    cx.export_function(
        "groveDbCommitTransaction",
        GroveDbWrapper::js_commit_transaction,
    )?;
    cx.export_function(
        "groveDbRollbackTransaction",
        GroveDbWrapper::js_rollback_transaction,
    )?;
    cx.export_function(
        "groveDbIsTransactionStarted",
        GroveDbWrapper::js_is_transaction_started,
    )?;
    cx.export_function(
        "groveDbAbortTransaction",
        GroveDbWrapper::js_abort_transaction,
    )?;
    cx.export_function("groveDbPutAux", GroveDbWrapper::js_put_aux)?;
    cx.export_function("groveDbDeleteAux", GroveDbWrapper::js_delete_aux)?;
    cx.export_function("groveDbGetAux", GroveDbWrapper::js_get_aux)?;
    cx.export_function("groveDbGetPathQuery", GroveDbWrapper::js_get_path_query)?;
    cx.export_function("groveDbRootHash", GroveDbWrapper::js_root_hash)?;

    Ok(())
}
