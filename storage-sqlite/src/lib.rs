// Copyright 2025 The Tari Project
// SPDX-License-Identifier: BSD-3-Clause

mod error;
mod reader;
mod store;
mod writer;

pub use error::*;
pub use reader::*;
pub use store::*;

#[cfg(test)]
mod tests {
    use super::*;
    use ootle_payment_processor_storage::models::{JobStatus, JobType};
    use ootle_payment_processor_storage::{
        ReadableStore, StoreReadTransaction, StoreWriteTransaction, WriteableStore,
    };
    use tari_template_lib::constants::TARI_TOKEN;

    #[tokio::test]
    async fn sqlite_requeue_for_balance_and_delete_job_match_by_uuid() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        store.migrate().await.unwrap();

        let job_id = {
            let mut tx = store.create_write_tx().await.unwrap();
            let id = tx
                .enqueue_work(JobType::ProcessPayment, 5, Some(serde_json::json!({"test": true})))
                .await
                .unwrap();
            tx.commit().await.unwrap();
            id
        };

        // Requeue the job waiting for balance on TARI_TOKEN
        {
            let mut tx = store.create_write_tx().await.unwrap();
            tx.job_requeue_for_balance(&job_id, TARI_TOKEN, 1_000)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        // Verify the job transitioned to WaitingForBalance and can be queried by balance threshold
        {
            let mut tx = store.create_read_tx().await.unwrap();
            let job = tx.get_job_by_id(job_id).await.unwrap().unwrap();
            assert!(matches!(job.status, JobStatus::WaitingForBalance));
            assert_eq!(job.attempts, 1);

            let none_waiting = tx
                .get_next_job_waiting_for_balance(TARI_TOKEN, 999)
                .await
                .unwrap();
            assert_eq!(none_waiting, None);

            let ready = tx
                .get_next_job_waiting_for_balance(TARI_TOKEN, 1_000)
                .await
                .unwrap();
            assert_eq!(ready, Some(job_id));
        }

        // Delete the job by UUID and verify it is removed
        {
            let mut tx = store.create_write_tx().await.unwrap();
            tx.delete_job(&job_id).await.unwrap();
            tx.commit().await.unwrap();
        }

        {
            let mut tx = store.create_read_tx().await.unwrap();
            assert!(tx.get_job_by_id(job_id).await.unwrap().is_none());
        }
    }
}

