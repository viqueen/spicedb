package migrations

import (
	"context"
	"github.com/jackc/pgx/v5"
)

const addTenantIDColumnToRelationTupleTable = `ALTER TABLE relation_tuple ADD COLUMN IF NOT EXISTS tenant_id VARCHAR NOT NULL DEFAULT '';`

const createIndexForRelationTupleTenantID = `CREATE INDEX IF NOT EXISTS idx_relation_tuple_tenant_id ON relation_tuple (tenant_id);`

const dropReverseQueryIndex = `DROP INDEX IF EXISTS ix_relation_tuple_by_subject`
const reCreateReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (tenant_id, userset_object_id, userset_namespace, userset_relation, namespace, relation)`

const dropReverseCheckIndex = `DROP INDEX IF EXISTS ix_relation_tuple_by_subject_relation`
const reCreateReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (tenant_id, userset_namespace, userset_relation, namespace, relation)`

const dropDeletedTransactionIndex = `DROP INDEX IF EXISTS ix_relation_tuple_by_deleted_transaction`
const reCreateDeletedTransactionIndex = `CREATE INDEX ix_relation_tuple_by_deleted_transaction ON relation_tuple (tenant_id, deleted_transaction)`

func init() {
	err := DatabaseMigrations.Register(
		"add-tenant-id-column-to-relation-tuple-table",
		"add-index-for-transaction-gc",
		noNonatomicMigration,
		func(ctx context.Context, trx pgx.Tx) error {
			if _, err := trx.Exec(ctx, addTenantIDColumnToRelationTupleTable); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, createIndexForRelationTupleTenantID); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, dropReverseQueryIndex); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, reCreateReverseQueryIndex); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, dropReverseCheckIndex); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, reCreateReverseCheckIndex); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, dropDeletedTransactionIndex); err != nil {
				return err
			}
			if _, err := trx.Exec(ctx, reCreateDeletedTransactionIndex); err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
