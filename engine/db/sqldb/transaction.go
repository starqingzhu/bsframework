package sqldb

import "github.com/jmoiron/sqlx"

type Transaction struct {
	*sqlx.Tx
}

func (tx *Transaction) Rollback() error {
	return tx.Tx.Rollback()
}

func (tx *Transaction) Commit() error {
	return tx.Tx.Commit()
}
