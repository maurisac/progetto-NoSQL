CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE;
CREATE CONSTRAINT ON (b:Bank) ASSERT b.bank_id IS UNIQUE;
CREATE INDEX ON :Transaction(transaction_id);
