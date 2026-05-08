-- +goose Up
CREATE INDEX IF NOT EXISTS idx_resources_user ON resources(user_id);
CREATE INDEX IF NOT EXISTS idx_checks_due ON checks(enabled, paused_until, next_run_at) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_checks_user ON checks(user_id);
CREATE INDEX IF NOT EXISTS idx_check_executions_check_observed ON check_executions(check_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_check_opened ON incidents(check_id, opened_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_incidents_one_open_per_check ON incidents(check_id) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status, triggered_at);
CREATE INDEX IF NOT EXISTS idx_metrics_check_bucket ON metrics_aggregated(check_id, bucket_start DESC);

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_resources_updated_at ON resources;
CREATE TRIGGER trg_resources_updated_at BEFORE UPDATE ON resources FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_checks_updated_at ON checks;
CREATE TRIGGER trg_checks_updated_at BEFORE UPDATE ON checks FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_incidents_updated_at ON incidents;
CREATE TRIGGER trg_incidents_updated_at BEFORE UPDATE ON incidents FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_alerts_updated_at ON alerts;
CREATE TRIGGER trg_alerts_updated_at BEFORE UPDATE ON alerts FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- +goose Down
DROP TRIGGER IF EXISTS trg_alerts_updated_at ON alerts;
DROP TRIGGER IF EXISTS trg_incidents_updated_at ON incidents;
DROP TRIGGER IF EXISTS trg_checks_updated_at ON checks;
DROP TRIGGER IF EXISTS trg_resources_updated_at ON resources;
DROP TRIGGER IF EXISTS trg_users_updated_at ON users;

DROP INDEX IF EXISTS idx_metrics_check_bucket;
DROP INDEX IF EXISTS idx_alerts_status;
DROP INDEX IF EXISTS idx_incidents_one_open_per_check;
DROP INDEX IF EXISTS idx_incidents_check_opened;
DROP INDEX IF EXISTS idx_check_executions_check_observed;
DROP INDEX IF EXISTS idx_checks_user;
DROP INDEX IF EXISTS idx_checks_due;
DROP INDEX IF EXISTS idx_resources_user;
