package bigtabletest

import (
	"context"
	"fmt"

	gbigtable "cloud.google.com/go/bigtable"
)

var (
	journalFamilies = []string{"event", "history"}
	headFamilies    = []string{"head", "snapshot"}
)

// CreateTables provisions the journal and head tables the way an operator does: every data family keeps at most one
// version and has no age rule; the config family has no rule.
func CreateTables(ctx context.Context, admin *gbigtable.AdminClient, journalID, headID string) error {
	for table, families := range map[string][]string{journalID: journalFamilies, headID: headFamilies} {
		if err := admin.CreateTable(ctx, table); err != nil {
			return err
		}
		for _, family := range families {
			if err := admin.CreateColumnFamily(ctx, table, family); err != nil {
				return err
			}
			if err := admin.SetGCPolicy(ctx, table, family, gbigtable.MaxVersionsPolicy(1)); err != nil {
				return err
			}
		}
		if err := admin.CreateColumnFamily(ctx, table, "config"); err != nil {
			return err
		}
	}
	return nil
}

// ConfigJSON is the configuration row value. keep is the history count or nil for null; the count is a decimal string.
func ConfigJSON(storeID string, keep *int64, envelopeLimitBytes int) string {
	count := "null"
	if keep != nil {
		count = fmt.Sprintf("%q", fmt.Sprint(*keep))
	}
	return fmt.Sprintf(`{"store_id":%q,"layout_version":1,"keep_snapshot_count":%s,"retention_mode":"delete","envelope_limit_bytes":%d}`,
		storeID, count, envelopeLimitBytes)
}

// WriteConfig replaces the configuration row (row key 0x00, config:metadata) of one table, with cell timestamp 0.
func WriteConfig(ctx context.Context, data *gbigtable.Client, tableID, value string) error {
	m := gbigtable.NewMutation()
	m.DeleteCellsInColumn("config", "metadata")
	m.Set("config", "metadata", 0, []byte(value))
	return data.Open(tableID).Apply(ctx, "\x00", m)
}
