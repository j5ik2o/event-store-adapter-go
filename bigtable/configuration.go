package bigtable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	gbigtable "cloud.google.com/go/bigtable"
	eventstore "github.com/j5ik2o/event-store-adapter-go/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordedConfiguration is the JSON value of config:metadata in the configuration row (key 0x00) of both tables.
type recordedConfiguration struct {
	storeID       string
	layoutVersion int64
	keep          *uint64
	retentionMode string
	envelopeLimit uint64
}

var errInvalid = errors.New("the configuration row is invalid")

// parseConfiguration reads the configuration JSON. A duplicate key (even with the same value), a missing field, or a
// field of the wrong type or shape is invalid; key order and spaces do not matter.
func parseConfiguration(raw []byte) (recordedConfiguration, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if delim, ok := token.(json.Delim); err != nil || !ok || delim != '{' {
		return recordedConfiguration{}, errInvalid
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return recordedConfiguration{}, errInvalid
		}
		if _, duplicate := fields[key]; duplicate {
			return recordedConfiguration{}, errInvalid
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return recordedConfiguration{}, errInvalid
		}
		fields[key] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return recordedConfiguration{}, errInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return recordedConfiguration{}, errInvalid
	}
	var record recordedConfiguration
	if err := json.Unmarshal(fields["store_id"], &record.storeID); err != nil || fields["store_id"][0] != '"' {
		return recordedConfiguration{}, errInvalid
	}
	version, err := integer(fields["layout_version"])
	if err != nil {
		return recordedConfiguration{}, errInvalid
	}
	record.layoutVersion = int64(version)
	switch text := string(fields["keep_snapshot_count"]); {
	case text == "null":
	case strings.HasPrefix(text, `"`):
		var digits string
		if err := json.Unmarshal(fields["keep_snapshot_count"], &digits); err != nil {
			return recordedConfiguration{}, errInvalid
		}
		count, ok := parseKeepCount(digits)
		if !ok {
			return recordedConfiguration{}, errInvalid
		}
		record.keep = &count
	default:
		return recordedConfiguration{}, errInvalid
	}
	if err := json.Unmarshal(fields["retention_mode"], &record.retentionMode); err != nil || fields["retention_mode"][0] != '"' {
		return recordedConfiguration{}, errInvalid
	}
	limit, err := integer(fields["envelope_limit_bytes"])
	if err != nil || limit < 0 {
		return recordedConfiguration{}, errInvalid
	}
	record.envelopeLimit = uint64(limit)
	return record, nil
}

// integer reads a JSON number written as an integer (no fraction or exponent).
func integer(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, errInvalid
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

// parseKeepCount reads a positive decimal integer without a sign, exponent or leading zero.
func parseKeepCount(text string) (uint64, bool) {
	if text == "" || text[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, false
		}
	}
	count, err := strconv.ParseUint(text, 10, 64)
	return count, err == nil
}

func (r recordedConfiguration) json() []byte {
	keep := "null"
	if r.keep != nil {
		keep = strconv.Quote(strconv.FormatUint(*r.keep, 10))
	}
	return []byte(fmt.Sprintf(`{"store_id":%s,"layout_version":%d,"keep_snapshot_count":%s,"retention_mode":%s,"envelope_limit_bytes":%d}`,
		strconv.Quote(r.storeID), r.layoutVersion, keep, strconv.Quote(r.retentionMode), r.envelopeLimit))
}

func (r recordedConfiguration) equal(o recordedConfiguration) bool {
	sameKeep := (r.keep == nil) == (o.keep == nil) && (r.keep == nil || *r.keep == *o.keep)
	return r.storeID == o.storeID && r.layoutVersion == o.layoutVersion && sameKeep &&
		r.retentionMode == o.retentionMode && r.envelopeLimit == o.envelopeLimit
}

// verifyConfiguration compares the two rows with each other and with the client's settings.
func verifyConfiguration(journal, head []byte, s settings) error {
	first, err := parseConfiguration(journal)
	if err != nil {
		return configurationError(err)
	}
	second, err := parseConfiguration(head)
	if err != nil {
		return configurationError(err)
	}
	mismatch := func(what string) error {
		return configurationError(fmt.Errorf("the configuration rows do not match the client settings: %s", what))
	}
	switch {
	case !first.equal(second):
		return mismatch("the two tables differ")
	case first.layoutVersion != 1:
		return mismatch("unsupported layout_version")
	case first.retentionMode != "delete":
		return mismatch("retention_mode is not delete")
	case !isLowercaseUUID(first.storeID):
		return mismatch("store_id is not a lowercase UUID")
	case first.envelopeLimit != uint64(s.envelopeLimit):
		return mismatch("envelope_limit_bytes differs")
	}
	want := s.keepSnapshotCount()
	if (first.keep == nil) != (want == nil) || (want != nil && *first.keep != uint64(*want)) {
		return mismatch("keep_snapshot_count differs")
	}
	return nil
}

// verifyFamilies reads the table schema (the administrative API) and checks the column families. Data families must
// keep exactly one version and carry no age rule; the config family only has to exist.
func verifyFamilies(ctx context.Context, admin *gbigtable.AdminClient, tableID string, families []string) error {
	info, err := admin.TableInfo(ctx, tableID)
	if status.Code(err) == codes.NotFound {
		return configurationError(fmt.Errorf("table %q does not exist", tableID))
	}
	if err != nil {
		return storageError(err)
	}
	for _, family := range families {
		var found *gbigtable.FamilyInfo
		for i := range info.FamilyInfos {
			if info.FamilyInfos[i].Name == family {
				found = &info.FamilyInfos[i]
			}
		}
		if found == nil {
			return configurationError(fmt.Errorf("table %q lacks the column family %q", tableID, family))
		}
		if family == familyConfig {
			continue
		}
		if policy, ok := found.FullGCPolicy.(gbigtable.MaxVersionsGCPolicy); !ok || int(policy) != 1 {
			return configurationError(fmt.Errorf("column family %q of table %q must keep at most one version and have no age rule", family, tableID))
		}
	}
	return nil
}

// readConfiguration returns config:metadata of the configuration row, or nil when the row or the cell is absent.
func readConfiguration(ctx context.Context, data *gbigtable.Client, tableID string) ([]byte, error) {
	row, err := data.Open(tableID).ReadRow(ctx, configRowKey, gbigtable.RowFilter(latestCell(familyConfig, configMetadata)))
	if err != nil {
		return nil, storageError(err)
	}
	value, _ := cell(row, familyConfig, configMetadata)
	return value, nil
}

// observe verifies the destination: column families first, then both configuration rows. It writes nothing (BT-D1).
func observe(ctx context.Context, data *gbigtable.Client, admin *gbigtable.AdminClient, s settings) error {
	if err := verifyFamilies(ctx, admin, s.journalTable, journalFamilies); err != nil {
		return err
	}
	if err := verifyFamilies(ctx, admin, s.headTable, headFamilies); err != nil {
		return err
	}
	journal, err := readConfiguration(ctx, data, s.journalTable)
	if err != nil {
		return err
	}
	head, err := readConfiguration(ctx, data, s.headTable)
	if err != nil {
		return err
	}
	if journal == nil || head == nil {
		return configurationError(errors.New("a configuration row is missing: initialize the store or restore the row"))
	}
	return verifyConfiguration(journal, head, s)
}

// Initialize writes the configuration rows of two new tables once: first the journal table, then the head table,
// each as a conditional write that succeeds only when config:metadata does not exist. It never creates tables, column
// families or data rows. If either table already has a configuration row, nothing is written and the result is a
// configuration error. See the schema document for the recovery from an interrupted initialization.
func Initialize(ctx context.Context, data *gbigtable.Client, cfg Config, opts ...eventstore.Option) error {
	s, err := validate(data, cfg, opts...)
	if err != nil {
		return err
	}
	for _, table := range []string{s.journalTable, s.headTable} {
		existing, err := readConfiguration(ctx, data, table)
		if err != nil {
			return err
		}
		if existing != nil {
			return configurationError(fmt.Errorf("table %q already has a configuration row", table))
		}
	}
	storeID, err := newStoreID()
	if err != nil {
		return storageError(err)
	}
	record := recordedConfiguration{storeID: storeID, layoutVersion: 1, retentionMode: "delete", envelopeLimit: uint64(s.envelopeLimit)}
	if keep := s.keepSnapshotCount(); keep != nil {
		count := uint64(*keep)
		record.keep = &count
	}
	value := record.json()
	for _, table := range []string{s.journalTable, s.headTable} {
		create := gbigtable.NewMutation()
		setCell(create, familyConfig, configMetadata, value)
		var exists bool
		condition := gbigtable.NewCondMutation(gbigtable.ChainFilters(columnFilter(familyConfig, configMetadata), gbigtable.LatestNFilter(1)), nil, create)
		if err := data.Open(table).Apply(ctx, configRowKey, condition, gbigtable.GetCondMutationResult(&exists)); err != nil {
			return storageError(err)
		}
		if exists {
			return configurationError(fmt.Errorf("table %q already has a configuration row", table))
		}
	}
	return nil
}

// newStoreID returns a random version 4 UUID in lowercase text form.
func newStoreID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// isLowercaseUUID accepts only the 36-character form with lowercase hexadecimal digits and hyphens.
func isLowercaseUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return false
			}
		default:
			c := s[i]
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}
