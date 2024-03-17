package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BigtableRepository struct {
	client       *bigtable.Client
	readTimeout  time.Duration
	writeTimeout time.Duration
	tableName    string
}

func NewBigtableRepository() BigtableRepository {
	connectionPoolOption := option.WithGRPCConnectionPool(10)
	// WithGRPCConn
	// WithGRPCDialOption
	// WithGRPCConnectionPool

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	clientConfig := bigtable.ClientConfig{AppProfile: "default"}
	client, err := bigtable.NewClientWithConfig(
		ctx,
		"local",
		"local-instance",
		clientConfig,
		connectionPoolOption)

	if err != nil {
		logger.Warn("error when create a new client", err)
	}

	return BigtableRepository{
		client:       client,
		readTimeout:  time.Duration(1 * time.Second),
		writeTimeout: time.Duration(1 * time.Second),
		tableName:    "media_progress",
	}
}

func (r BigtableRepository) insert(ctx context.Context, mediaProgress MediaProgress) error {
	// Row Key: user_id#data_type#title_id#media_id
	rowKey := fmt.Sprintf("%s#%s#%s#%s",
		mediaProgress.UserId,
		mediaProgress.DataType,
		mediaProgress.TitleId,
		mediaProgress.MediaId,
	)

	var version bigtable.Timestamp = 0
	mutation := bigtable.NewMutation()
	mutation.Set("data", "milliseconds", version, convertInt64ToByteArray(mediaProgress.Milliseconds))
	mutation.Set("data", "event_at", version, convertInt64ToByteArray(mediaProgress.EventAt))

	ctxTimeout, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	table := r.client.Open(r.tableName)
	if err := table.Apply(ctxTimeout, rowKey, mutation); err != nil {
		logger.Error(fmt.Sprintf("error when insert on %s", r.tableName), err)
		return err
	}

	logger.Info(fmt.Sprintf("successfully wrote row %s on %s", rowKey, r.tableName))
	return nil
}

func (r BigtableRepository) insertConditional(ctx context.Context, mediaProgress MediaProgress) error {
	// Row Key: user_id#data_type#title_id#media_id
	rowKey := fmt.Sprintf("%s#%s#%s#%s",
		mediaProgress.UserId,
		mediaProgress.DataType,
		mediaProgress.TitleId,
		mediaProgress.MediaId,
	)

	var version bigtable.Timestamp = 0
	mutation := bigtable.NewMutation()
	mutation.Set("data", "milliseconds", version, convertInt64ToByteArray(mediaProgress.Milliseconds))
	mutation.Set("data", "event_at", version, convertInt64ToByteArray(mediaProgress.EventAt))

	ctxTimeout, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	start := convertInt64ToByteArray((time.Now().Unix()))
	end := convertInt64ToByteArray(99999999999999999)

	filter := bigtable.ChainFilters(
		bigtable.FamilyFilter("data"),
		bigtable.ColumnFilter("event_at"),
		bigtable.ValueRangeFilter(start, end))

	conditionalMutation := bigtable.NewCondMutation(filter, nil, mutation)

	var wasConditionMatched bool
	applyOptions := []bigtable.ApplyOption{bigtable.GetCondMutationResult(&wasConditionMatched)}

	table := r.client.Open(r.tableName)
	if err := table.Apply(ctxTimeout, rowKey, conditionalMutation, applyOptions...); err != nil {
		logger.Error(fmt.Sprintf("error when insert on %s", r.tableName), err)
		return err
	}

	if wasConditionMatched {
		logger.Info(fmt.Sprintf("Mutation not applied row %s on %s", rowKey, r.tableName))
	} else {
		logger.Info(fmt.Sprintf("Mutation applied row %s on %s", rowKey, r.tableName))
	}

	return nil
}

func (r BigtableRepository) insertBatch(ctx context.Context, mediaProgressList []MediaProgress) error {
	rowKeys := []string{}
	mutations := []*bigtable.Mutation{}

	for _, mediaProgress := range mediaProgressList {
		// Row Key: user_id#data_type#title_id#media_id
		rowKey := fmt.Sprintf("%s#%s#%s#%s",
			mediaProgress.UserId,
			mediaProgress.DataType,
			mediaProgress.TitleId,
			mediaProgress.MediaId,
		)

		var version bigtable.Timestamp = 0
		mutation := bigtable.NewMutation()
		mutation.Set("data", "milliseconds", version, convertInt64ToByteArray(mediaProgress.Milliseconds))
		mutation.Set("data", "event_at", version, convertInt64ToByteArray(mediaProgress.EventAt))

		rowKeys = append(rowKeys, rowKey)
		mutations = append(mutations, mutation)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	table := r.client.Open(r.tableName)
	if _, err := table.ApplyBulk(ctxTimeout, rowKeys, mutations); err != nil {
		logger.Error(fmt.Sprintf("error when insert on %s", r.tableName), err)
		return err
	}

	logger.Info(fmt.Sprintf("successfully wrote row keys %s on %s", rowKeys, r.tableName))
	return nil
}

func (r BigtableRepository) delete(ctx context.Context, userID, dataType, titleID, mediaID string) error {
	// Row Key: user_id#data_type#title_id#media_id
	rowKey := fmt.Sprintf("%s#%s#%s#%s",
		userID,
		dataType,
		titleID,
		mediaID,
	)

	mutation := bigtable.NewMutation()
	mutation.DeleteRow()

	ctxTimeout, cancel := context.WithTimeout(ctx, r.writeTimeout)
	defer cancel()

	table := r.client.Open(r.tableName)
	if err := table.Apply(ctxTimeout, rowKey, mutation); err != nil {
		logger.Error(fmt.Sprintf("error when delete on %s", r.tableName), err)
		return err
	}

	logger.Info(fmt.Sprintf("successfully deleted row %s on %s", rowKey, r.tableName))
	return nil
}

func (r BigtableRepository) readOne(ctx context.Context, userID, dataType, titleID, mediaID string) (MediaProgress, error) {
	// Row Key: user_id#data_type#title_id#media_id
	rowKey := fmt.Sprintf("%s#%s#%s#%s",
		userID,
		dataType,
		titleID,
		mediaID,
	)

	ctxTimeout, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()

	table := r.client.Open(r.tableName)
	row, err := table.ReadRow(ctxTimeout, rowKey)

	databaseError, _ := status.FromError(err)
	if err != nil && databaseError.Code() != codes.NotFound {
		return MediaProgress{}, err
	}

	if len(row) == 0 || databaseError.Code() == codes.NotFound {
		return MediaProgress{}, errors.New("not found")
	}

	mediaProgress, err := parseRow(row["data"])
	if err != nil {
		return MediaProgress{}, err
	}

	return mediaProgress, nil
}

func (r BigtableRepository) readMultiple(ctx context.Context, userID, dataType, titleID string, mediaIDs []string) ([]MediaProgress, error) {
	rowKeys := bigtable.RowList{}
	for _, mediaID := range mediaIDs {
		// Row Key: user_id#data_type#title_id#media_id
		rowKey := fmt.Sprintf("%s#%s#%s#%s",
			userID,
			dataType,
			titleID,
			mediaID,
		)

		rowKeys = append(rowKeys, rowKey)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()

	mediaProgressList := []MediaProgress{}
	table := r.client.Open(r.tableName)
	err := table.ReadRows(ctxTimeout, rowKeys, func(row bigtable.Row) bool {
		readItems := row["data"]
		mediaProgress, err := parseRow(readItems)

		if err != nil {
			return false
		}

		mediaProgressList = append(mediaProgressList, mediaProgress)
		return true
	})

	if err != nil {
		return []MediaProgress{}, err
	}

	return mediaProgressList, nil
}

func (r BigtableRepository) readByPartialRowKey(ctx context.Context, userID, dataType, titleID string) ([]MediaProgress, error) {
	// Row Key: user_id#data_type#title_id#media_id
	rowKey := fmt.Sprintf("%s#%s#%s",
		userID,
		dataType,
		titleID,
	)

	ctxTimeout, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()

	mediaProgressList := []MediaProgress{}
	table := r.client.Open(r.tableName)
	err := table.ReadRows(ctxTimeout, bigtable.PrefixRange(rowKey), func(row bigtable.Row) bool {
		readItems := row["data"]
		mediaProgress, err := parseRow(readItems)

		if err != nil {
			return false
		}

		mediaProgressList = append(mediaProgressList, mediaProgress)
		return true
	})

	if err != nil {
		return []MediaProgress{}, err
	}

	return mediaProgressList, nil
}

func parseRow(itemRows []bigtable.ReadItem) (MediaProgress, error) {
	mediaProgress := MediaProgress{}

	// rowKey = user_id#data_type#title_id#media_id
	for _, itemRow := range itemRows {
		keyParts := strings.Split(itemRow.Row, "#")

		mediaProgress.UserId = keyParts[0]
		mediaProgress.DataType = keyParts[1]
		mediaProgress.TitleId = keyParts[2]
		mediaProgress.MediaId = keyParts[3]

		if itemRow.Column == "data:milliseconds" {
			milliseconds, err := convertByteArrayToInt64(itemRow.Value)
			if err != nil {
				return MediaProgress{}, err
			}

			mediaProgress.Milliseconds = milliseconds
		}

		if itemRow.Column == "data:event_at" {
			eventAt, err := convertByteArrayToInt64(itemRow.Value)
			if err != nil {
				return MediaProgress{}, err
			}

			mediaProgress.EventAt = eventAt
		}
	}

	return mediaProgress, nil
}

func convertInt64ToByteArray(value int64) []byte {
	return []byte(strconv.FormatInt(value, 10))
}

func convertByteArrayToInt64(value []byte) (int64, error) {
	return strconv.ParseInt(string(value), 10, 64)
}
