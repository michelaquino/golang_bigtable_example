package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(insertCmd)
	rootCmd.AddCommand(readCmd)
	rootCmd.AddCommand(deleteCmd)
}

var rootCmd = &cobra.Command{
	Use:   "examples",
	Short: "Bigtable examples",
}

func Execute() error {
	return rootCmd.Execute()
}

var insertCmd = &cobra.Command{
	Use:   "insert",
	Short: "Insert examples",
	Long:  "Example to insert items in Bigtable",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		repo := NewBigtableRepository()

		switch args[0] {
		case "one":
			insertOneExample(ctx, repo)
		case "conditional":
			insertConditionalExample(ctx, repo)
		case "batch":
			insertBatchExample(ctx, repo)
		default:
			panic("invalid option")
		}
	},
}

var readCmd = &cobra.Command{
	Use:   "read",
	Short: "Read examples",
	Long:  "Example to read items from Bigtable",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		repo := NewBigtableRepository()

		switch args[0] {
		case "one":
			readOneExample(ctx, repo)
		case "multiple":
			readMultipleExample(ctx, repo)
		case "partialKey":
			readPartialKeyExample(ctx, repo)
		default:
			panic("invalid option")
		}
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete examples",
	Long:  "Example to delete items from Bigtable",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		repo := NewBigtableRepository()
		deleteExample(ctx, repo)
	},
}

func insertOneExample(ctx context.Context, repo BigtableRepository) {
	mediaProgress := MediaProgress{
		UserId:       "user_1",
		DataType:     "AUDIO",
		TitleId:      "pod_1",
		MediaId:      "media_4",
		Milliseconds: 1111111111,
		EventAt:      time.Now().Unix(),
	}

	repo.insert(ctx, mediaProgress)
}

func insertConditionalExample(ctx context.Context, repo BigtableRepository) {
	mediaProgress := MediaProgress{
		UserId:       "user_3",
		DataType:     "VIDEO",
		TitleId:      "title_1",
		MediaId:      "media_1",
		Milliseconds: 2222222222,
		EventAt:      time.Now().Add(1 * time.Minute).Unix(),
	}

	repo.insertConditional(ctx, mediaProgress)

	oldMediaProgress := MediaProgress{
		UserId:       "user_3",
		DataType:     "VIDEO",
		TitleId:      "title_1",
		MediaId:      "media_1",
		Milliseconds: 3333333333,
		EventAt:      time.Now().Unix(),
	}

	repo.insertConditional(ctx, oldMediaProgress)
}

func insertBatchExample(ctx context.Context, repo BigtableRepository) {
	mediaProgressToInsert := []MediaProgress{
		{
			UserId:       "user_1",
			DataType:     "AUDIO",
			TitleId:      "pod_1",
			MediaId:      "media_4",
			Milliseconds: 4444444444,
			EventAt:      time.Now().Unix(),
		},
		{
			UserId:       "user_1",
			DataType:     "VIDEO",
			TitleId:      "title_1",
			MediaId:      "media_1",
			Milliseconds: 5555555555,
			EventAt:      time.Now().Add(-1 * time.Hour).Unix(),
		},
		{
			UserId:       "user_1",
			DataType:     "VIDEO",
			TitleId:      "title_1",
			MediaId:      "media_2",
			Milliseconds: 6666666666,
			EventAt:      time.Now().Add(-2 * time.Hour).Unix(),
		},
		{
			UserId:       "user_1",
			DataType:     "VIDEO",
			TitleId:      "title_2",
			MediaId:      "media_3",
			Milliseconds: 7777777777,
			EventAt:      time.Now().Add(-3 * time.Hour).Unix(),
		},
		{
			UserId:       "user_2",
			DataType:     "VIDEO",
			TitleId:      "title_4",
			MediaId:      "media_1",
			Milliseconds: 8888888888,
			EventAt:      time.Now().Add(-4 * time.Hour).Unix(),
		},
	}

	repo.insertBatch(ctx, mediaProgressToInsert)
}

func readOneExample(ctx context.Context, repo BigtableRepository) {
	mediaProgress, err := repo.readOne(ctx, "user_1", "VIDEO", "title_1", "media_1")
	if err != nil {
		logger.Error(err.Error())
		return
	}

	mediaProgressJSON, _ := json.Marshal(mediaProgress)
	fmt.Printf("%s\n", string(mediaProgressJSON))
}

func readMultipleExample(ctx context.Context, repo BigtableRepository) {
	mediaProgressList, err := repo.readMultiple(ctx, "user_1", "VIDEO", "title_1", []string{"media_1", "media_2"})
	if err != nil {
		logger.Error(err.Error())
		return
	}

	mediaProgressListJSON, _ := json.Marshal(mediaProgressList)
	fmt.Printf("%s\n", string(mediaProgressListJSON))
}

func readPartialKeyExample(ctx context.Context, repo BigtableRepository) {
	mediaProgressList, err := repo.readByPartialRowKey(ctx, "user_1", "VIDEO", "title_1")
	if err != nil {
		logger.Error(err.Error())
		return
	}

	mediaProgressListJSON, _ := json.Marshal(mediaProgressList)
	fmt.Printf("%s\n", string(mediaProgressListJSON))
}

func deleteExample(ctx context.Context, repo BigtableRepository) {
	err := repo.delete(ctx, "user_1", "VIDEO", "title_1", "media_1")
	if err != nil {
		logger.Error(err.Error())
		return
	}
}
