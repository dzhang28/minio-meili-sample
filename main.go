package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/meilisearch/meilisearch-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type FileObj struct {
	minio.ObjectInfo
	Id int64 `json:"id"`
}

func main() {
	var idCnt int64

	ctx := context.Background()

	if len(os.Args) > 1 {
		idCnt, _ = strconv.ParseInt(os.Args[1], 10, 64)
	} else {
		idCnt = 1
	}

	// Create a MinIO client
	minioClient, err := minio.New(
		"localhost:33200", &minio.Options{
			Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
			Secure: false,
		})
	if err != nil {
		log.Fatalln(err)
	}

	// Create a meilisearch client
	mlClient := meilisearch.NewClient(meilisearch.ClientConfig{
		Host:   "http://127.0.0.1:33270",
		APIKey: "minioapikey",
	})

	mlClient.DeleteIndex("minio")

	// An index is where the documents are stored.
	index := mlClient.Index("minio")

	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, bucket := range buckets {
		fmt.Println(bucket)
		files := make([]FileObj, 0)

		objectCh := minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{Prefix: "", Recursive: true})
		for object := range objectCh {
			var f FileObj

			f.ObjectInfo = object

			objInfo, err := minioClient.StatObject(ctx, bucket.Name, object.Key, minio.StatObjectOptions{})
			if err != nil {
				fmt.Println(err)
				return
			}

			f.ContentType = objInfo.ContentType
			f.Metadata = objInfo.Metadata
			f.UserTagCount = objInfo.UserTagCount
			f.UserMetadata = objInfo.UserMetadata

			if f.UserTagCount > 0 {
				tags, err := minioClient.GetObjectTagging(ctx, bucket.Name, object.Key, minio.GetObjectTaggingOptions{})
				if err != nil {
					fmt.Println(err)
					return
				}
				f.UserTags = tags.ToMap()
			}

			f.Id = idCnt
			idCnt++
			files = append(files, f)
		}

		// Add documents to the index
		task, err := index.AddDocuments(files, "id")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(task.TaskUID)
	}
}
