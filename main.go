package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"image/jpeg"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/nfnt/resize"
)

type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	MaxRetry        int
	Sizes           []image.Point
}

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	wg := new(sync.WaitGroup)
	wg.Add(len(s3Event.Records))

	for _, record := range s3Event.Records {
		fmt.Printf("%s Object: %s\n", record.EventName, record.S3.Object.Key)
		if strings.HasSuffix(record.S3.Object.Key, "/") {
			// 创建了目录
			fmt.Printf("Create a dir %s", record.S3.Object.Key)
			wg.Done()
			continue
		}

		go onFileCreated(ctx, record, wg)
	}
	wg.Wait()
}

func onFileCreated(ctx context.Context, record events.S3EventRecord, wg *sync.WaitGroup) {

	conf, err := readConfig()
	if err != nil {
		fmt.Printf("Read config failed due to %v", err)
		return
	}

	// 初始化s3 session
	creds := credentials.NewStaticCredentialsFromCreds(credentials.Value{AccessKeyID: conf.AccessKeyID, SecretAccessKey: conf.SecretAccessKey})
	config := aws.NewConfig().WithCredentials(creds).WithRegion(conf.Region).WithMaxRetries(conf.MaxRetry)

	fmt.Printf("config region [%s](%v)\n", aws.StringValue(config.Region), config.Region)

	_session, err := session.NewSession(config)
	if err != nil {
		fmt.Printf("New session failed due to %v", err)
		return
	}

	fmt.Printf("_session config region [%s]\n", aws.StringValue(_session.Config.Region))
	// 为了包含region设置不许要new一个config
	client := s3.New(_session, aws.NewConfig().WithRegion(conf.Region))
	fmt.Printf("client config region [%s](%v) and SigningRegion [%s]\n", aws.StringValue(client.Config.Region), client.Config.Region, client.SigningRegion)

	// 尝试从S3读取图像
	src, err := readImage(ctx, client, record)
	if err != nil {
		fmt.Printf("Read image from object %s from bucket %s failed due to %v", record.S3.Object.Key, record.S3.Bucket.Name, err)
		return
	}

	for _, size := range conf.Sizes {

		// 生成缩略图
		dest := resize.Thumbnail(uint(size.X), uint(size.Y), src, resize.Lanczos3)

		// 尝试保存到S3
		key := objectKeyWithSize(record.S3.Object.Key, size)
		err = saveImage(ctx, client, dest, record.S3.Bucket.Name, key)
		if err != nil {
			fmt.Printf("Save image to bucket %s object %s failed due to %v", record.S3.Bucket.Name, key, err)
			return
		}

		// 发送完成通知
	}

	wg.Done()
}

func readConfig() (*Config, error) {
	accessKeyID := os.Getenv("AccessKeyID")
	secretAccessKey := os.Getenv("SecretAccessKey")
	region := os.Getenv("Region")
	sizeString := os.Getenv("Sizes")
	if accessKeyID == "" || secretAccessKey == "" || region == "" || sizeString == "" {
		return nil, fmt.Errorf("Environment viriables is invalid")
	}

	var sizes []image.Point
	pattern := regexp.MustCompile("(\\d+)x(\\d+)")
	for _, group := range pattern.FindAllStringSubmatch(sizeString, -1) {
		if len(group) != 3 {
			return nil, fmt.Errorf("Environment viriables Sizes %v is invalid", group)
		}

		width, err := strconv.Atoi(group[1])
		if err != nil {
			return nil, fmt.Errorf("Environment viriables Sizes %v is invalid: %v", group, err)
		}

		height, err := strconv.Atoi(group[2])
		if err != nil {
			return nil, fmt.Errorf("Environment viriables Sizes %v is invalid: %v", group, err)
		}

		sizes = append(sizes, image.Pt(width, height))
	}

	var err error
	maxRetry, err := strconv.Atoi(os.Getenv("MaxRetries"))
	if err != nil {
		maxRetry = 3
	}

	fmt.Printf("AccessKeyID: %s\n", accessKeyID)
	fmt.Printf("SecretAccessKey: %s\n", secretAccessKey)
	fmt.Printf("Region: %s\n", region)
	fmt.Printf("Sizes: %v\n", sizes)
	fmt.Printf("MaxRetries: %d\n", maxRetry)

	return &Config{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Region:          region,
		Sizes:           sizes,
		MaxRetry:        maxRetry,
	}, nil
}

func readImage(ctx context.Context, client *s3.S3, record events.S3EventRecord) (image.Image, error) {

	// 获取文件
	output, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(record.S3.Object.Key),
	})
	if err != nil {
		fmt.Printf("Get object %s from bucket %s failed due to %v", record.S3.Object.Key, record.S3.Bucket.Name, err)
		return nil, err
	}
	defer output.Body.Close()

	// 读取图像
	return jpeg.Decode(output.Body)
}

func saveImage(ctx context.Context, client *s3.S3, dest image.Image, bucket, key string) error {

	buffer := new(bytes.Buffer)
	err := jpeg.Encode(buffer, dest, nil)
	if err != nil {
		fmt.Printf("Encode jpeg failed due to %v", err)
		return err
	}

	_, err = client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(buffer.Bytes()),
		StorageClass: aws.String(s3.ObjectStorageClassStandard),
	})
	if err != nil {
		fmt.Printf("Put bucket %s object %s failed due to %v", bucket, key, err)
		return err
	}

	return nil
}

func objectKeyWithSize(key string, size image.Point) string {
	ext := filepath.Ext(key)
	return strings.Replace(key, ext, fmt.Sprintf("_%dx%d.%s", size.X, size.Y, ext), -1)
}
