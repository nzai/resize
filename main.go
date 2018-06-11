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
	"time"

	"image/jpeg"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/nfnt/resize"
)

var (
	sizePattern = regexp.MustCompile("(\\d+)x(\\d+)")
)

func main() {

	fmt.Printf("[Start]\n")
	config, err := readConfig()
	if err != nil {
		fmt.Printf("Read config failed due to %v\n", err)
		return
	}

	// 初始化s3 client
	creds := credentials.NewStaticCredentialsFromCreds(credentials.Value{AccessKeyID: config.AccessKeyID, SecretAccessKey: config.SecretAccessKey})
	awsConfig := aws.NewConfig().WithCredentials(creds).WithRegion(config.Region).WithMaxRetries(config.MaxRetry)
	client := s3.New(session.New(awsConfig))

	// 处理事件
	imaging := NewImaging(config, client)
	lambda.Start(imaging.S3Event)

	fmt.Printf("[End]\n")
}

// Config 配置
type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	MaxRetry        int
	Sizes           []image.Point
}

// readConfig 从环境变量中读取配置
func readConfig() (*Config, error) {
	accessKeyID := os.Getenv("AccessKeyID")
	secretAccessKey := os.Getenv("SecretAccessKey")
	region := os.Getenv("Region")
	sizeString := os.Getenv("Sizes")
	if accessKeyID == "" || secretAccessKey == "" || region == "" || sizeString == "" {
		return nil, fmt.Errorf("Environment viriables is invalid")
	}

	var sizes []image.Point
	for _, group := range sizePattern.FindAllStringSubmatch(sizeString, -1) {
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

	if os.Getenv("debug") == "true" {
		fmt.Printf("AccessKeyID: %s\n", accessKeyID)
		fmt.Printf("SecretAccessKey: %s\n", secretAccessKey)
		fmt.Printf("Sizes: %v\n", sizes)
		fmt.Printf("MaxRetries: %d\n", maxRetry)
	}

	return &Config{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Region:          region,
		Sizes:           sizes,
		MaxRetry:        maxRetry,
	}, nil
}

// Imaging 图片处理
type Imaging struct {
	config *Config
	client *s3.S3
}

// NewImaging 新建图片处理
func NewImaging(config *Config, client *s3.S3) *Imaging {
	return &Imaging{config: config, client: client}
}

// S3Event S3事件
func (s Imaging) S3Event(ctx context.Context, s3Event events.S3Event) {

	wg := new(sync.WaitGroup)
	wg.Add(len(s3Event.Records))

	for _, record := range s3Event.Records {

		// 创建了目录
		if strings.HasSuffix(record.S3.Object.Key, "/") {
			fmt.Printf("Ignore create dir %s\n", record.S3.Object.Key)
			wg.Done()
			continue
		}

		// 忽略resize上传的缩略图
		if sizePattern.Match([]byte(record.S3.Object.Key)) {
			fmt.Printf("Ignore thumbnail %s\n", record.S3.Object.Key)
			wg.Done()
			continue
		}

		// 只支持jpg
		if !strings.HasSuffix(strings.ToLower(record.S3.Object.Key), ".jpg") {
			fmt.Printf("Ignore unknown file type %s\n", record.S3.Object.Key)
			wg.Done()
			continue
		}

		fmt.Printf("Image created: %s\n", record.S3.Object.Key)
		// 并行创建缩略图
		go s.onImageCreated(ctx, record, wg)
	}
	wg.Wait()
}

// onImageCreated 有图片更新时创建缩略图
func (s Imaging) onImageCreated(ctx context.Context, record events.S3EventRecord, wg *sync.WaitGroup) {
	defer wg.Done()

	// 尝试从S3读取图像
	src, err := s.readImage(ctx, record)
	if err != nil {
		fmt.Printf("Read image from bucket %s object %s failed due to %v\n", record.S3.Bucket.Name, record.S3.Object.Key, err)
		return
	}

	thumbnailWaitGroup := new(sync.WaitGroup)
	thumbnailWaitGroup.Add(len(s.config.Sizes))
	for _, size := range s.config.Sizes {
		// 并行创建缩略图
		go s.createThumbnail(ctx, record.S3.Bucket.Name, record.S3.Object.Key, src, size, thumbnailWaitGroup)
	}

	thumbnailWaitGroup.Wait()
}

// readImage 从key中读取图像
func (s Imaging) readImage(ctx context.Context, record events.S3EventRecord) (image.Image, error) {

	start := time.Now()
	// 获取文件
	output, err := s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(record.S3.Object.Key),
	})
	if err != nil {
		fmt.Printf("Get object %s failed due to %v\n", record.S3.Object.Key, err)
		return nil, err
	}
	defer output.Body.Close()
	read := time.Now()
	fmt.Printf("Read image %s in %s\n", record.S3.Object.Key, read.Sub(start).String())

	// 读取图像
	img, err := jpeg.Decode(output.Body)
	if err != nil {
		fmt.Printf("Decode image from %s failed due to %v\n", record.S3.Object.Key, err)
		return nil, err
	}
	fmt.Printf("Decode image %s in %s\n", record.S3.Object.Key, time.Now().Sub(read).String())

	return img, nil
}

// createThumbnail 创建缩略图
func (s Imaging) createThumbnail(ctx context.Context, bucket, key string, src image.Image, size image.Point, wg *sync.WaitGroup) {
	defer wg.Done()
	start := time.Now()
	fmt.Printf("Start create %dx%d thumbnail for %s\n", size.X, size.Y, key)

	// 生成缩略图
	thumbnail := resize.Thumbnail(uint(size.X), uint(size.Y), src, resize.Bilinear)
	reiszed := time.Now()
	fmt.Printf("Create %dx%d thumbnail for %s in %s\n", size.X, size.Y, key, reiszed.Sub(start).String())

	// 尝试保存到S3
	thumbnailKey := s.thumbnailKey(key, size)
	err := s.saveThumbnail(ctx, thumbnail, bucket, thumbnailKey)
	if err != nil {
		fmt.Printf("Save thumbnail %s failed due to %v\n", thumbnailKey, err)
		return
	}

	// 发送完成通知
	fmt.Printf("Save thumbnail %s success in %s\n", thumbnailKey, time.Now().Sub(reiszed).String())
}

// saveThumbnail 保存缩略图
func (s Imaging) saveThumbnail(ctx context.Context, thumbnail image.Image, bucket, key string) error {

	// 按默认(75)的质量编码缩略图
	buffer := new(bytes.Buffer)
	err := jpeg.Encode(buffer, thumbnail, nil)
	if err != nil {
		fmt.Printf("Encode jpeg failed due to %v", err)
		return err
	}

	_, err = s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(buffer.Bytes()),
		StorageClass: aws.String(s3.ObjectStorageClassStandard),
		Metadata:     map[string]*string{"kind": aws.String("thumbnail")},
	})
	if err != nil {
		fmt.Printf("Put bucket %s object %s failed due to %v\n", bucket, key, err)
		return err
	}

	return nil
}

// thumbnailKey 缩略图的key
func (s Imaging) thumbnailKey(key string, size image.Point) string {
	ext := filepath.Ext(key)
	return strings.Replace(key, ext, fmt.Sprintf("_%dx%d%s", size.X, size.Y, ext), -1)
}
