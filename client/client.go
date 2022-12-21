package main

import (
	"context"
	"github.com/alexflint/go-arg"
	"github.com/erayaydin/mailing-list/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

func logResponse(res *proto.EmailResponse, err error) {
	if err != nil {
		log.Fatalf("└──── error: %v", err)
	}

	if res.EmailEntry == nil {
		log.Printf("└──── email not found")
		return
	}

	log.Printf("└──── response: %v", res.EmailEntry)
}

func createEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("create email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.CreateEmail(ctx, &proto.CreateEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("get email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmail(ctx, &proto.GetEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmailBatch(client proto.MailingListServiceClient, count int, page int) {
	log.Println("get email batch")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmailBatch(ctx, &proto.GetEmailBatchRequest{Page: int32(page), Count: int32(count)})
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	for i := 0; i < len(res.EmailEntries); i++ {
		log.Printf("└──── item [%v of %v]: %s", i+1, len(res.EmailEntries), res.EmailEntries[i])
	}
}

func updateEmail(client proto.MailingListServiceClient, entry proto.EmailEntry) *proto.EmailEntry {
	log.Println("update email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.UpdateEmail(ctx, &proto.UpdateEmailRequest{EmailEntry: &entry})
	logResponse(res, err)

	return res.EmailEntry
}

func deleteEmail(client proto.MailingListServiceClient, addr string) *proto.EmailEntry {
	log.Println("delete email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.DeleteEmail(ctx, &proto.DeleteEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

var args struct {
	GrpcAddr string `arg:"env:MAILINGLIST_GRPC_ADDR"`
}

func main() {
	arg.MustParse(&args)

	if args.GrpcAddr == "" {
		args.GrpcAddr = ":8081"
	}

	conn, err := grpc.Dial(args.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("could not close client connection: %v", err)
		}
	}(conn)

	client := proto.NewMailingListServiceClient(conn)

	newEmail := createEmail(client, "client5@test.tld")
	newEmail.ConfirmedAt = 10000
	updateEmail(client, *newEmail)
	deleteEmail(client, newEmail.Email)
	getEmailBatch(client, 5, 1)
}
