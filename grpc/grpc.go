package grpc

import (
	"context"
	"database/sql"
	"google.golang.org/grpc"
	"log"
	"mailing-list/mdb"
	"mailing-list/proto"
	"net"
	"time"
)

type MailServer struct {
	proto.UnimplementedMailingListServiceServer
	db *sql.DB
}

func protoEntryToMdbEntry(protoEntry *proto.EmailEntry) mdb.EmailEntry {
	t := time.Unix(protoEntry.ConfirmedAt, 0)
	return mdb.EmailEntry{
		Id:          protoEntry.Id,
		Email:       protoEntry.Email,
		ConfirmedAt: &t,
		OptOut:      protoEntry.OptOut,
	}
}

func mdbEntryToProtoEntry(mdbEntry *mdb.EmailEntry) proto.EmailEntry {
	return proto.EmailEntry{
		Id:          mdbEntry.Id,
		Email:       mdbEntry.Email,
		ConfirmedAt: mdbEntry.ConfirmedAt.Unix(),
		OptOut:      mdbEntry.OptOut,
	}
}

func emailResponse(db *sql.DB, email string) (*proto.EmailResponse, error) {
	entry, err := mdb.GetEmail(db, email)
	if err != nil {
		return &proto.EmailResponse{}, err
	}
	if entry == nil {
		return &proto.EmailResponse{}, nil
	}

	res := mdbEntryToProtoEntry(entry)

	return &proto.EmailResponse{EmailEntry: &res}, nil
}

func (s *MailServer) GetEmail(_ context.Context, req *proto.GetEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC GetEmail: %v\n", req)
	return emailResponse(s.db, req.EmailAddr)
}

func (s *MailServer) GetEmailBatch(_ context.Context, req *proto.GetEmailBatchRequest) (*proto.GetEmailBatchResponse, error) {
	log.Printf("gRPC GetEmailBatch: %v\n", req)

	params := mdb.GetEmailBatchQueryParams{
		Page:  int(req.Page),
		Count: int(req.Count),
	}

	mdbEntries, err := mdb.GetEmailBatch(s.db, params)
	if err != nil {
		return &proto.GetEmailBatchResponse{}, err
	}

	protoEntries := make([]*proto.EmailEntry, 0, len(mdbEntries))
	for i := 0; i < len(mdbEntries); i++ {
		entry := mdbEntryToProtoEntry(&mdbEntries[i])
		protoEntries = append(protoEntries, &entry)
	}

	return &proto.GetEmailBatchResponse{EmailEntries: protoEntries}, nil
}

func (s *MailServer) CreateEmail(_ context.Context, req *proto.CreateEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC CreateEmail: %v\n", req)

	err := mdb.CreateEmail(s.db, req.EmailAddr)
	if err != nil {
		return &proto.EmailResponse{}, err
	}

	return emailResponse(s.db, req.EmailAddr)
}

func (s *MailServer) UpdateEmail(_ context.Context, req *proto.UpdateEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC UpdateEmail: %v\n", req)

	entry := protoEntryToMdbEntry(req.EmailEntry)

	err := mdb.UpdateEmail(s.db, entry)
	if err != nil {
		return &proto.EmailResponse{}, err
	}

	return emailResponse(s.db, entry.Email)
}

func (s *MailServer) DeleteEmail(_ context.Context, req *proto.DeleteEmailRequest) (*proto.EmailResponse, error) {
	log.Printf("gRPC DeleteEmail: %v\n", req)

	err := mdb.DeleteEmail(s.db, req.EmailAddr)
	if err != nil {
		return &proto.EmailResponse{}, err
	}

	return emailResponse(s.db, req.EmailAddr)
}

func Serve(db *sql.DB, bind string) {
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatalf("gRPC server error: failure to bind %v\n", bind)
	}

	grpcServer := grpc.NewServer()

	mailServer := MailServer{db: db}

	proto.RegisterMailingListServiceServer(grpcServer, &mailServer)

	log.Printf("gRPC API server listening on %v\n", bind)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server error: %v\n", err)
	}
}
