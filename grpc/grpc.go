package grpc

import (
	"database/sql"
	"mailing-list/mdb"
	"mailing-list/proto"
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
