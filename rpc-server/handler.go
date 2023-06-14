package main

import (
	"context"
	"math/rand"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)


// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	// Save the message details to the database
	err := saveMessageToDatabase(req.Chat, req.Text, req.Sender)
	if err != nil {
		return nil, err
	}
	// Create and return the send response
	resp := &api.SendResponse{}
	return resp, nil

}


func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	// Retrieve messages from the database based on the pull request
	messages, err := retrieveMessagesFromDatabase(req.Chat, req.Cursor, req.Limit, req.Reverse)
	if err != nil {
		return nil, err
	}
	// Create the pull response with the retrieved messages
	resp := &api.PullResponse{
		Messages: messages,
		HasMore:  len(messages) == req.Limit, // Determine if there are more messages available
	}
	// Set the next cursor if there are more messages
	if len(messages) > 0 {
		lastMessage := messages[len(messages)-1]
		resp.NextCursor = lastMessage.SendTime
	}

	return resp, nil
}



func saveMessageToDatabase(db *sql.DB, sender, receiver, message string) error {
	query := "INSERT INTO user (sender, receiver, message, timestamp) VALUES (?, ?, ?, CURRENT_TIMESTAMP)"
	_, err := db.Exec(query, sender, receiver, message)
	if err != nil {
		return err
	}
	log.Println("Message saved to the database")
	return nil
}



func saveMessageToDatabase(db *sql.DB, sender, receiver, message string) error {
	query := "INSERT INTO user (sender, receiver, message, timestamp) VALUES (?, ?, ?, CURRENT_TIMESTAMP)"
	_, err := db.Exec(query, sender, receiver, message)
	if err != nil {
		return err
	}
	log.Println("Message saved to the database")
	return nil
}

func retrieveMessagesFromDatabase(db *sql.DB, chat string, cursor int64, limit int32, reverse bool) ([]*api.Message, error) {
	var query string
	var args []interface{}

	if reverse {
		query = "SELECT sender, receiver, message, timestamp FROM user WHERE (sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?) AND timestamp < ? ORDER BY timestamp DESC LIMIT ?"
		args = []interface{}{chat, chat, chat, chat, cursor, limit}
	} else {
		query = "SELECT sender, receiver, message, timestamp FROM user WHERE (sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?) AND timestamp > ? ORDER BY timestamp ASC LIMIT ?"
		args = []interface{}{chat, chat, chat, chat, cursor, limit}
	}

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []*api.Message{}
	for rows.Next() {
		var sender, receiver, message string
		var timestamp string
		err := rows.Scan(&sender, &receiver, &message, &timestamp)
		if err != nil {
			return nil, err
		}
		message := &api.Message{
			Sender:    sender,
			Receiver:  receiver,
			Message:   message,
			Timestamp: timestamp,
		}
		messages = append(messages, message)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("Retrieved %d messages from the database", len(messages))
	return messages, nil
}