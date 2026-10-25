package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
	"github.com/mdp/qrterminal"
	qrgen "rsc.io/qr"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages.
	// WAL + busy_timeout reduces SQLITE_BUSY contention between the
	// Go writer (handleMessage / handleHistorySync) and Python readers.
	db, err := sql.Open("sqlite", "file:store/messages.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages 
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	_, err = client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Remove query parameters
	pathPart = strings.SplitN(pathPart, "?", 2)[0]

	// Create proper direct path format
	return "/" + pathPart
}

// Start a REST API server to expose the WhatsApp client functionality.
// ln is the loopback listener main() claimed before connecting to WhatsApp.
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, ln net.Listener) {
	// /api/status + /api/groups/* (groups.go)
	registerGroupRoutes(client)

	// Handler: list all WhatsApp contacts known to the multi-device
	// session. This is RELIABLE — whatsmeow keeps a synced contact
	// store from the phone, with full names, push names, and business
	// names. Useful when messages.db is sparse (right after pairing).
	http.HandleFunc("/api/contacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		contacts, err := client.Store.Contacts.GetAllContacts(ctx)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{
				"success": false,
				"message": err.Error(),
			})
			return
		}
		queryParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := make([]map[string]any, 0, len(contacts))
		for jid, ci := range contacts {
			displayName := ci.FullName
			if displayName == "" {
				displayName = ci.PushName
			}
			if displayName == "" {
				displayName = ci.BusinessName
			}
			if queryParam != "" {
				hay := strings.ToLower(displayName + " " + ci.PushName + " " + ci.BusinessName + " " + jid.User)
				if !strings.Contains(hay, queryParam) {
					continue
				}
			}
			out = append(out, map[string]any{
				"jid":           jid.String(),
				"phone_number":  jid.User,
				"display_name":  displayName,
				"first_name":    ci.FirstName,
				"full_name":     ci.FullName,
				"push_name":     ci.PushName,
				"business_name": ci.BusinessName,
			})
		}
		// Sort alphabetically by display_name (case-insensitive) so
		// the result is deterministic and friendly.
		sortContacts(out)
		json.NewEncoder(w).Encode(map[string]any{
			"success":  true,
			"count":    len(out),
			"contacts": out,
		})
	})

	// Handler: best-effort per-chat history sync. EXPERIMENTAL.
	// The phone may take 30s+ to respond, may ignore the request, or
	// may only return duplicates. Use sparingly. Cooldown is enforced
	// at the bridge level (30s per call) to avoid spamming WhatsApp.
	//
	// Body: {chat_jid: string, count: int (default 50, max 100)}
	// Returns: {request_accepted, error, cooldown_remaining_sec,
	//           in_flight, last_attempt, anchor}
	http.HandleFunc("/api/sync-history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID string `json:"chat_jid"`
			Count   int    `json:"count"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.ChatJID == "" {
			http.Error(w, `{"error":"chat_jid required"}`, http.StatusBadRequest)
			return
		}
		if req.Count <= 0 {
			req.Count = 50
		} else if req.Count > 100 {
			req.Count = 100
		}

		// Cooldown / in-flight check
		historySyncMu.Lock()
		now := time.Now()
		remaining := historySyncCooldown - now.Sub(historySyncLastAttempt)
		if historySyncInFlight {
			historySyncMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"request_accepted": false,
				"in_flight":        true,
				"error":            "another history-sync request is in flight",
			})
			return
		}
		if !historySyncLastAttempt.IsZero() && remaining > 0 {
			historySyncMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"request_accepted":        false,
				"cooldown_remaining_sec":  int(remaining.Seconds()) + 1,
				"last_attempt":            historySyncLastAttempt.Format(time.RFC3339),
				"error":                   "cooldown active",
			})
			return
		}
		historySyncInFlight = true
		historySyncLastAttempt = now
		historySyncMu.Unlock()

		defer func() {
			historySyncMu.Lock()
			historySyncInFlight = false
			historySyncMu.Unlock()
		}()

		// Look up the most-recent stored message for this chat to use
		// as the anchor (whatsmeow back-paginates from anchor->older).
		var (
			anchorID    string
			anchorTS    time.Time
			anchorIsMe  bool
			anchorFound bool
		)
		row := messageStore.db.QueryRow(`
			SELECT id, timestamp, is_from_me FROM messages
			WHERE chat_jid = ?
			ORDER BY timestamp ASC LIMIT 1`, req.ChatJID)
		if err := row.Scan(&anchorID, &anchorTS, &anchorIsMe); err == nil {
			anchorFound = true
		}

		chatJID, err := types.ParseJID(req.ChatJID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{
				"request_accepted": false,
				"error":            fmt.Sprintf("invalid chat_jid: %v", err),
			})
			return
		}

		// If we have no anchor, synthesize one with time.Now() so the
		// phone is asked for messages older than "now" in this chat.
		// This is best-effort — whatsmeow's protocol prefers a real
		// stored anchor message, and the phone may ignore the request.
		if !anchorFound {
			anchorID = "synthetic-anchor"
			anchorTS = time.Now()
			anchorIsMe = false
		}
		anchor := &types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chatJID,
				Sender:   chatJID,
				IsFromMe: anchorIsMe,
			},
			ID:        anchorID,
			Timestamp: anchorTS,
		}

		err = requestHistorySync(client, anchor, req.Count)
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"request_accepted":   err == nil,
			"in_flight":          false,
			"anchor_synthetic":   !anchorFound,
			"chat_jid":           req.ChatJID,
			"count":              req.Count,
			"last_attempt":       historySyncLastAttempt.Format(time.RFC3339),
			"note":               "history sync events arrive asynchronously and are persisted to messages.db. Poll the messages table after a few seconds.",
		}
		if err != nil {
			resp["error"] = err.Error()
			w.WriteHeader(http.StatusBadGateway)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for the god-scheduler stop/continue intent check.
	//
	// Returns the latest control reply (`stop#<code>` or
	// `continue#<code>` on its own line, case-insensitive) found in
	// any chat. The scheduler uses this to honour opt-outs from
	// recipients of recurring WhatsApp reminders without needing
	// direct file-system access to the bridge's messages.db (which
	// matters for the Business bridge running on EC2, where the
	// scheduler talks over HTTPS via Apache reverse proxy).
	//
	// Query params:
	//   recipient — phone / JID / contact (currently informational;
	//               match is global because stop codes are unique
	//               per-task and LID migrations make chat_jid
	//               unreliable). Kept in the API for forward compat.
	//   stopCode  — the unique stop code emitted on each task.
	//
	// Response (always 200 unless DB error):
	//   { "intent": "stop"|"continue"|"none",
	//     "content": "<message body that matched, if any>",
	//     "messageId": "<messages.id PK of the match, if any>" }
	http.HandleFunc("/api/latest-intent", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		stopCode := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("stopCode")))
		if stopCode == "" {
			http.Error(w, "stopCode is required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		needle := "%#" + stopCode + "%"
		rows, err := messageStore.db.Query(
			"SELECT id, content FROM messages WHERE lower(content) LIKE ? ORDER BY timestamp DESC LIMIT 50",
			needle,
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		defer rows.Close()
		cmdRe, err := regexp.Compile(`(?im)^[\s>"'*_~]*(stop|continue)#` + regexp.QuoteMeta(stopCode) + `[\s.!?,;:"'*_~]*$`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		resp := map[string]string{"intent": "none", "content": "", "messageId": ""}
		for rows.Next() {
			var id, body string
			if scanErr := rows.Scan(&id, &body); scanErr != nil {
				continue
			}
			m := cmdRe.FindStringSubmatch(body)
			if m == nil {
				continue
			}
			cmd := strings.ToLower(m[1])
			if cmd == "stop" || cmd == "continue" {
				resp["intent"] = cmd
				resp["content"] = body
				resp["messageId"] = id
				break
			}
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Handler: fetch a contact/group's profile-picture URL. Read-only.
	// GET /api/profile-picture?jid=<jid>[&preview=1]
	// Returns {success, url, id, direct_path}. url is a short-lived
	// pps.whatsapp.net HTTPS link the caller can download directly.
	http.HandleFunc("/api/profile-picture", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rawJID := strings.TrimSpace(r.URL.Query().Get("jid"))
		if rawJID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "jid query param required"})
			return
		}
		jid, err := types.ParseJID(rawJID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "invalid jid: " + err.Error()})
			return
		}
		preview := r.URL.Query().Get("preview") == "1"
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		info, err := client.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: preview})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}
		if info == nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "no profile picture (private or none set)"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"success":     true,
			"url":         info.URL,
			"id":          info.ID,
			"type":        info.Type,
			"direct_path": info.DirectPath,
		})
	})

	// Bind to loopback only so the bridge isn't exposed on the LAN. The
	// listener was claimed in main() before connecting to WhatsApp.
	fmt.Printf("Starting REST API server on %s...\n", ln.Addr())

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

// bridgePortFromEnv returns the REST port (WHATSAPP_BRIDGE_PORT, default 9876,
// picked to avoid colliding with the god backend on 9871/9872/9873).
func bridgePortFromEnv() int {
	bridgePort := 9876
	if v := strings.TrimSpace(os.Getenv("WHATSAPP_BRIDGE_PORT")); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			bridgePort = p
		}
	}
	return bridgePort
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Single-instance guard: claim the REST port BEFORE touching the WhatsApp
	// session. Two bridges on one session make WhatsApp kick the older
	// connection ("stream replaced") - on 2026-10-25 a duplicate supervisor
	// started a second bridge that could not bind :9876 but still connected,
	// leaving the REST-serving bridge disconnected for hours. A bridge that
	// cannot own the port now exits without ever connecting.
	restListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", bridgePortFromEnv()))
	if err != nil {
		logger.Errorf("REST port unavailable (%v) - another bridge instance owns it; exiting WITHOUT connecting to WhatsApp", err)
		os.Exit(3)
	}

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite", "file:store/whatsapp.db?_pragma=foreign_keys(1)", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// v-stitch: request a FULL history sync at pair time so the companion
	// receives the group history that WhatsApp shares when this account is
	// added to a group ("history shared by <admin>"). The default companion
	// registration only asks for a thin recent sync, which is why group
	// history posted before this device was linked never reached the bridge.
	// Harmless once already paired (DeviceProps is only read at registration).
	store.DeviceProps.RequireFullSync = proto.Bool(true)
	if store.DeviceProps.HistorySyncConfig != nil {
		store.DeviceProps.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(90)
		store.DeviceProps.HistorySyncConfig.FullSyncSizeMbLimit = proto.Uint32(2048)
		store.DeviceProps.HistorySyncConfig.StorageQuotaMb = proto.Uint32(10240)
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.StreamReplaced:
			// Another client logged in with this session and WhatsApp closed
			// ours. whatsmeow never auto-reconnects after this, so staying up
			// would leave the REST API serving a dead connection. Exit and
			// let the supervisor restart us (we still own the REST port, so
			// no duplicate can be the one that reconnects).
			logger.Errorf("WhatsApp stream replaced by another client - exiting so the supervisor restarts the bridge")
			os.Exit(4)

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone (terminal) AND write a PNG
		// to apache so the user can scan it from a browser - the bridge
		// often runs unattended via Task Scheduler where stdout is hidden.
		// QR PNG output path is overridable via WHATSAPP_QR_PATH so a
		// second instance (e.g. a Business account on EC2) can pair
		// without trampling the personal account's QR file. An optional
		// WHATSAPP_QR_URL prints a friendly scan-this-URL line.
		qrPath := strings.TrimSpace(os.Getenv("WHATSAPP_QR_PATH"))
		if qrPath == "" {
			qrPath = `C:\Users\arushi\apache-www\god\whatsapp\qr.png`
		}
		qrURL := strings.TrimSpace(os.Getenv("WHATSAPP_QR_URL"))
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				if code, err := qrgen.Encode(evt.Code, qrgen.M); err == nil {
					png := code.PNG()
					_ = os.MkdirAll(filepath.Dir(qrPath), 0o755)
					if err := os.WriteFile(qrPath, png, 0o644); err == nil {
						fmt.Printf("QR PNG also written to %s\n", qrPath)
						if qrURL != "" {
							fmt.Printf("Open %s and scan with WhatsApp -> Linked devices.\n", qrURL)
						} else {
							fmt.Println("Open https://gogillu.in:9871/whatsapp/qr.png (or http://20.244.37.91/god/whatsapp/qr.png) and scan with WhatsApp -> Linked devices.")
						}
					}
				}
			} else if evt.Event == "success" {
				connected <- true
				_ = os.Remove(qrPath)
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	startRESTServer(client, messageStore, restListener)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// Cooldown state for /api/sync-history. Prevents rapid-fire history
// requests from spamming the WhatsApp peer protocol when the MCP server
// retries (or when multiple clients call simultaneously).
var (
	historySyncMu             sync.Mutex
	historySyncInFlight       bool
	historySyncLastAttempt    time.Time
	historySyncCooldown       = 30 * time.Second
)

// requestHistorySync asks the phone (our primary device) for up to
// `count` messages older than `anchor` in the chat indicated by
// anchor.Chat. anchor must be a real, previously-stored message —
// whatsmeow's HistorySyncOnDemandRequest is per-chat back-pagination,
// not a "give me everything" backfill. (For pre-pair backfill the
// proven path is RequireFullSync at pair-time, which is too late here.)
func requestHistorySync(client *whatsmeow.Client, anchor *types.MessageInfo, count int) error {
	if client == nil {
		return fmt.Errorf("client is not initialized")
	}
	if !client.IsConnected() {
		return fmt.Errorf("client is not connected to WhatsApp")
	}
	if client.Store.ID == nil {
		return fmt.Errorf("client is not logged in")
	}
	if anchor == nil {
		return fmt.Errorf("anchor message is required (whatsmeow needs a real per-chat anchor)")
	}
	if count <= 0 {
		count = 50
	}

	historyMsg := client.BuildHistorySyncRequest(anchor, count)
	if historyMsg == nil {
		return fmt.Errorf("failed to build history sync request")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// SendPeerMessage routes via the multi-device protocol to our own
	// linked devices (the phone), bypassing the usync pipeline that
	// times out when sending to status@s.whatsapp.net.
	_, err := client.SendPeerMessage(ctx, historyMsg)
	return err
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}


// sortContacts orders the contacts slice in-place alphabetically by
// display_name, case-insensitively. Used by /api/contacts.
func sortContacts(rows []map[string]any) {
sort.SliceStable(rows, func(i, j int) bool {
ai, _ := rows[i]["display_name"].(string)
aj, _ := rows[j]["display_name"].(string)
return strings.ToLower(ai) < strings.ToLower(aj)
})
}
