package main

// groups.go - WhatsApp group management over the loopback REST API.
//
// Added for the Nimantran guest-list app (nimantran.gogillu.live): create
// the wedding group from this account, add guests in small paced batches,
// promote family members to admin, send group-invite messages to people
// whose privacy settings block a direct add, and read live membership so
// the app can show who is (and is not) in each group.
//
// Pacing / anti-spam policy lives in the CALLER (Nimantran adds a few
// people per request with long gaps). These handlers do exactly one
// WhatsApp operation per request and never loop over large lists.
//
// Every response is JSON with "success". Failures carry "message" and,
// for WhatsApp IQ errors, the numeric "code" (e.g. 406 = subject not
// acceptable, 403 = not an admin, 404 = group not found).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type grpParticipant struct {
	JID                  string `json:"jid"`
	Phone                string `json:"phone"`
	LID                  string `json:"lid,omitempty"`
	IsAdmin              bool   `json:"is_admin"`
	IsSuperAdmin         bool   `json:"is_super_admin"`
	DisplayName          string `json:"display_name,omitempty"`
	Error                int    `json:"error,omitempty"`
	Status               string `json:"status"`
	AddRequestCode       string `json:"add_request_code,omitempty"`
	AddRequestExpiration int64  `json:"add_request_expiration,omitempty"`
}

type grpInfo struct {
	JID              string           `json:"jid"`
	Name             string           `json:"name"`
	Topic            string           `json:"topic,omitempty"`
	OwnerPhone       string           `json:"owner_phone,omitempty"`
	Created          int64            `json:"created"`
	AddressingMode   string           `json:"addressing_mode,omitempty"`
	ParticipantCount int              `json:"participant_count"`
	IsAnnounce       bool             `json:"is_announce"`
	IsLocked         bool             `json:"is_locked"`
	MemberAddMode    string           `json:"member_add_mode,omitempty"`
	MeAdmin          bool             `json:"me_admin"`
	MeSuperAdmin     bool             `json:"me_super_admin"`
	Participants     []grpParticipant `json:"participants,omitempty"`
}

func grpJSON(w http.ResponseWriter, status int, v map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func grpFail(w http.ResponseWriter, status int, err error, msg string) {
	out := map[string]any{"success": false, "message": msg}
	if err != nil {
		out["message"] = msg + ": " + err.Error()
		var iq *whatsmeow.IQError
		if errors.As(err, &iq) {
			out["code"] = iq.Code
		}
	}
	grpJSON(w, status, out)
}

// participantStatus maps WhatsApp's per-participant error codes.
func participantStatus(code int) string {
	switch code {
	case 0, 200:
		return "ok"
	case 403:
		return "invite_required" // privacy setting blocks direct add; add_request carries an invite code
	case 408:
		return "recently_left" // left recently; can only rejoin via invite
	case 409:
		return "already"
	case 404:
		return "not_found"
	case 401:
		return "not_authorized"
	default:
		return "error"
	}
}

func grpDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// parsePhones turns "+91 98260 12345", "919826012345" or a full JID into JIDs.
func parsePhones(in []string) ([]types.JID, []string) {
	var out []types.JID
	var bad []string
	seen := map[string]bool{}
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		var j types.JID
		if strings.Contains(raw, "@") {
			parsed, err := types.ParseJID(raw)
			if err != nil {
				bad = append(bad, raw)
				continue
			}
			j = parsed.ToNonAD()
		} else {
			d := grpDigits(raw)
			if len(d) < 8 || len(d) > 15 {
				bad = append(bad, raw)
				continue
			}
			j = types.NewJID(d, types.DefaultUserServer)
		}
		if !seen[j.String()] {
			seen[j.String()] = true
			out = append(out, j)
		}
	}
	return out, bad
}

func parseGroupJID(s string) (types.JID, error) {
	j, err := types.ParseJID(strings.TrimSpace(s))
	if err != nil {
		return j, err
	}
	if j.Server != types.GroupServer {
		return j, fmt.Errorf("%q is not a group JID (@g.us)", s)
	}
	return j, nil
}

// participantOut renders a participant, resolving its phone number even when
// the group uses LID addressing.
func participantOut(ctx context.Context, client *whatsmeow.Client, p types.GroupParticipant) grpParticipant {
	out := grpParticipant{
		JID:          p.JID.String(),
		IsAdmin:      p.IsAdmin,
		IsSuperAdmin: p.IsSuperAdmin,
		DisplayName:  p.DisplayName,
		Error:        p.Error,
		Status:       participantStatus(p.Error),
	}
	if !p.LID.IsEmpty() {
		out.LID = p.LID.String()
	}
	switch {
	case !p.PhoneNumber.IsEmpty():
		out.Phone = p.PhoneNumber.User
	case p.JID.Server == types.DefaultUserServer:
		out.Phone = p.JID.User
	case p.JID.Server == types.HiddenUserServer:
		if pn, err := client.Store.LIDs.GetPNForLID(ctx, p.JID); err == nil && !pn.IsEmpty() {
			out.Phone = pn.User
		}
	}
	if p.AddRequest != nil {
		out.AddRequestCode = p.AddRequest.Code
		out.AddRequestExpiration = p.AddRequest.Expiration.Unix()
	}
	return out
}

func myIdentity(client *whatsmeow.Client) (phone string, lid types.JID) {
	if client.Store.ID != nil {
		phone = client.Store.ID.User
	}
	return phone, client.Store.LID
}

func groupOut(ctx context.Context, client *whatsmeow.Client, g *types.GroupInfo, withParticipants bool) grpInfo {
	myPhone, myLID := myIdentity(client)
	out := grpInfo{
		JID:              g.JID.String(),
		Name:             g.Name,
		Topic:            g.Topic,
		AddressingMode:   string(g.AddressingMode),
		ParticipantCount: len(g.Participants),
		IsAnnounce:       g.IsAnnounce,
		IsLocked:         g.IsLocked,
		MemberAddMode:    string(g.MemberAddMode),
	}
	if g.ParticipantCount > out.ParticipantCount {
		out.ParticipantCount = g.ParticipantCount
	}
	if !g.GroupCreated.IsZero() {
		out.Created = g.GroupCreated.Unix()
	}
	if !g.OwnerPN.IsEmpty() {
		out.OwnerPhone = g.OwnerPN.User
	} else if g.OwnerJID.Server == types.DefaultUserServer {
		out.OwnerPhone = g.OwnerJID.User
	}
	for _, p := range g.Participants {
		isMe := (myPhone != "" && (p.PhoneNumber.User == myPhone || (p.JID.Server == types.DefaultUserServer && p.JID.User == myPhone))) ||
			(!myLID.IsEmpty() && (p.JID.User == myLID.User && p.JID.Server == types.HiddenUserServer || p.LID.User == myLID.User))
		if isMe {
			out.MeAdmin = p.IsAdmin
			out.MeSuperAdmin = p.IsSuperAdmin
		}
		if withParticipants {
			out.Participants = append(out.Participants, participantOut(ctx, client, p))
		}
	}
	return out
}

func requireConnected(w http.ResponseWriter, client *whatsmeow.Client) bool {
	if !client.IsConnected() || !client.IsLoggedIn() {
		grpJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "message": "bridge is not connected to WhatsApp"})
		return false
	}
	return true
}

// resolveForAdd mirrors what the official clients do before adding people:
// look the numbers up (usync / IsOnWhatsApp), drop the ones that are not on
// WhatsApp, and address the rest by LID when known (every group on this
// account is LID-addressed; whatsmeow attaches the phone_number itself).
// It returns the JIDs to send plus a phone -> JID map for result mapping.
func resolveForAdd(ctx context.Context, client *whatsmeow.Client, jids []types.JID) (send []types.JID, notOnWA []string, err error) {
	var queries []string
	for _, j := range jids {
		if j.Server == types.DefaultUserServer {
			queries = append(queries, "+"+j.User)
		}
	}
	found := map[string]types.IsOnWhatsAppResponse{}
	if len(queries) > 0 {
		res, err := client.IsOnWhatsApp(ctx, queries)
		if err != nil {
			return nil, nil, fmt.Errorf("IsOnWhatsApp: %w", err)
		}
		for _, x := range res {
			found[grpDigits(x.Query)] = x
		}
	}
	for _, j := range jids {
		if j.Server != types.DefaultUserServer {
			send = append(send, j)
			continue
		}
		x, ok := found[j.User]
		switch {
		case ok && !x.IsIn:
			notOnWA = append(notOnWA, j.User)
		case ok && !x.JID.IsEmpty():
			send = append(send, x.JID.ToNonAD())
		default:
			send = append(send, j) // not in the usync answer: try the phone JID as-is
		}
	}
	return send, notOnWA, nil
}

// perRequestResults returns exactly one result per requested phone, in
// request order, from what WhatsApp answered (results are matched by phone).
func perRequestResults(ctx context.Context, client *whatsmeow.Client, requested []types.JID, notOnWA []string, answered []types.GroupParticipant) []grpParticipant {
	byPhone := map[string]grpParticipant{}
	for _, p := range answered {
		po := participantOut(ctx, client, p)
		if po.Phone != "" {
			byPhone[po.Phone] = po
		}
	}
	skip := map[string]bool{}
	for _, p := range notOnWA {
		skip[p] = true
	}
	out := make([]grpParticipant, 0, len(requested))
	for _, j := range requested {
		phone := j.User
		if j.Server == types.HiddenUserServer {
			if pn, err := client.Store.LIDs.GetPNForLID(ctx, j); err == nil && !pn.IsEmpty() {
				phone = pn.User
			}
		}
		switch {
		case skip[phone]:
			out = append(out, grpParticipant{JID: j.String(), Phone: phone, Error: 404, Status: "not_on_whatsapp"})
		default:
			if po, ok := byPhone[phone]; ok {
				out = append(out, po)
			} else {
				out = append(out, grpParticipant{JID: j.String(), Phone: phone, Status: "unknown"})
			}
		}
	}
	return out
}

func registerGroupRoutes(client *whatsmeow.Client) {
	// GET /api/status - who is this bridge and is it online?
	http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		phone, lid := myIdentity(client)
		out := map[string]any{
			"success":   true,
			"connected": client.IsConnected(),
			"logged_in": client.IsLoggedIn(),
			"phone":     phone,
			"push_name": client.Store.PushName,
		}
		if !lid.IsEmpty() {
			out["lid"] = lid.String()
		}
		if client.Store.ID != nil {
			out["jid"] = client.Store.ID.ToNonAD().String()
		}
		if ps := currentPairingStatus(); ps.Active {
			out["pairing"] = ps
		}
		grpJSON(w, http.StatusOK, out)
	})

	// POST /api/groups/check {phones:[...]} - which numbers are on WhatsApp.
	http.HandleFunc("/api/groups/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Phones []string `json:"phones"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		if len(req.Phones) == 0 || len(req.Phones) > 50 {
			grpFail(w, http.StatusBadRequest, nil, "send 1-50 phones per request")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		queries := make([]string, 0, len(req.Phones))
		for _, p := range req.Phones {
			if d := grpDigits(p); len(d) >= 8 && len(d) <= 15 {
				queries = append(queries, "+"+d)
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		defer cancel()
		res, err := client.IsOnWhatsApp(ctx, queries)
		if err != nil {
			grpFail(w, http.StatusBadGateway, err, "IsOnWhatsApp failed")
			return
		}
		out := make([]map[string]any, 0, len(res))
		for _, x := range res {
			item := map[string]any{"phone": grpDigits(x.Query), "on_whatsapp": x.IsIn}
			if !x.JID.IsEmpty() {
				item["jid"] = x.JID.String()
			}
			out = append(out, item)
		}
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "results": out})
	})

	// POST /api/groups/create {name, participants:[phones], member_add_mode?}
	http.HandleFunc("/api/groups/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Name          string   `json:"name"`
			Participants  []string `json:"participants"`
			MemberAddMode string   `json:"member_add_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		name := strings.TrimSpace(req.Name)
		if name == "" {
			grpFail(w, http.StatusBadRequest, nil, "name is required")
			return
		}
		jids, bad := parsePhones(req.Participants)
		if len(bad) > 0 {
			grpJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "invalid participants", "invalid": bad})
			return
		}
		if len(jids) > 50 {
			grpFail(w, http.StatusBadRequest, nil, "create with at most 50 participants; add the rest in batches")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		send, notOnWA, err := resolveForAdd(ctx, client, jids)
		if err != nil {
			grpFail(w, http.StatusBadGateway, err, "number lookup failed")
			return
		}
		creq := whatsmeow.ReqCreateGroup{Name: name, Participants: send}
		switch req.MemberAddMode {
		case string(types.GroupMemberAddModeAdmin), string(types.GroupMemberAddModeAllMember):
			creq.MemberAddMode = types.GroupMemberAddMode(req.MemberAddMode)
		}
		info, err := client.CreateGroup(ctx, creq)
		if err != nil {
			fmt.Printf("[groups] create %q with %d participant(s) FAILED: %v\n", name, len(send), err)
			grpFail(w, http.StatusBadGateway, err, "create group failed")
			return
		}
		fmt.Printf("[groups] created %s %q with %d participant(s) (%d not on WhatsApp)\n", info.JID, info.Name, len(send), len(notOnWA))
		grpJSON(w, http.StatusOK, map[string]any{
			"success": true,
			"group":   groupOut(ctx, client, info, true),
			"results": perRequestResults(ctx, client, jids, notOnWA, info.Participants),
		})
	})

	// POST /api/groups/participants {group_jid, action: add|remove|promote|demote, participants:[phones]}
	http.HandleFunc("/api/groups/participants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			GroupJID     string   `json:"group_jid"`
			Action       string   `json:"action"`
			Participants []string `json:"participants"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		gjid, err := parseGroupJID(req.GroupJID)
		if err != nil {
			grpFail(w, http.StatusBadRequest, err, "bad group_jid")
			return
		}
		action := whatsmeow.ParticipantChange(req.Action)
		switch action {
		case whatsmeow.ParticipantChangeAdd, whatsmeow.ParticipantChangeRemove, whatsmeow.ParticipantChangePromote, whatsmeow.ParticipantChangeDemote:
		default:
			grpFail(w, http.StatusBadRequest, nil, "action must be add, remove, promote or demote")
			return
		}
		jids, bad := parsePhones(req.Participants)
		if len(bad) > 0 {
			grpJSON(w, http.StatusBadRequest, map[string]any{"success": false, "message": "invalid participants", "invalid": bad})
			return
		}
		if len(jids) == 0 || len(jids) > 20 {
			grpFail(w, http.StatusBadRequest, nil, "send 1-20 participants per request")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		requested := append([]types.JID(nil), jids...)
		var notOnWA []string
		if action == whatsmeow.ParticipantChangeAdd {
			jids, notOnWA, err = resolveForAdd(ctx, client, jids)
			if err != nil {
				grpFail(w, http.StatusBadGateway, err, "number lookup failed")
				return
			}
		} else {
			// remove/promote/demote must name the participant exactly as the
			// group lists it (a LID in LID-addressed groups).
			info, err := client.GetGroupInfo(ctx, gjid)
			if err != nil {
				grpFail(w, http.StatusBadGateway, err, "get group info failed")
				return
			}
			byPhone := map[string]types.JID{}
			for _, p := range info.Participants {
				po := participantOut(ctx, client, p)
				if po.Phone != "" {
					byPhone[po.Phone] = p.JID
				}
			}
			for i, j := range jids {
				if j.Server == types.DefaultUserServer {
					if real, ok := byPhone[j.User]; ok {
						jids[i] = real
					}
				}
			}
		}
		var res []types.GroupParticipant
		if len(jids) > 0 {
			res, err = client.UpdateGroupParticipants(ctx, gjid, jids, action)
			if err != nil {
				fmt.Printf("[groups] %s %d participant(s) in %s FAILED: %v\n", action, len(jids), gjid, err)
				grpFail(w, http.StatusBadGateway, err, string(action)+" failed")
				return
			}
		}
		out := perRequestResults(ctx, client, requested, notOnWA, res)
		okCount := 0
		for _, p := range out {
			if p.Status == "ok" {
				okCount++
			}
		}
		fmt.Printf("[groups] %s %d/%d participant(s) in %s\n", action, okCount, len(requested), gjid)
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "results": out})
	})

	// GET /api/groups/info?jid=<group jid>
	http.HandleFunc("/api/groups/info", func(w http.ResponseWriter, r *http.Request) {
		gjid, err := parseGroupJID(r.URL.Query().Get("jid"))
		if err != nil {
			grpFail(w, http.StatusBadRequest, err, "bad jid")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		defer cancel()
		info, err := client.GetGroupInfo(ctx, gjid)
		if err != nil {
			status := http.StatusBadGateway
			if errors.Is(err, whatsmeow.ErrGroupNotFound) || errors.Is(err, whatsmeow.ErrNotInGroup) {
				status = http.StatusNotFound
			}
			grpFail(w, status, err, "get group info failed")
			return
		}
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "group": groupOut(ctx, client, info, true)})
	})

	// GET /api/groups[?participants=1] - every group this account is in.
	http.HandleFunc("/api/groups", func(w http.ResponseWriter, r *http.Request) {
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		groups, err := client.GetJoinedGroups(ctx)
		if err != nil {
			grpFail(w, http.StatusBadGateway, err, "list groups failed")
			return
		}
		withP := r.URL.Query().Get("participants") == "1"
		out := make([]grpInfo, 0, len(groups))
		for _, g := range groups {
			if g == nil {
				continue
			}
			out = append(out, groupOut(ctx, client, g, withP))
		}
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "count": len(out), "groups": out})
	})

	// GET /api/groups/invite-link?jid=<group jid>[&reset=1]
	http.HandleFunc("/api/groups/invite-link", func(w http.ResponseWriter, r *http.Request) {
		gjid, err := parseGroupJID(r.URL.Query().Get("jid"))
		if err != nil {
			grpFail(w, http.StatusBadRequest, err, "bad jid")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		link, err := client.GetGroupInviteLink(ctx, gjid, r.URL.Query().Get("reset") == "1")
		if err != nil {
			grpFail(w, http.StatusBadGateway, err, "get invite link failed")
			return
		}
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "link": link})
	})

	// POST /api/groups/name {group_jid, name}
	http.HandleFunc("/api/groups/name", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			GroupJID string `json:"group_jid"`
			Name     string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		gjid, err := parseGroupJID(req.GroupJID)
		if err != nil || strings.TrimSpace(req.Name) == "" {
			grpFail(w, http.StatusBadRequest, err, "group_jid and name are required")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := client.SetGroupName(ctx, gjid, strings.TrimSpace(req.Name)); err != nil {
			grpFail(w, http.StatusBadGateway, err, "rename failed")
			return
		}
		fmt.Printf("[groups] renamed %s to %q\n", gjid, req.Name)
		grpJSON(w, http.StatusOK, map[string]any{"success": true})
	})

	// POST /api/groups/invite-message {group_jid, phone, code?, expiration?, group_name, caption}
	// With the add_request code from a privacy-blocked add, sends WhatsApp's
	// native "join group" invite card. Without a code it sends the group's
	// invite link as text.
	http.HandleFunc("/api/groups/invite-message", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			GroupJID   string `json:"group_jid"`
			Phone      string `json:"phone"`
			Code       string `json:"code"`
			Expiration int64  `json:"expiration"`
			GroupName  string `json:"group_name"`
			Caption    string `json:"caption"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		gjid, err := parseGroupJID(req.GroupJID)
		if err != nil {
			grpFail(w, http.StatusBadRequest, err, "bad group_jid")
			return
		}
		to, bad := parsePhones([]string{req.Phone})
		if len(bad) > 0 || len(to) != 1 {
			grpFail(w, http.StatusBadRequest, nil, "a valid phone is required")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		var msg *waProto.Message
		kind := "invite_card"
		if strings.TrimSpace(req.Code) != "" {
			exp := req.Expiration
			if exp <= 0 {
				exp = time.Now().Add(72 * time.Hour).Unix()
			}
			msg = &waProto.Message{GroupInviteMessage: &waProto.GroupInviteMessage{
				GroupJID:         proto.String(gjid.String()),
				InviteCode:       proto.String(strings.TrimSpace(req.Code)),
				InviteExpiration: proto.Int64(exp),
				GroupName:        proto.String(req.GroupName),
				Caption:          proto.String(req.Caption),
			}}
		} else {
			link, err := client.GetGroupInviteLink(ctx, gjid, false)
			if err != nil {
				grpFail(w, http.StatusBadGateway, err, "get invite link failed")
				return
			}
			text := strings.TrimSpace(req.Caption + "\n\n" + link)
			msg = &waProto.Message{Conversation: proto.String(text)}
			kind = "invite_link"
		}
		resp, err := client.SendMessage(ctx, to[0], msg)
		if err != nil {
			grpFail(w, http.StatusBadGateway, err, "send invite failed")
			return
		}
		fmt.Printf("[groups] sent %s for %s to %s\n", kind, gjid, to[0].User)
		grpJSON(w, http.StatusOK, map[string]any{"success": true, "kind": kind, "message_id": resp.ID})
	})

	// POST /api/groups/leave {group_jid}
	http.HandleFunc("/api/groups/leave", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			GroupJID string `json:"group_jid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			grpFail(w, http.StatusBadRequest, err, "invalid JSON")
			return
		}
		gjid, err := parseGroupJID(req.GroupJID)
		if err != nil {
			grpFail(w, http.StatusBadRequest, err, "bad group_jid")
			return
		}
		if !requireConnected(w, client) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := client.LeaveGroup(ctx, gjid); err != nil {
			grpFail(w, http.StatusBadGateway, err, "leave failed")
			return
		}
		fmt.Printf("[groups] left %s\n", gjid)
		grpJSON(w, http.StatusOK, map[string]any{"success": true})
	})
}
