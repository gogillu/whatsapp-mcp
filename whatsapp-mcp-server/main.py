import os
import sys
import threading
import time
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
    list_messages as whatsapp_list_messages,
    list_chats as whatsapp_list_chats,
    get_chat as whatsapp_get_chat,
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
    get_contact_chats as whatsapp_get_contact_chats,
    get_last_interaction as whatsapp_get_last_interaction,
    get_message_context as whatsapp_get_message_context,
    send_message as whatsapp_send_message,
    send_file as whatsapp_send_file,
    send_audio_message as whatsapp_audio_voice_message,
    download_media as whatsapp_download_media,
    list_all_contacts as whatsapp_list_all_contacts,
    sync_history as whatsapp_sync_history,
)


# --- Orphan-process watchdog ------------------------------------------------
#
# This MCP server is launched as a stdio child of the MCP host (Copilot CLI,
# Claude Desktop, etc.) via `uv run main.py`. When the host crashes or is
# killed ungracefully, the chain (host → uv → python) can end up with the
# python child orphaned: its stdin pipe is broken but FastMCP's anyio loop
# stays parked waiting for the next request and never notices. Result: every
# host crash leaks one whatsapp-mcp python process.
#
# Two cheap watchdogs fix this:
#   1. Parent watchdog — wait for the SPECIFIC parent process to exit:
#      * On Windows: OpenProcess(PROCESS_SYNCHRONIZE, ppid) + WaitForSingleObject
#        on the resulting handle. The handle is bound to the kernel process
#        object, not the PID, so PID reuse (which is aggressive on Windows
#        and routinely defeats `os.kill(ppid, 0)` polling — re-allocates the
#        dead parent's PID to a fresh unrelated process within seconds, so
#        the kill check keeps reporting "alive" and the watchdog never
#        fires) cannot mask the parent's death. This was the actual cause
#        of every leaked python.exe with multi-day uptime that the user
#        kept finding in Task Manager.
#      * On POSIX: poll os.kill(ppid, 0). Dead parents are reaped, so kill(0)
#        raises ESRCH cleanly. PID reuse on POSIX is much slower and the
#        race is tolerable.
#   2. Stdin EOF watchdog — read one byte from stdin in a background thread.
#      Returns immediately on EOF when the host closes the pipe. (FastMCP's
#      own stdio reader will get the same EOF and shut down cleanly first;
#      this is the belt to its braces.) Disabled by default because it
#      competes with FastMCP for the stdin pipe.
#
# Both threads are daemon=True so they never block a normal shutdown.

def _parent_watchdog_windows(initial_ppid: int) -> None:
    """Block on a Win32 handle to the parent process until it exits.

    OpenProcess + WaitForSingleObject is the canonical "wait for parent
    to die" pattern on Windows. The handle is bound to the specific
    kernel process object, so the watchdog correctly detects parent
    death even when the dead PID is later re-allocated to another
    process. Polling-by-PID does NOT have this property and is the bug
    we are fixing.
    """
    import ctypes
    from ctypes import wintypes

    PROCESS_SYNCHRONIZE = 0x00100000
    INFINITE = 0xFFFFFFFF
    WAIT_OBJECT_0 = 0x00000000
    WAIT_FAILED = 0xFFFFFFFF
    ERROR_ACCESS_DENIED = 5
    ERROR_INVALID_PARAMETER = 87

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    OpenProcess = kernel32.OpenProcess
    OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    OpenProcess.restype = wintypes.HANDLE
    WaitForSingleObject = kernel32.WaitForSingleObject
    WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    WaitForSingleObject.restype = wintypes.DWORD
    CloseHandle = kernel32.CloseHandle
    CloseHandle.argtypes = [wintypes.HANDLE]
    CloseHandle.restype = wintypes.BOOL

    h = OpenProcess(PROCESS_SYNCHRONIZE, False, initial_ppid)
    if not h:
        err = ctypes.get_last_error()
        # ERROR_INVALID_PARAMETER usually means the PID is already gone.
        # ERROR_ACCESS_DENIED means a sandboxed/elevated parent we can't
        # touch — exit either way; the alternative is silently falling
        # back to the broken poll path that caused this leak.
        reason = "gone" if err == ERROR_INVALID_PARAMETER else (
            "access denied" if err == ERROR_ACCESS_DENIED else f"err={err}"
        )
        sys.stderr.write(
            f"[whatsapp-mcp] OpenProcess({initial_ppid}) failed ({reason}); "
            f"orphaned at startup, exiting\n"
        )
        sys.stderr.flush()
        os._exit(0)
    try:
        rc = WaitForSingleObject(h, INFINITE)
    finally:
        CloseHandle(h)
    if rc == WAIT_OBJECT_0:
        sys.stderr.write(
            f"[whatsapp-mcp] parent pid {initial_ppid} exited, exiting\n"
        )
    elif rc == WAIT_FAILED:
        sys.stderr.write(
            f"[whatsapp-mcp] WaitForSingleObject(parent={initial_ppid}) "
            f"failed (err={ctypes.get_last_error()}); exiting defensively\n"
        )
    else:
        sys.stderr.write(
            f"[whatsapp-mcp] WaitForSingleObject(parent={initial_ppid}) "
            f"returned 0x{rc:08x}; exiting defensively\n"
        )
    sys.stderr.flush()
    os._exit(0)


def _parent_watchdog_posix(initial_ppid: int, poll_sec: float = 5.0) -> None:
    """Poll the parent PID on POSIX. Dead PIDs raise ESRCH after reaping."""
    while True:
        time.sleep(poll_sec)
        try:
            os.kill(initial_ppid, 0)
        except OSError:
            sys.stderr.write(
                f"[whatsapp-mcp] parent pid {initial_ppid} gone, exiting\n"
            )
            sys.stderr.flush()
            os._exit(0)


def _stdin_eof_watchdog() -> None:
    try:
        # Block until the host closes stdin. We don't care about the byte
        # itself — FastMCP's own reader has already consumed any real
        # JSON-RPC frames. This thread only wakes up on EOF (host crash
        # or pipe close).
        data = sys.stdin.buffer.read(1)
        if not data:
            sys.stderr.write("[whatsapp-mcp] stdin EOF, exiting\n")
            sys.stderr.flush()
            os._exit(0)
    except Exception:
        # If sys.stdin.buffer is unusable (e.g. tests), just give up.
        return


def _install_orphan_watchdogs() -> None:
    # Disable with an env var if a future MCP host wraps us in a way that
    # confuses the watchdog (we haven't seen one yet, but it's cheap).
    if os.environ.get("WHATSAPP_MCP_DISABLE_WATCHDOG", "").lower() in ("1", "true", "yes"):
        return
    ppid = os.getppid()
    if ppid <= 1:
        return
    if sys.platform == "win32":
        target = _parent_watchdog_windows
    else:
        target = _parent_watchdog_posix
    threading.Thread(
        target=target,
        args=(ppid,),
        name="whatsapp-mcp-parent-watchdog",
        daemon=True,
    ).start()
    # The stdin watchdog races with FastMCP's own stdio reader. Skip it
    # by default so we don't compete for the input pipe — the parent
    # watchdog alone is sufficient. Enable with WHATSAPP_MCP_STDIN_EOF=1
    # if a future MCP transport ever needs the belt-and-braces guard.
    if os.environ.get("WHATSAPP_MCP_STDIN_EOF", "").lower() in ("1", "true", "yes"):
        threading.Thread(
            target=_stdin_eof_watchdog,
            name="whatsapp-mcp-stdin-watchdog",
            daemon=True,
        ).start()


# Initialize FastMCP server
mcp = FastMCP("whatsapp")

@mcp.tool()
def search_contacts(query: str) -> List[Dict[str, Any]]:
    """Search WhatsApp contacts by name or phone number.
    
    Args:
        query: Search term to match against contact names or phone numbers
    """
    contacts = whatsapp_search_contacts(query)
    return contacts

@mcp.tool()
def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Dict[str, Any]]:
    """Get WhatsApp messages matching specified criteria with optional context.
    
    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
    """
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after
    )
    return messages

@mcp.tool()
def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.
    
    Args:
        query: Optional search term to filter chats by name or JID
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
        include_last_message: Whether to include the last message in each chat (default True)
        sort_by: Field to sort results by, either "last_active" or "name" (default "last_active")
    """
    chats = whatsapp_list_chats(
        query=query,
        limit=limit,
        page=page,
        include_last_message=include_last_message,
        sort_by=sort_by
    )
    return chats

@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> Dict[str, Any]:
    """Get WhatsApp chat metadata by JID.
    
    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat

@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> Dict[str, Any]:
    """Get WhatsApp chat metadata by sender phone number.
    
    Args:
        sender_phone_number: The phone number to search for
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat

@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats

@mcp.tool()
def get_last_interaction(jid: str) -> str:
    """Get most recent WhatsApp message involving the contact.
    
    Args:
        jid: The JID of the contact to search for
    """
    message = whatsapp_get_last_interaction(jid)
    return message

@mcp.tool()
def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> Dict[str, Any]:
    """Get context around a specific WhatsApp message.
    
    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return context

@mcp.tool()
def send_message(
    recipient: str,
    message: str
) -> Dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send
    
    Returns:
        A dictionary containing success status and a status message
    """
    # Validate input
    if not recipient:
        return {
            "success": False,
            "message": "Recipient must be provided"
        }
    
    # Call the whatsapp_send_message function with the unified recipient parameter
    success, status_message = whatsapp_send_message(recipient, message)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_file(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document)
    
    Returns:
        A dictionary containing success status and a status message
    """
    
    # Call the whatsapp_send_file function
    success, status_message = whatsapp_send_file(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_audio_message(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send any audio file as a WhatsApp audio message to the specified recipient. For group messages use the JID. If it errors due to ffmpeg not being installed, use send_file instead.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send (will be converted to Opus .ogg if it's not a .ogg file)
    
    Returns:
        A dictionary containing success status and a status message
    """
    success, status_message = whatsapp_audio_voice_message(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> Dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    file_path = whatsapp_download_media(message_id, chat_jid)
    
    if file_path:
        return {
            "success": True,
            "message": "Media downloaded successfully",
            "file_path": file_path
        }
    else:
        return {
            "success": False,
            "message": "Failed to download media"
        }


@mcp.tool()
def list_all_contacts() -> Dict[str, Any]:
    """Return the user's full WhatsApp address book (all contacts the
    phone has synced to the bridge). This is the *reliable* source for
    contacts — much larger than messages.db, which only contains chats
    the user has actually exchanged messages in.

    Use this when search_contacts returns too few results, or when the
    user asks for "everyone" or "all contacts".

    Returns: {"count": int, "contacts": [{jid, phone_number,
    display_name, first_name, full_name, push_name, business_name}]}
    """
    contacts = whatsapp_list_all_contacts()
    return {"count": len(contacts), "contacts": contacts}


@mcp.tool()
def sync_history(chat_jid: str, count: int = 50) -> Dict[str, Any]:
    """Ask the user's phone for older messages in a specific chat.

    WhatsApp's HistorySyncOnDemand protocol is per-chat back-pagination.
    The phone may take 30+ seconds to respond, may ignore the request,
    or may only return a handful of messages. After this returns, wait
    a few seconds then call list_messages(chat_jid=...) to read what
    arrived. A 30-second cooldown is enforced per call to avoid
    flooding the WhatsApp servers.

    Args:
        chat_jid: The chat to fetch history for (e.g.,
                  "917000189190@s.whatsapp.net" for a 1:1 chat,
                  "...@g.us" for a group).
        count: Max messages to ask for (default 50, server-capped at 100).

    Returns: {request_accepted, error?, cooldown_remaining_sec?,
              anchor_synthetic, last_attempt}
    """
    return whatsapp_sync_history(chat_jid, count)


if __name__ == "__main__":
    # Install orphan-process watchdogs BEFORE entering FastMCP's blocking
    # stdio loop. Without these, every ungraceful MCP-host crash leaks one
    # python.exe that sits forever waiting for the next JSON-RPC frame.
    _install_orphan_watchdogs()
    # Initialize and run the server
    mcp.run(transport='stdio')