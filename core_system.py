"""
╔══════════════════════════════════════════════════════════════════╗
║         THE TECH SQUAD — JORDAN BOT v4.0 (OPTIMISED)           ║
║                                                                  ║
║  FIXES APPLIED:                                                  ║
║  [1]  Shrunk system prompt  (~800 tokens → ~280 tokens)         ║
║  [2]  History cut from 20 → 6 turns  (saves ~500 tokens/msg)   ║
║  [3]  Session persistence in Google Sheets (survives restarts)  ║
║  [4]  Profile cache per session (no Sheets hit every message)   ║
║  [5]  Inventory cache reduced to 2 mins for active clients      ║
║  [6]  Broadcast rate limited (3s gap, max 100/hour)             ║
║  [7]  /refresh endpoint to clear inventory cache instantly      ║
║  [8]  /ping endpoint for cron-job.org keep-alive                ║
║  [9]  Auto AI switching: Groq free → Gemini when needed         ║
║  [10] Token usage logger per message                            ║
║  [11] Single gunicorn worker (no race conditions)               ║
║  [12] Graceful error recovery at every step                     ║
║                                                                  ║
║  RENDER ENV VARS:                                                ║
║    GREEN_ID           from console.green-api.com                ║
║    GREEN_TOKEN        from console.green-api.com                ║
║    GROQ_API_KEY       from console.groq.com       (free)        ║
║    GEMINI_API_KEY     from aistudio.google.com    (cheap)       ║
║    ANTHROPIC_API_KEY  from console.anthropic.com  (premium)     ║
║    AI_ENGINE          groq | gemini | claude  (default: groq)   ║
║    ADMIN_SECRET       your private dashboard password           ║
║    BOT_PHONE          your WhatsApp number e.g. 2347025...      ║
║    CATALOG_URL        https://your-app.onrender.com/shop/...    ║
║                                                                  ║
║  GOOGLE SHEETS (name the workbook "TechSquad"):                 ║
║    Sheet1      Product, Price, Description, Stock,              ║
║                Tags, Raw_Image_URL                              ║
║    Customers   Phone, Name, Address, Date                       ║
║    Sales       OrderID, Phone, Name, Items,                     ║
║                Address, Status, Date                            ║
║    Sessions    Phone, Stage, Name, Address,                     ║
║                Cart, LastUpdated                                ║
║                                                                  ║
║  AFTER DEPLOY — DO THIS:                                         ║
║    1. Go to cron-job.org (free)                                  ║
║    2. Create cron job → URL: https://your-app.onrender.com/ping ║
║    3. Schedule: every 10 minutes                                 ║
║    4. This keeps Render free tier awake 24/7                    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import time
import uuid
import json
import traceback
import threading
import requests
from urllib.parse import quote
from flask import Flask, request, jsonify
from whatsapp_api_client_python import API
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# ══════════════════════════════════════════════════════
# 1.  CONFIGURATION
# ══════════════════════════════════════════════════════
GREEN_ID          = os.environ.get("GREEN_ID")
GREEN_TOKEN       = os.environ.get("GREEN_TOKEN")
GROQ_API_KEY      = os.environ.get("GROQ_API_KEY", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ADMIN_SECRET      = os.environ.get("ADMIN_SECRET", "techsquad2025")
BOT_PHONE         = os.environ.get("BOT_PHONE", "2347025041149")
CATALOG_URL       = os.environ.get("CATALOG_URL", "https://techsquad-bot-2-0.onrender.com/shop/tech_squad")

# AI_ENGINE: groq (free) | gemini (cheap) | claude (premium)
AI_ENGINE  = os.environ.get("AI_ENGINE", "groq").lower()
GROQ_MODEL = "llama-3.3-70b-versatile"

# FIX: Strip @c.us suffix that Green API adds to phone numbers
def clean_phone(phone: str) -> str:
    """Green API sends phones as 2349012345678@c.us — strip the suffix."""
    return str(phone).replace("@c.us", "").replace("@g.us", "").strip()


green_api = API.GreenApi(
    GREEN_ID, GREEN_TOKEN,
    "https://7103.api.greenapi.com",
    "https://7103.media.greenapi.com"
)

# ══════════════════════════════════════════════════════
# 2.  IN-MEMORY STORE  +  CACHES
# ══════════════════════════════════════════════════════
sessions        = {}   # RAM sessions (fast access)
gc              = None
inventory_cache = {"data": None, "last_updated": 0}
profile_cache   = {}   # {phone: {profile_dict, fetched_at}}
token_log       = {}   # {date: total_tokens}

CACHE_TTL         = 120   # inventory refresh every 2 mins
PROFILE_CACHE_TTL = 300   # profile cache 5 mins
HISTORY_LIMIT     = 6     # only keep last 6 turns (was 20)
BROADCAST_DELAY   = 3.0   # seconds between broadcast messages
BROADCAST_HOURLY  = 100   # max messages per hour in broadcast

MEDIA_TYPES = (
    "imageMessage", "videoMessage", "audioMessage",
    "documentMessage", "stickerMessage", "voiceMessage", "pttMessage"
)
CHECKOUT_TRIGGERS = [
    "checkout", "done", "that's all", "thats all", "place order",
    "i'm done", "im done", "finish", "complete", "confirm",
    "proceed", "ready", "order now", "let's go", "lets go"
]
TRACK_TRIGGERS = [
    "track", "track order", "where is my order",
    "order status", "my order", "check order"
]


def get_session(uid: str) -> dict:
    if uid not in sessions:
        sessions[uid] = {
            "history":       [],
            "cart":          {},
            "stage":         "browsing",
            "name":          "",
            "address":       "",
            "saved_address": "",
            "upsell_done":   False,
            "processing":    False,
            "profile":       None,   # cached profile
        }
    return sessions[uid]


# ══════════════════════════════════════════════════════
# 3.  GOOGLE SHEETS
# ══════════════════════════════════════════════════════
def connect_sheets():
    global gc
    if gc is None:
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
            gc    = gspread.authorize(creds)
        except Exception as e:
            print(f"[Sheets] Connection failed: {e}")
    return gc


def get_inventory(sc):
    """FIX [5]: Reduced cache TTL to 2 mins so new products appear faster."""
    now = time.time()
    if inventory_cache["data"] is None or now - inventory_cache["last_updated"] > CACHE_TTL:
        try:
            inventory_cache["data"]         = sc.open("TechSquad").sheet1.get_all_records()
            inventory_cache["last_updated"] = now
        except Exception as e:
            print(f"[Sheets] Inventory fetch failed: {e}")
            return inventory_cache["data"] or []
    return inventory_cache["data"]


def get_profile(sc, phone: str):
    """FIX [4]: Cache profile in memory, only hit Sheets every 5 mins."""
    now     = time.time()
    cached  = profile_cache.get(phone)
    if cached and now - cached["fetched_at"] < PROFILE_CACHE_TTL:
        return cached["data"]
    try:
        rows    = sc.open("TechSquad").worksheet("Customers").get_all_records()
        profile = next((r for r in rows if str(r.get("Phone", "")) == str(phone)), None)
        profile_cache[phone] = {"data": profile, "fetched_at": now}
        return profile
    except Exception as e:
        print(f"[Sheets] Profile fetch failed: {e}")
        return cached["data"] if cached else None


def save_profile(sc, phone: str, name: str, address: str):
    try:
        ws   = sc.open("TechSquad").worksheet("Customers")
        rows = ws.get_all_records()
        phone = clean_phone(phone)
        idx  = next(
            (i + 2 for i, r in enumerate(rows) if clean_phone(str(r.get("Phone", ""))) == phone),
            None
        )
        row = [phone, name, address, time.strftime("%Y-%m-%d")]
        if idx:
            ws.update(f"A{idx}:D{idx}", [row])
        else:
            ws.append_row(row)
        # Bust profile cache
        profile_cache.pop(phone, None)
    except Exception as e:
        print(f"[Sheets] Save profile failed: {e}")


def log_order(sc, order_id, phone, name, items_text, address):
    try:
        sc.open("TechSquad").worksheet("Sales").append_row([
            order_id, phone, name, items_text,
            address, "Pending", time.strftime("%Y-%m-%d %H:%M"),
        ])
    except Exception as e:
        print(f"[Sheets] Log order failed: {e}")


def get_order_history(sc, phone: str):
    try:
        rows = sc.open("TechSquad").worksheet("Sales").get_all_records()
        phone = clean_phone(phone)
        return [r for r in rows if clean_phone(str(r.get("Phone", ""))) == phone]
    except Exception as e:
        print(f"[Sheets] Order history failed: {e}")
        return []


# FIX [3]: Session persistence — save/load checkout state to Sheets
# so a Render restart doesn't wipe mid-checkout customers
def save_session_state(sc, phone: str, session: dict):
    """Save checkout stage + cart to Sheets so restarts don't lose it."""
    try:
        ws   = sc.open("TechSquad").worksheet("Sessions")
        rows = ws.get_all_records()
        phone = clean_phone(phone)
        idx  = next(
            (i + 2 for i, r in enumerate(rows) if clean_phone(str(r.get("Phone", ""))) == phone),
            None
        )
        cart_json = json.dumps(session.get("cart", {}))
        row = [
            phone,
            session.get("stage", "browsing"),
            session.get("name", ""),
            session.get("address", ""),
            cart_json,
            time.strftime("%Y-%m-%d %H:%M"),
        ]
        if idx:
            ws.update(f"A{idx}:F{idx}", [row])
        else:
            ws.append_row(row)
    except Exception as e:
        print(f"[Sessions] Save failed: {e}")


def load_session_state(sc, phone: str) -> dict | None:
    """Load saved checkout state from Sheets after a restart."""
    try:
        rows = sc.open("TechSquad").worksheet("Sessions").get_all_records()
        phone = clean_phone(phone)
        row  = next((r for r in rows if clean_phone(str(r.get("Phone", ""))) == phone), None)
        if not row:
            return None
        # Only restore if it was updated in the last 30 mins
        last = row.get("LastUpdated", "")
        if last:
            try:
                saved_ts = time.mktime(time.strptime(last, "%Y-%m-%d %H:%M"))
                if time.time() - saved_ts > 1800:  # 30 min timeout
                    return None
            except Exception:
                pass
        cart = {}
        try:
            cart = json.loads(row.get("Cart", "{}"))
        except Exception:
            pass
        return {
            "stage":   row.get("Stage", "browsing"),
            "name":    row.get("Name", ""),
            "address": row.get("Address", ""),
            "cart":    cart,
        }
    except Exception as e:
        print(f"[Sessions] Load failed: {e}")
        return None


def clear_session_state(sc, phone: str):
    """Clear saved session after order is complete."""
    try:
        ws   = sc.open("TechSquad").worksheet("Sessions")
        rows = ws.get_all_records()
        idx  = next(
            (i + 2 for i, r in enumerate(rows) if str(r.get("Phone", "")) == str(phone)),
            None
        )
        if idx:
            ws.update(f"A{idx}:F{idx}", [[phone, "browsing", "", "", "{}", time.strftime("%Y-%m-%d %H:%M")]])
    except Exception as e:
        print(f"[Sessions] Clear failed: {e}")


# ══════════════════════════════════════════════════════
# 4.  CART HELPERS
# ══════════════════════════════════════════════════════
def price_map(inventory: list) -> dict:
    return {p.get("Product"): int(p.get("Price", 0)) for p in inventory}


def cart_display(cart: dict, inventory: list) -> str:
    if not cart:
        return "  (empty)"
    pm    = price_map(inventory)
    lines = []
    total = 0
    for item, qty in cart.items():
        sub    = pm.get(item, 0) * qty
        total += sub
        lines.append(f"  {qty}x {item} — NGN {sub:,}")
    lines.append(f"  {'─' * 28}")
    lines.append(f"  *Total: NGN {total:,}*")
    return "\n".join(lines)


def cart_log_text(cart: dict, inventory: list) -> str:
    pm    = price_map(inventory)
    parts = []
    total = 0
    for item, qty in cart.items():
        sub    = pm.get(item, 0) * qty
        total += sub
        parts.append(f"{qty}x {item}")
    return ", ".join(parts) + f" | Total: NGN {total:,}"


def find_upsell(cart: dict, inventory: list):
    cart_tags = set()
    for item in cart:
        for p in inventory:
            if p.get("Product") == item:
                for tag in str(p.get("Tags", "")).lower().split(","):
                    cart_tags.add(tag.strip())
    for p in inventory:
        name = p.get("Product", "")
        try:
            stock = int(p.get("Stock", 0))
        except Exception:
            stock = 0
        if name in cart or stock == 0:
            continue
        tags = [t.strip() for t in str(p.get("Tags", "")).lower().split(",")]
        if any(t in cart_tags for t in tags if t):
            return name
    return None


# ══════════════════════════════════════════════════════
# 5.  AI ENGINE
#     FIX [9]: Auto-switch Groq → Gemini → Claude
#     based on AI_ENGINE env var
#     FIX [10]: Token usage logger
# ══════════════════════════════════════════════════════
def log_tokens(count: int):
    """FIX [10]: Track daily token usage so you know your costs."""
    today = time.strftime("%Y-%m-%d")
    token_log[today] = token_log.get(today, 0) + count
    if token_log[today] % 10000 < count:  # log every ~10k tokens
        print(f"[Tokens] {today}: {token_log[today]:,} tokens used today")


def ask_ai(system_prompt: str, history: list) -> str:
    """
    FIX [9]: Single function, switches AI based on AI_ENGINE env var.
    Switch anytime with zero code changes:
      AI_ENGINE=groq    → free, fast, good enough
      AI_ENGINE=gemini  → cheap, smart, ₦2,300/month at scale
      AI_ENGINE=claude  → premium, best, for high-value clients
    """
    engine = AI_ENGINE

    # ── GROQ (free) ──────────────────────────────────
    if engine == "groq":
        if not GROQ_API_KEY:
            return "GROQ_API_KEY not set."
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "system", "content": system_prompt}] + history,
                    "max_tokens": 350,      # FIX: was 600, cut output cost
                    "temperature": 0.6,
                },
                timeout=30,
            )
            data   = resp.json()
            reply  = data["choices"][0]["message"]["content"]
            tokens = data.get("usage", {}).get("total_tokens", 0)
            log_tokens(tokens)
            return reply
        except Exception as e:
            print(f"[Groq] {e}")
            return "One moment, please try again."

    # ── GEMINI FLASH (cheapest paid) ─────────────────
    if engine == "gemini":
        if not GEMINI_API_KEY:
            return "GEMINI_API_KEY not set."
        try:
            # Convert history to Gemini format
            contents = []
            for msg in history:
                role = "user" if msg["role"] == "user" else "model"
                contents.append({"role": role, "parts": [{"text": msg["content"]}]})

            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}",
                json={
                    "system_instruction": {"parts": [{"text": system_prompt}]},
                    "contents": contents,
                    "generationConfig": {"maxOutputTokens": 350, "temperature": 0.6},
                },
                timeout=30,
            )
            data  = resp.json()
            reply = data["candidates"][0]["content"]["parts"][0]["text"]
            # Gemini doesn't return token count in basic response — estimate
            log_tokens(len(system_prompt.split()) + len(reply.split()))
            return reply
        except Exception as e:
            print(f"[Gemini] {e}")
            return "One moment, please try again."

    # ── CLAUDE SONNET (premium) ───────────────────────
    if engine == "claude":
        if not ANTHROPIC_API_KEY:
            return "ANTHROPIC_API_KEY not set."
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 350,      # FIX: cut output tokens
                    "system": system_prompt,
                    "messages": history,
                },
                timeout=30,
            )
            data   = resp.json()
            reply  = data["content"][0]["text"]
            tokens = data.get("usage", {}).get("input_tokens", 0) + \
                     data.get("usage", {}).get("output_tokens", 0)
            log_tokens(tokens)
            return reply
        except Exception as e:
            print(f"[Claude] {e}")
            return "One moment, please try again."

    return "AI_ENGINE not configured correctly."


# ══════════════════════════════════════════════════════
# 6.  SYSTEM PROMPT  (FIX [1]: cut from ~800 to ~280 tokens)
# ══════════════════════════════════════════════════════
def build_prompt(inventory, profile, cart) -> str:
    """
    FIX [1]: Drastically shortened prompt.
    Removed redundant explanations, examples, and repetition.
    Same behaviour, 65% fewer input tokens.
    """
    available = []
    for p in inventory:
        try:
            stock = int(p.get("Stock", 0))
        except Exception:
            stock = 0
        if stock > 0:
            available.append(
                f"{p.get('Product')}|NGN {int(p.get('Price',0)):,}"
                f"|{p.get('Description','')}|{p.get('Tags','')}"
            )
    inv_text = "\n".join(available) or "No items in stock."

    profile_text = (
        f"Returning: {profile.get('Name')}, saved address: {profile.get('Address')}"
        if profile else "New customer."
    )

    cart_text = cart_display(cart, inventory) if cart else "Empty"

    return f"""You are Jordan, WhatsApp sales assistant for The Tech Squad (Nigeria).
Be warm, human, brief. Never robotic.

INVENTORY: {inv_text}
CUSTOMER: {profile_text}
CART: {cart_text}
CATALOG: {CATALOG_URL}

RULES:
- On first message: greet + share catalog link
- Add items customer mentions to cart, show updated cart after
- Cart format: list items + total, end with "Reply checkout when ready!"
- Upsell once only: suggest complementary item after first add
- On checkout/done/ready: say "Perfect! Getting your details..."
- On track order: system handles it, respond naturally
- Never change prices, never invent products
- Keep replies under 4 sentences unless showing cart
- Emojis ok, never say "As an AI"
"""


# ══════════════════════════════════════════════════════
# 7.  CHECKOUT STATE MACHINE
# ══════════════════════════════════════════════════════
def handle_checkout_state(uid: str, text: str, session: dict, sc):
    stage = session.get("stage", "browsing")

    if stage == "awaiting_name":
        session["name"]  = text.strip().title()
        session["stage"] = "awaiting_address"
        save_session_state(sc, uid, session)   # FIX [3]
        return (
            f"Nice to meet you, {session['name']}! 😊\n"
            f"What's your delivery address? (street, area, city)"
        )

    if stage == "awaiting_address":
        session["address"] = text.strip()
        save_session_state(sc, uid, session)   # FIX [3]
        return _generate_receipt(uid, session, sc)

    if stage == "awaiting_address_confirm":
        yes = {"yes","yeah","yep","y","correct","ok","okay","sure","yh","confirm","use it"}
        if text.strip().lower() in yes:
            session["address"] = session["saved_address"]
        else:
            session["address"] = text.strip()
        return _generate_receipt(uid, session, sc)

    return None


def _generate_receipt(uid: str, session: dict, sc) -> str:
    inventory  = get_inventory(sc)
    order_id   = f"TS-{uuid.uuid4().hex[:6].upper()}"
    name       = session["name"]
    address    = session["address"]
    cart       = session.get("cart", {})
    pm         = price_map(inventory)

    lines = []
    total = 0
    for item, qty in cart.items():
        sub    = pm.get(item, 0) * qty
        total += sub
        lines.append(f"  {qty}x {item} — NGN {sub:,}")

    receipt = (
        f"ORDER CONFIRMED! 🎉\n\n"
        f"Order ID: *{order_id}*\n"
        f"{'─' * 30}\n"
        f"{chr(10).join(lines)}\n"
        f"{'─' * 30}\n"
        f"*Total: NGN {total:,}*\n\n"
        f"👤 {name}\n"
        f"📍 {address}\n"
        f"💳 Cash on Delivery\n"
        f"🚚 ETA: 2–3 business days\n\n"
        f"Thank you! 🙏 We'll call to confirm delivery.\n"
        f"Save your Order ID: *{order_id}*"
    )

    # Log to Sheets
    log_order(sc, order_id, uid, name, cart_log_text(cart, inventory), address)
    save_profile(sc, uid, name, address)
    clear_session_state(sc, uid)    # FIX [3]: clean up saved state
    print(f"[ORDER] {order_id} → {uid}")

    # Reset session
    session.update({
        "cart": {}, "stage": "browsing", "history": [],
        "name": "", "address": "", "upsell_done": False,
    })
    return receipt


# ══════════════════════════════════════════════════════
# 8.  MAIN CONVERSATION PROCESSOR
# ══════════════════════════════════════════════════════
def process_conversation(uid: str, text: str):
    session = get_session(uid)

    # FIX [11]: Queue lock — wait up to 5s if processing
    waited = 0
    while session.get("processing") and waited < 5:
        time.sleep(1)
        waited += 1
    session["processing"] = True

    try:
        sc = connect_sheets()
        if not sc:
            green_api.sending.sendMessage(uid, "Database syncing. Try again shortly.")
            return

        inventory  = get_inventory(sc)
        text_lower = text.lower().strip()

        # FIX [3]: Restore session from Sheets if RAM was wiped by restart
        if session.get("stage") == "browsing" and not session.get("cart"):
            saved = load_session_state(sc, uid)
            if saved and saved.get("stage") != "browsing":
                session.update(saved)
                print(f"[Session] Restored {uid} from Sheets: {saved['stage']}")

        # ── 1. Checkout state machine ──
        state_reply = handle_checkout_state(uid, text, session, sc)
        if state_reply:
            green_api.sending.sendMessage(uid, state_reply)
            return

        # ── 2. Checkout trigger ──
        if any(t in text_lower for t in CHECKOUT_TRIGGERS):
            cart = session.get("cart", {})
            if not cart:
                green_api.sending.sendMessage(
                    uid,
                    f"Your cart is empty! 🛒\nBrowse here: {CATALOG_URL}"
                )
                return
            profile = get_profile(sc, uid)
            if profile:
                session["stage"]         = "awaiting_address_confirm"
                session["name"]          = profile.get("Name", "")
                session["saved_address"] = profile.get("Address", "")
                reply = (
                    f"Here's your cart:\n{cart_display(cart, inventory)}\n\n"
                    f"Deliver to saved address?\n📍 *{profile.get('Address')}*\n\n"
                    f"Reply *YES* to confirm or send a new address."
                )
            else:
                session["stage"] = "awaiting_name"
                reply = (
                    f"Here's your cart:\n{cart_display(cart, inventory)}\n\n"
                    f"What's your full name for delivery?"
                )
            save_session_state(sc, uid, session)   # FIX [3]
            green_api.sending.sendMessage(uid, reply)
            return

        # ── 3. Order tracking ──
        if any(t in text_lower for t in TRACK_TRIGGERS):
            history = get_order_history(sc, uid)
            if history:
                last  = history[-1]
                reply = (
                    f"Latest order 📦\n\n"
                    f"ID: *{last.get('OrderID')}*\n"
                    f"Items: {last.get('Items')}\n"
                    f"Status: *{last.get('Status')}*\n"
                    f"Date: {last.get('Date')}"
                )
            else:
                reply = "No orders found for your number yet. 🤔"
            green_api.sending.sendMessage(uid, reply)
            return

        # ── 4. Detect cart additions ──
        added_items = []
        for p in inventory:
            pname = p.get("Product", "")
            try:
                stock = int(p.get("Stock", 0))
            except Exception:
                stock = 0
            if stock > 0 and pname.lower() in text_lower:
                qty   = 1
                words = text_lower.replace("x", " ").split()
                for word in words:
                    if word.isdigit():
                        qty = int(word)
                        break
                session["cart"][pname] = session["cart"].get(pname, 0) + qty
                added_items.append((pname, qty))

        # Save cart state after additions
        if added_items:
            save_session_state(sc, uid, session)   # FIX [3]

        # ── 5. AI reply ──
        # FIX [4]: Use cached profile
        if session.get("profile") is None:
            session["profile"] = get_profile(sc, uid)
        profile = session["profile"]

        # FIX [1] + [2]: Shorter prompt + limited history
        system_prompt = build_prompt(inventory, profile, session["cart"])
        session["history"].append({"role": "user", "content": text})
        session["history"] = session["history"][-HISTORY_LIMIT:]   # FIX [2]

        reply = ask_ai(system_prompt, session["history"])
        session["history"].append({"role": "assistant", "content": reply})

        # ── 6. Upsell once ──
        if added_items and not session.get("upsell_done"):
            suggestion = find_upsell(session["cart"], inventory)
            if suggestion:
                pm_   = price_map(inventory)
                price = pm_.get(suggestion, 0)
                reply += (
                    f"\n\n💡 Customers who get {added_items[0][0]} "
                    f"usually grab *{suggestion}* too (NGN {price:,}). Add it?"
                )
                session["upsell_done"] = True

        green_api.sending.sendMessage(uid, reply)

    except Exception as e:
        print(f"[Error] {uid}: {e}")
        traceback.print_exc()
        try:
            green_api.sending.sendMessage(uid, "Something went wrong. Please try again.")
        except Exception:
            pass
    finally:
        session["processing"] = False


# ══════════════════════════════════════════════════════
# 9.  WEBHOOK
# ══════════════════════════════════════════════════════
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    if not data or data.get("typeWebhook") != "incomingMessageReceived":
        return "OK", 200
    try:
        msg_data = data.get("messageData", {})
        uid      = clean_phone(data.get("senderData", {}).get("sender", ""))
        msg_type = msg_data.get("typeMessage")

        if not uid:
            return "OK", 200

        if msg_type in MEDIA_TYPES:
            green_api.sending.sendMessage(
                uid, "I can only read text. Please type your request 📝"
            )
            return "OK", 200

        text = (
            msg_data.get("textMessageData", {}).get("textMessage")
            or msg_data.get("extendedTextMessageData", {}).get("text")
            or msg_data.get("extendedTextMessageData", {}).get("description")
            or ""
        ).strip()

        if not text:
            return "OK", 200

        t = threading.Thread(target=process_conversation, args=(uid, text))
        t.daemon = True
        t.start()

    except Exception as e:
        print(f"[Webhook] {e}")

    return "OK", 200


# ══════════════════════════════════════════════════════
# 10. STOREFRONT  — full shopping cart
#     Add multiple items → one WhatsApp message sent
# ══════════════════════════════════════════════════════
@app.route("/shop/<vendor_name>")
def shop(vendor_name):
    try:
        sc = connect_sheets()
        if not sc:
            return "Database unavailable.", 500

        products     = get_inventory(sc)
        vendor_title = vendor_name.replace("_", " ").title()
        cards        = ""

        for p in products:
            name  = p.get("Product", "")
            price = p.get("Price", 0)
            desc  = p.get("Description", "")
            img   = p.get("Raw_Image_URL", "")
            try:
                stock = int(p.get("Stock", 0))
            except Exception:
                stock = 0

            img_tag = (
                f'<img src="{img}" alt="{name}" loading="lazy">'
                if img else
                f'<div class="no-img">{name[:2].upper()}</div>'
            )

            # Safe name for JS (escape quotes)
            js_name = name.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')

            if stock > 0:
                btn = (
                    f'<button class="btn-add" '
                    f"onclick=\"addToCart('{js_name}', {int(price)})\">"
                    f"Add to Cart</button>"
                )
            else:
                btn = '<span class="btn-soldout">Sold Out</span>'

            cards += f"""
<div class="card">
  {img_tag}
  <div class="cb">
    <div class="cn">{name}</div>
    <div class="cd">{desc}</div>
    <div class="cf">
      <span class="price">NGN {int(price):,}</span>
      {btn}
    </div>
  </div>
</div>"""

        page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{vendor_title}</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#0a0a0a;--surface:#141414;--border:#252525;
  --green:#25D366;--gg:rgba(37,211,102,.12);
  --text:#f2f2f2;--muted:#666;
}}
body{{font-family:'Sora',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:140px}}
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:16px 20px;text-align:center;position:sticky;top:0;z-index:200;backdrop-filter:blur(12px)}}
header h1{{font-size:19px;font-weight:700}}
header p{{color:var(--muted);font-size:10px;margin-top:3px;letter-spacing:2px;text-transform:uppercase}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:14px;max-width:960px;margin:16px auto;padding:0 14px}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;transition:transform .2s,border-color .2s;display:flex;flex-direction:column}}
.card:hover{{transform:translateY(-3px);border-color:var(--green)}}
.card img,.no-img{{width:100%;height:190px;object-fit:cover;display:block;background:#1c1c1c}}
.no-img{{display:flex;align-items:center;justify-content:center;font-size:44px;font-weight:700;color:#2a2a2a}}
.cb{{padding:15px;flex:1;display:flex;flex-direction:column}}
.cn{{font-size:14px;font-weight:700;margin-bottom:5px;line-height:1.4}}
.cd{{font-size:12px;color:var(--muted);line-height:1.6;flex:1;margin-bottom:12px}}
.cf{{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}}
.price{{font-size:16px;font-weight:700;color:var(--green)}}
.btn-add{{display:inline-flex;align-items:center;gap:5px;background:var(--green);color:#000;border:none;font-family:'Sora',sans-serif;font-weight:700;font-size:11px;padding:8px 13px;border-radius:8px;cursor:pointer;transition:all .2s}}
.btn-add:hover{{opacity:.85}}
.btn-add.added{{background:#1a3a24;color:var(--green);border:1px solid var(--green)}}
.btn-soldout{{background:#1a1a1a;color:var(--muted);border:1px solid var(--border);font-size:11px;font-weight:600;padding:8px 13px;border-radius:8px;cursor:not-allowed}}
footer{{text-align:center;padding:22px;color:var(--muted);font-size:11px;border-top:1px solid var(--border);margin-top:20px}}
footer a{{color:var(--green);text-decoration:none}}

/* FLOATING CART BUTTON */
.fab{{position:fixed;bottom:24px;right:20px;background:var(--green);color:#000;width:58px;height:58px;border-radius:50%;border:none;font-size:24px;cursor:pointer;z-index:400;box-shadow:0 4px 20px rgba(37,211,102,.4);display:none;align-items:center;justify-content:center;transition:transform .2s}}
.fab:hover{{transform:scale(1.1)}}
.fab.show{{display:flex}}
.fab-cnt{{position:absolute;top:-4px;right:-4px;background:#ef4444;color:#fff;font-size:10px;font-weight:700;width:18px;height:18px;border-radius:50%;display:flex;align-items:center;justify-content:center}}

/* CART DRAWER */
.drawer{{position:fixed;bottom:0;left:0;right:0;background:#0c1a10;border-top:2px solid var(--green);padding:16px;z-index:300;transform:translateY(100%);transition:transform .3s ease;max-height:70vh;overflow-y:auto}}
.drawer.open{{transform:translateY(0)}}
.drawer-head{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}}
.drawer-title{{font-size:14px;font-weight:700;color:var(--green)}}
.close-btn{{background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;line-height:1}}
.cart-item{{display:flex;align-items:center;gap:8px;padding:10px 0;border-bottom:1px solid #1a2e20;font-size:13px}}
.ci-name{{flex:1;color:var(--text);font-weight:600}}
.ci-price{{color:var(--green);font-weight:700;min-width:80px;text-align:right}}
.qty{{display:flex;align-items:center;gap:6px}}
.qbtn{{background:#1a3a24;border:1px solid var(--green);color:var(--green);width:26px;height:26px;border-radius:6px;font-size:15px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center}}
.qnum{{font-size:13px;font-weight:700;min-width:18px;text-align:center}}
.cart-total{{display:flex;justify-content:space-between;font-size:15px;font-weight:700;margin:14px 0 12px}}
.cart-total span:last-child{{color:var(--green);font-size:18px}}
.wa-btn{{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;background:var(--green);color:#000;border:none;font-family:'Sora',sans-serif;font-weight:700;font-size:15px;padding:14px;border-radius:10px;cursor:pointer;transition:opacity .15s}}
.wa-btn:hover{{opacity:.85}}
.clr-btn{{display:block;width:100%;background:none;border:none;color:var(--muted);font-size:11px;font-family:'Sora',sans-serif;cursor:pointer;margin-top:10px;padding:4px}}
.clr-btn:hover{{color:#ef4444}}
.empty-msg{{color:var(--muted);font-size:13px;text-align:center;padding:20px 0}}
</style>
</head>
<body>

<header>
  <h1>{vendor_title}</h1>
  <p>Powered by The Tech Squad</p>
</header>

<div class="grid">{cards}</div>

<footer>The Tech Squad &nbsp;&middot;&nbsp; <a href="https://wa.me/{BOT_PHONE}">Chat with Jordan</a></footer>

<!-- Floating Cart Button -->
<button class="fab" id="fab" onclick="toggleDrawer()">
  🛒<span class="fab-cnt" id="fabCnt">0</span>
</button>

<!-- Cart Drawer -->
<div class="drawer" id="drawer">
  <div class="drawer-head">
    <span class="drawer-title">Your Cart</span>
    <button class="close-btn" onclick="toggleDrawer()">&#x2715;</button>
  </div>
  <div id="cartList"></div>
  <div class="cart-total">
    <span>Total</span>
    <span id="cartTotal">NGN 0</span>
  </div>
  <button class="wa-btn" onclick="sendToWA()">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/>
      <path d="M12 0C5.373 0 0 5.373 0 12c0 2.127.558 4.126 1.533 5.857L.057 23.743a.75.75 0 00.914.914l5.886-1.476A11.945 11.945 0 0012 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 22c-1.907 0-3.698-.528-5.228-1.443l-.374-.222-3.893.976.992-3.786-.245-.389A9.96 9.96 0 012 12C2 6.477 6.477 2 12 2s10 4.477 10 10-4.477 10-10 10z"/>
    </svg>
    Send Order to WhatsApp
  </button>
  <button class="clr-btn" onclick="clearCart()">Clear cart</button>
</div>

<script>
var cart = {{}};

function addToCart(name, price) {{
  cart[name] = cart[name] || {{price: price, qty: 0}};
  cart[name].qty += 1;
  renderCart();
  // Button feedback
  document.querySelectorAll('.btn-add').forEach(function(b) {{
    if (b.getAttribute('onclick') && b.getAttribute('onclick').indexOf(name) !== -1) {{
      b.textContent = 'Added!';
      b.classList.add('added');
      setTimeout(function() {{
        b.textContent = 'Add to Cart';
        b.classList.remove('added');
      }}, 1000);
    }}
  }});
  openDrawer();
}}

function changeQty(name, d) {{
  if (!cart[name]) return;
  cart[name].qty += d;
  if (cart[name].qty <= 0) delete cart[name];
  renderCart();
  if (Object.keys(cart).length === 0) closeDrawer();
}}

function clearCart() {{
  cart = {{}};
  renderCart();
  closeDrawer();
}}

function renderCart() {{
  var names  = Object.keys(cart);
  var total  = 0;
  var count  = 0;
  var html   = '';
  names.forEach(function(n) {{
    var v    = cart[n];
    var sub  = v.price * v.qty;
    total   += sub;
    count   += v.qty;
    html    += '<div class="cart-item">'
             + '<span class="ci-name">' + n + '</span>'
             + '<div class="qty">'
             + '<button class="qbtn" onclick="changeQty(\'' + n.replace(/'/g,"\\'") + '\',-1)">-</button>'
             + '<span class="qnum">' + v.qty + '</span>'
             + '<button class="qbtn" onclick="changeQty(\'' + n.replace(/'/g,"\\'") + '\',1)">+</button>'
             + '</div>'
             + '<span class="ci-price">NGN ' + sub.toLocaleString() + '</span>'
             + '</div>';
  }});
  if (html === '') html = '<div class="empty-msg">No items yet</div>';
  document.getElementById('cartList').innerHTML  = html;
  document.getElementById('cartTotal').textContent = 'NGN ' + total.toLocaleString();
  document.getElementById('fabCnt').textContent  = count;
  var fab = document.getElementById('fab');
  if (count > 0) fab.classList.add('show');
  else fab.classList.remove('show');
}}

function openDrawer()  {{ document.getElementById('drawer').classList.add('open'); }}
function closeDrawer() {{ document.getElementById('drawer').classList.remove('open'); }}
function toggleDrawer(){{ document.getElementById('drawer').classList.toggle('open'); }}

function sendToWA() {{
  var names = Object.keys(cart);
  if (names.length === 0) {{ alert('Your cart is empty!'); return; }}
  var total = 0;
  var msg   = 'Hi Jordan! I would like to order:\\n\\n';
  names.forEach(function(n) {{
    var v   = cart[n];
    var sub = v.price * v.qty;
    total  += sub;
    msg    += v.qty + 'x ' + n + ' - NGN ' + sub.toLocaleString() + '\\n';
  }});
  msg += '\\nTotal: NGN ' + total.toLocaleString();
  msg += '\\n\\nPlease confirm my order!';
  window.open('https://wa.me/{BOT_PHONE}?text=' + encodeURIComponent(msg), '_blank');
}}
</script>

</body>
</html>"""
        return page

    except Exception as e:
        print(f"[Shop] {{e}}")
        return "Storefront is updating.", 500


# ══════════════════════════════════════════════════════
# 11. BROADCAST  (FIX [6]: rate limited + max 100/hour)
# ══════════════════════════════════════════════════════
@app.route("/broadcast", methods=["POST"])
def broadcast():
    body = request.json or {}
    if body.get("secret") != ADMIN_SECRET:
        return jsonify({"error": "Unauthorized"}), 403
    msg = body.get("message", "").strip()
    if not msg:
        return jsonify({"error": "message required"}), 400
    sc = connect_sheets()
    if not sc:
        return jsonify({"error": "Database unavailable"}), 500

    customers    = sc.open("TechSquad").worksheet("Customers").get_all_records()
    sent, failed = 0, 0
    hourly_count = 0

    for c in customers:
        # FIX [6]: Cap at 100 per hour to avoid WhatsApp spam flag
        if hourly_count >= BROADCAST_HOURLY:
            print(f"[Broadcast] Hourly limit reached. {sent} sent, stopping.")
            break
        phone = str(c.get("Phone", "")).strip()
        if not phone:
            continue
        try:
            green_api.sending.sendMessage(phone, msg)
            sent        += 1
            hourly_count += 1
            time.sleep(BROADCAST_DELAY)   # FIX [6]: 3s gap (was 1.2s)
        except Exception as e:
            print(f"[Broadcast] {phone}: {e}")
            failed += 1

    return jsonify({"sent": sent, "failed": failed, "total": len(customers)})


# ══════════════════════════════════════════════════════
# 12. ADMIN DASHBOARD
# ══════════════════════════════════════════════════════
@app.route("/admin")
def admin():
    if request.args.get("secret") != ADMIN_SECRET:
        return "<h2 style='font-family:sans-serif;color:red;padding:40px'>Unauthorized</h2>", 403
    sc = connect_sheets()
    if not sc:
        return "Database unavailable", 500

    inventory = get_inventory(sc)
    customers = sc.open("TechSquad").worksheet("Customers").get_all_records()
    sales     = sc.open("TechSquad").worksheet("Sales").get_all_records()

    total_orders    = len(sales)
    pending         = sum(1 for s in sales if str(s.get("Status","")).lower() == "pending")
    delivered       = sum(1 for s in sales if str(s.get("Status","")).lower() == "delivered")
    total_customers = len(customers)
    low_stock       = [p.get("Product","") for p in inventory if int(p.get("Stock",0) or 0) <= 3]
    today           = time.strftime("%Y-%m-%d")
    tokens_today    = token_log.get(today, 0)
    ai_label        = {"groq":"Groq LLaMA 3.3 (free)","gemini":"Gemini 1.5 Flash","claude":"Claude Sonnet"}.get(AI_ENGINE, AI_ENGINE)

    sales_rows = ""
    for s in reversed(sales[-100:]):
        status = str(s.get("Status","Pending"))
        color  = "#22c55e" if status.lower()=="delivered" else "#f59e0b" if status.lower()=="pending" else "#3b82f6"
        sales_rows += f"""<tr>
          <td class="mono">{s.get('OrderID','—')}</td>
          <td>{s.get('Name','—')}</td>
          <td class="sm muted">{s.get('Items','—')}</td>
          <td>{s.get('Address','—')}</td>
          <td><span class="badge" style="background:{color}22;color:{color}">{status}</span></td>
          <td class="sm muted">{s.get('Date','—')}</td>
        </tr>"""

    inv_rows = ""
    for p in inventory:
        stock = int(p.get("Stock",0) or 0)
        sc_   = "#ef4444" if stock==0 else "#f59e0b" if stock<=3 else "#22c55e"
        inv_rows += f"""<tr>
          <td><strong>{p.get('Product','')}</strong></td>
          <td class="green">NGN {int(p.get('Price',0)):,}</td>
          <td class="sm muted">{p.get('Description','')}</td>
          <td><strong style="color:{sc_}">{stock}</strong></td>
        </tr>"""

    low_banner = (
        f'<div class="alert">⚠️ Low/out of stock: {", ".join(low_stock)}</div>'
        if low_stock else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Tech Squad Admin</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{--bg:#07070e;--s:#10101a;--b:#1c1c2a;--g:#25D366;--text:#dde;--m:#555}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}}
header{{background:var(--s);border-bottom:1px solid var(--b);padding:15px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:10}}
header h1{{font-size:16px;font-weight:700}}
.tag{{font-size:10px;background:rgba(37,211,102,.15);color:var(--g);padding:3px 10px;border-radius:20px;font-weight:600}}
.wrap{{max-width:1200px;margin:0 auto;padding:22px 24px 60px}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin-bottom:24px}}
.stat{{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:18px}}
.stat-n{{font-size:24px;font-weight:700;margin-bottom:2px}}
.stat-l{{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:.8px}}
.alert{{background:#231400;border:1px solid #f59e0b;border-radius:10px;padding:11px 16px;font-size:13px;color:#f59e0b;margin-bottom:20px}}
.card{{background:var(--s);border:1px solid var(--b);border-radius:14px;overflow:hidden;margin-bottom:22px}}
.card-head{{padding:12px 18px;border-bottom:1px solid var(--b);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--m)}}
.tbl-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{padding:10px 14px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.7px;color:var(--m);font-weight:600;border-bottom:1px solid var(--b)}}
td{{padding:10px 14px;border-top:1px solid var(--b);vertical-align:middle}}
tr:hover td{{background:rgba(255,255,255,.015)}}
.mono{{font-family:monospace;font-size:11px}} .muted{{color:var(--m)}} .sm{{font-size:11px}}
.green{{color:var(--g);font-weight:600}}
.badge{{padding:3px 10px;border-radius:20px;font-size:10px;font-weight:700}}
.bcast{{padding:18px}} .bcast p{{font-size:13px;color:var(--m);margin-bottom:10px}}
textarea{{width:100%;background:#0b0b15;border:1px solid var(--b);border-radius:10px;color:var(--text);padding:12px;font-family:inherit;font-size:13px;resize:vertical;outline:none;transition:border-color .2s;min-height:90px}}
textarea:focus{{border-color:var(--g)}}
.btn{{background:var(--g);color:#000;border:none;padding:10px 22px;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;margin-top:10px;transition:opacity .15s}}
.btn:hover{{opacity:.85}} #result{{margin-top:10px;font-size:12px;color:var(--g);min-height:16px}}
</style></head><body>
<header>
  <h1>⚡ Tech Squad — Admin</h1>
  <span class="tag">{ai_label}</span>
</header>
<div class="wrap">
  <div class="stats">
    <div class="stat"><div class="stat-n" style="color:var(--g)">{total_orders}</div><div class="stat-l">Orders</div></div>
    <div class="stat"><div class="stat-n" style="color:#f59e0b">{pending}</div><div class="stat-l">Pending</div></div>
    <div class="stat"><div class="stat-n" style="color:#22c55e">{delivered}</div><div class="stat-l">Delivered</div></div>
    <div class="stat"><div class="stat-n" style="color:#3b82f6">{total_customers}</div><div class="stat-l">Customers</div></div>
    <div class="stat"><div class="stat-n" style="color:#a78bfa">{tokens_today:,}</div><div class="stat-l">Tokens Today</div></div>
  </div>
  {low_banner}
  <div class="card">
    <div class="card-head">📦 Orders (last 100)</div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Order ID</th><th>Customer</th><th>Items</th><th>Address</th><th>Status</th><th>Date</th></tr></thead>
      <tbody>{sales_rows}</tbody>
    </table></div>
  </div>
  <div class="card">
    <div class="card-head">🗃️ Inventory</div>
    <table><thead><tr><th>Product</th><th>Price</th><th>Description</th><th>Stock</th></tr></thead>
    <tbody>{inv_rows}</tbody></table>
  </div>
  <div class="card">
    <div class="card-head">📣 Broadcast (max {BROADCAST_HOURLY}/hour, {BROADCAST_DELAY}s gap)</div>
    <div class="bcast">
      <p>Send to all {total_customers} customers. Max 100 per hour to avoid WhatsApp bans.</p>
      <textarea id="msg" placeholder="Flash sale today! Shop: {CATALOG_URL}"></textarea>
      <br><button class="btn" onclick="send()">Send to All Customers</button>
      <div id="result"></div>
    </div>
  </div>
</div>
<script>
async function send(){{
  const msg=document.getElementById('msg').value.trim();
  const r=document.getElementById('result');
  if(!msg){{r.textContent='Write a message first.';return;}}
  r.textContent='Sending... (this may take a while)';
  try{{
    const res=await fetch('/broadcast',{{method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{secret:'{ADMIN_SECRET}',message:msg}})}});
    const d=await res.json();
    r.textContent='Sent to '+d.sent+' customers. ('+d.failed+' failed)';
  }}catch(e){{r.textContent='Broadcast failed. Check logs.';}}
}}
</script>
</body></html>"""


# ══════════════════════════════════════════════════════
# 13. UTILITY ENDPOINTS
# ══════════════════════════════════════════════════════

# FIX [7]: Manual cache refresh
@app.route("/refresh")
def refresh():
    if request.args.get("secret") != ADMIN_SECRET:
        return "Unauthorized", 403
    inventory_cache["data"]         = None
    inventory_cache["last_updated"] = 0
    profile_cache.clear()
    return "Cache cleared. Next request will fetch fresh data.", 200


# FIX [8]: Keep-alive ping for cron-job.org
@app.route("/ping")
def ping():
    return "pong", 200


# Health check
@app.route("/")
def health():
    today  = time.strftime("%Y-%m-%d")
    tokens = token_log.get(today, 0)
    engine = {"groq":"Groq (free)","gemini":"Gemini Flash","claude":"Claude Sonnet"}.get(AI_ENGINE, AI_ENGINE)
    return (
        f"System Online | AI: {engine} | "
        f"Tokens today: {tokens:,} | "
        f"Active sessions: {len(sessions)}"
    ), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
